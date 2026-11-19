//! The spool: events recorded between blocks.
//!
//! Each event stream has its own spool file in the `spool` subdirectory of the
//! state directory. The file is named by the hash of the stream name, since
//! stream names may contain characters that file names cannot. A spool file
//! holds length-delimited `event.Events` records, oldest first.
//!
//! Access is serialized by the lock on `spool/.lock`, taken with [`lock`].
//! The other functions do not lock themselves, so call them with it held.

use std::fs::File;

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::callbacks::Callbacks;
use crate::config::Config;
use crate::event;
use crate::proto::event::Events as ProtoEvents;
use crate::storage;
use crate::utils::compute_hash;

/// The name of the spool file of event stream `stream`.
fn file_name(stream: &str) -> String {
    compute_hash(stream.as_bytes())
}

/// Lock the spool. Creates the spool directory if it does not exist. The lock
/// is released when the returned handle is dropped.
pub fn lock(config: &Config) -> Result<File> {
    let dir = config.ensure_spool_dir()?;
    storage::lock(&dir, true, config.file_mode)
}

/// Append `events` to the spool of event stream `stream`. If that would make
/// the spool exceed the stream's `spool-max-size`, the spool is first
/// compacted to half of it. Compacting first means a failed compaction
/// spools nothing.
fn append(config: &Config, stream: &str, events: &ProtoEvents) -> Result<()> {
    let Some(event_config) = config.events.get(stream) else {
        bail!("event stream '{}' is not declared", stream);
    };

    let mut record = Vec::new();
    events
        .encode_length_delimited(&mut record)
        .context("failed to encode events")?;

    let path = config.spool_dir().join(file_name(stream));
    let size = match std::fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed to read the size of '{}'", path.display()));
        }
    };
    // An empty spool has nothing to compact
    if size > 0 && size + record.len() as u64 > event_config.spool_max_size {
        compact(config, stream, event_config.spool_max_size / 2)?;
    }

    storage::append(
        &config.spool_dir(),
        &file_name(stream),
        &record,
        config.file_mode,
    )
    .with_context(|| format!("failed to spool events of event stream '{}'", stream))
}

/// Pull the new events of event stream `stream` through `callbacks` and
/// append them to the spool.
///
/// The callbacks run before the spool is locked, since they are the caller's
/// code.
pub fn record(config: &Config, stream: &str, callbacks: &Callbacks) -> Result<()> {
    let Some(event_config) = config.events.get(stream) else {
        bail!("event stream '{}' is not declared", stream);
    };
    let Some(events) = event::load_stream(stream, event_config, callbacks)
        .with_context(|| format!("failed to load events of event stream '{}'", stream))?
    else {
        log::debug!("No events to spool for event stream '{}'", stream);
        return Ok(());
    };

    let _lock = lock(config)?;
    append(config, stream, &events)?;
    log::debug!(
        "Spooled {} events of event stream '{}'",
        events.events.len(),
        stream
    );
    Ok(())
}

/// Read the spool of event stream `stream`, oldest record first. Returns no
/// records if the stream has no spool file.
pub fn read(config: &Config, stream: &str) -> Result<Vec<ProtoEvents>> {
    let Some(data) = storage::load(&config.spool_dir(), &file_name(stream))? else {
        return Ok(Vec::new());
    };

    let mut buf = data.as_slice();
    let mut records = Vec::new();
    while !buf.is_empty() {
        let remaining = buf.len();
        match ProtoEvents::decode_length_delimited(&mut buf) {
            Ok(record) => records.push(record),
            Err(e) => {
                // A crash during an append can leave an incomplete record at
                // the end. Keep the complete records before it.
                log::warn!(
                    "Dropped the last {} bytes of the spool of event stream '{}', which could not be decoded: {}",
                    remaining,
                    stream,
                    e
                );
                break;
            }
        }
    }
    Ok(records)
}

/// Remove the spool file of event stream `stream`. Does nothing if it does
/// not exist.
pub fn remove(config: &Config, stream: &str) -> Result<()> {
    storage::remove(&config.spool_dir(), &file_name(stream), config.dry_run)
}

/// Rewrite the spool of event stream `stream` with its newest events, as many
/// as fit in about `budget` bytes.
fn compact(config: &Config, stream: &str, budget: u64) -> Result<()> {
    let mut records = read(config, stream)?;

    // All events across the records, newest first
    let newest_first = records
        .iter()
        .rev()
        .flat_map(|record| record.events.iter().rev());

    // Count the newest events that fit in the budget
    let mut size = 0;
    let mut keep = 0;
    for event in newest_first {
        size += event.encoded_len() as u64;
        if size > budget {
            break;
        }
        keep += 1;
    }

    // Drop the other events, oldest first.
    let total: usize = records.iter().map(|record| record.events.len()).sum();
    let mut to_drop = total - keep;
    for record in &mut records {
        let n = to_drop.min(record.events.len());
        record.events.drain(..n);
        to_drop -= n;
    }

    // Drop empty records as a result of dropping their events.
    records.retain(|record| !record.events.is_empty());

    log::warn!(
        "Spool of event stream '{}' exceeded spool-max-size, dropped the {} oldest events",
        stream,
        total - keep
    );

    let dir = config.spool_dir();
    let name = file_name(stream);
    if records.is_empty() {
        return storage::remove(&dir, &name, false);
    }
    let mut data = Vec::new();
    for record in &records {
        record
            .encode_length_delimited(&mut data)
            .context("failed to encode events")?;
    }
    storage::store(&dir, &name, &data, config.file_mode, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::Path;

    use crate::cell::{Cell, Kind};
    use crate::config::{EventConfig, FieldConfig};
    use crate::proto::event::Event as ProtoEvent;

    /// A config in `dir` with an `alerts` event stream of one TEXT field.
    fn config(dir: &Path) -> Config {
        let event_config = EventConfig {
            fields: vec![FieldConfig {
                name: "message".to_string(),
                kind: Kind::Text,
                ..Default::default()
            }],
            id_field: "eventid".to_string(),
            ..Default::default()
        };
        Config {
            work_dir: dir.to_path_buf(),
            events: HashMap::from([("alerts".to_string(), event_config)]),
            ..Default::default()
        }
    }

    /// A record of `alerts` events, one per message.
    fn events(messages: &[&str]) -> ProtoEvents {
        ProtoEvents {
            field_names: vec!["message".to_string()],
            events: messages
                .iter()
                .map(|message| ProtoEvent {
                    values: vec![Cell::Text(message.to_string()).into()],
                })
                .collect(),
        }
    }

    #[test]
    fn test_read_returns_appended_events() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();

        append(&config, "alerts", &events(&["disk full", "out of memory"])).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![events(&["disk full", "out of memory"])]
        );
    }
}
