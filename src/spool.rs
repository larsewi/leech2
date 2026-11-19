//! The spool: events recorded between blocks.
//!
//! Each event stream has its own spool file in the `spool` subdirectory of the
//! state directory. The file is named by the hash of the stream name, since
//! stream names may contain characters that file names cannot. A spool file
//! holds length-delimited `event.Events` records, oldest first.
//!
//! Access is serialized by the lock on `spool/.lock`, taken with [`lock`].
//! The other functions do not lock themselves, so call them with it held.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::callbacks::Callbacks;
use crate::config::Config;
use crate::event;
use crate::proto::event::Events as ProtoEvents;
use crate::storage;
use crate::utils::{compute_hash, indent, is_hex_hash};

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
/// spools nothing. A torn record at the end of the spool is cut off first, so
/// the new record does not follow it.
fn append(config: &Config, stream: &str, events: &ProtoEvents) -> Result<()> {
    let Some(event_config) = config.events.get(stream) else {
        bail!("event stream '{}' is not declared", stream);
    };

    let mut record = Vec::new();
    events
        .encode_length_delimited(&mut record)
        .context("failed to encode events")?;

    let size = repair(&config.spool_dir().join(file_name(stream)))?;
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

/// Cut off a torn record at the end of the spool file `path`, left by a crash
/// or a failed write during an append. Returns the size of the file after
/// that, or 0 if it does not exist.
///
/// Only the length prefixes are read: a record that reaches past the end of
/// the file is torn.
fn repair(path: &Path) -> Result<u64> {
    let file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("failed to open '{}'", path.display())),
    };
    let size = file
        .metadata()
        .with_context(|| format!("failed to read the size of '{}'", path.display()))?
        .len();

    // Jump from record to record by their length prefixes
    let mut pos = 0;
    while pos < size {
        // A length prefix is a varint of at most 10 bytes
        let mut prefix = Vec::new();
        (&file)
            .seek(SeekFrom::Start(pos))
            .and_then(|_| (&file).take(10).read_to_end(&mut prefix))
            .with_context(|| format!("failed to read '{}'", path.display()))?;
        let mut buf = prefix.as_slice();
        let Ok(len) = prost::encoding::decode_varint(&mut buf) else {
            break;
        };
        let prefix_len = (prefix.len() - buf.len()) as u64;
        match pos.checked_add(prefix_len).and_then(|p| p.checked_add(len)) {
            Some(next) if next <= size => pos = next,
            _ => break,
        }
    }

    if pos < size {
        log::warn!(
            "Cut off a torn record of {} bytes at the end of '{}'",
            size - pos,
            path.display()
        );
        file.set_len(pos)
            .with_context(|| format!("failed to truncate '{}'", path.display()))?;
    }
    Ok(pos)
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

/// Read the spool of every event stream in `config`, merged into one batch
/// per stream, oldest first. Records with other field names than the stream
/// has now are dropped. Streams without spooled events are left out. The
/// spool is not removed, see [`clear`].
pub fn take(config: &Config) -> Result<HashMap<String, ProtoEvents>> {
    let mut all_events = HashMap::new();
    for (stream, event_config) in &config.events {
        let mut merged = ProtoEvents {
            field_names: event_config
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect(),
            events: Vec::new(),
        };
        let mut dropped = 0;
        for record in read(config, stream)? {
            if record.field_names == merged.field_names {
                merged.events.extend(record.events);
            } else {
                dropped += record.events.len();
            }
        }

        if dropped > 0 {
            log::warn!(
                "Dropped {} spooled events of event stream '{}' due to a layout change",
                dropped,
                stream
            );
        }
        if !merged.events.is_empty() {
            all_events.insert(stream.clone(), merged);
        }
    }
    Ok(all_events)
}

/// Remove the spool files of all event streams, including the streams that
/// are no longer declared.
pub fn clear(config: &Config) -> Result<()> {
    let dir = config.spool_dir();
    let declared: HashSet<String> = config
        .events
        .keys()
        .map(|stream| file_name(stream))
        .collect();

    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_hex_hash(name) {
            // Not a spool file, e.g. the lock file
            continue;
        }
        if !declared.contains(name) {
            log::warn!(
                "Removed spool file '{:.7}...' of an event stream that is no longer declared in the config",
                name
            );
        }
        storage::remove(&dir, name, config.dry_run)?;
    }
    Ok(())
}

/// The spooled events of every event stream, as the next block would get
/// them. See [`take`].
pub struct Spool(pub HashMap<String, ProtoEvents>);

impl fmt::Display for Spool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spool ({} streams)", self.0.len())?;
        if !self.0.is_empty() {
            write!(f, ":")?;
        }
        for (stream, events) in &self.0 {
            write!(f, "\n  '{}' {}", stream, indent(&events.to_string(), "  "))?;
        }
        Ok(())
    }
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
    use std::ffi::{c_char, c_void};
    use std::path::Path;

    use crate::block::Block;
    use crate::cell::{Cell, Kind};
    use crate::config::{EventConfig, FieldConfig};
    use crate::ffi::{END_OF_TABLE, FfiCell, FfiCellPayload, SUCCESS, VALUE_TEXT};
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

    #[test]
    fn test_read_keeps_the_order_of_appends() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();

        append(&config, "alerts", &events(&["one"])).unwrap();
        append(&config, "alerts", &events(&["two", "three"])).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![events(&["one"]), events(&["two", "three"])]
        );
    }

    #[test]
    fn test_read_drops_an_incomplete_last_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();
        append(&config, "alerts", &events(&["one"])).unwrap();
        append(&config, "alerts", &events(&["two"])).unwrap();

        // Cut off the last byte, like a crash during the second append
        let path = config.spool_dir().join(file_name("alerts"));
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let size = file.metadata().unwrap().len();
        file.set_len(size - 1).unwrap();

        assert_eq!(read(&config, "alerts").unwrap(), vec![events(&["one"])]);
    }

    #[test]
    fn test_append_cuts_off_a_torn_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();
        append(&config, "alerts", &events(&["one"])).unwrap();
        append(&config, "alerts", &events(&["two"])).unwrap();

        // Cut off the last byte, like a crash during the second append
        let path = config.spool_dir().join(file_name("alerts"));
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let size = file.metadata().unwrap().len();
        file.set_len(size - 1).unwrap();

        append(&config, "alerts", &events(&["three"])).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![events(&["one"]), events(&["three"])]
        );
    }

    #[test]
    fn test_compact_keeps_the_newest_events() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();
        append(&config, "alerts", &events(&["event 0", "event 1"])).unwrap();
        append(
            &config,
            "alerts",
            &events(&["event 2", "event 3", "event 4"]),
        )
        .unwrap();

        // All events have the same size, so the budget fits exactly four
        let event_size = events(&["event 0"]).events[0].encoded_len() as u64;
        compact(&config, "alerts", 4 * event_size).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![
                events(&["event 1"]),
                events(&["event 2", "event 3", "event 4"])
            ]
        );
    }

    #[test]
    fn test_append_compacts_past_spool_max_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config(dir.path());
        // Room for eight events, so compaction keeps four
        let event_size = events(&["event 0"]).events[0].encoded_len() as u64;
        config.events.get_mut("alerts").unwrap().spool_max_size = 8 * event_size;
        let _lock = lock(&config).unwrap();
        let first = ["event 0", "event 1", "event 2", "event 3", "event 4"];
        append(&config, "alerts", &events(&first)).unwrap();

        // Seven events with their record overhead do not fit, so the spool is
        // compacted before this append
        append(&config, "alerts", &events(&["event 5", "event 6"])).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![
                events(&["event 1", "event 2", "event 3", "event 4"]),
                events(&["event 5", "event 6"])
            ]
        );
    }

    #[test]
    fn test_compact_removes_the_file_when_nothing_fits() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();
        append(&config, "alerts", &events(&["disk full"])).unwrap();

        compact(&config, "alerts", 1).unwrap();

        assert!(!config.spool_dir().join(file_name("alerts")).exists());
        assert!(read(&config, "alerts").unwrap().is_empty());
    }

    #[test]
    fn test_remove() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let _lock = lock(&config).unwrap();
        append(&config, "alerts", &events(&["disk full"])).unwrap();

        remove(&config, "alerts").unwrap();
        assert!(read(&config, "alerts").unwrap().is_empty());

        // Removing a spool that does not exist is not an error
        remove(&config, "alerts").unwrap();
    }

    /// Serves one event, "disk full", then ends the stream.
    unsafe extern "C" fn read_one_event(
        _table: *const c_char,
        row: usize,
        _col: usize,
        _field: *const c_char,
        out_cell: *mut FfiCell,
        _usr_data: *mut c_void,
    ) -> i32 {
        if row > 0 {
            return END_OF_TABLE;
        }
        let cell = FfiCell {
            kind: VALUE_TEXT,
            payload: FfiCellPayload {
                text: c"disk full".as_ptr(),
            },
        };
        unsafe { *out_cell = cell };
        SUCCESS
    }

    /// Callbacks that serve one event, "disk full", for every stream.
    fn callbacks() -> Callbacks {
        Callbacks {
            table_begin: None,
            read_cell: Some(read_one_event),
            destroy_cell: None,
            table_end: None,
            usr_data: std::ptr::null_mut(),
        }
    }

    #[test]
    fn test_record_spools_events() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());

        record(&config, "alerts", &callbacks()).unwrap();

        assert_eq!(
            read(&config, "alerts").unwrap(),
            vec![events(&["disk full"])]
        );
    }

    #[test]
    fn test_block_has_spooled_events_first() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        {
            let _lock = lock(&config).unwrap();
            append(&config, "alerts", &events(&["spooled"])).unwrap();
        }

        let hash = Block::create(&config, Some(&callbacks())).unwrap();

        let block = Block::load(&config.state_dir(), &hash).unwrap();
        assert_eq!(block.events["alerts"], events(&["spooled", "disk full"]));
    }

    #[test]
    fn test_block_creation_removes_the_spool() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        {
            let _lock = lock(&config).unwrap();
            append(&config, "alerts", &events(&["spooled"])).unwrap();
        }

        Block::create(&config, Some(&callbacks())).unwrap();

        assert!(!config.spool_dir().join(file_name("alerts")).exists());
    }

    #[test]
    fn test_block_creation_drops_events_after_a_layout_change() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        // Recorded when the stream had another field
        let old_layout = ProtoEvents {
            field_names: vec!["text".to_string()],
            ..events(&["spooled"])
        };
        {
            let _lock = lock(&config).unwrap();
            append(&config, "alerts", &old_layout).unwrap();
        }

        let hash = Block::create(&config, Some(&callbacks())).unwrap();

        let block = Block::load(&config.state_dir(), &hash).unwrap();
        assert_eq!(block.events["alerts"], events(&["disk full"]));
    }

    #[test]
    fn test_block_creation_removes_the_spool_of_an_undeclared_stream() {
        let dir = tempfile::tempdir().unwrap();
        // Spooled when the config still declared the stream
        let mut old_config = config(dir.path());
        let logins = old_config.events["alerts"].clone();
        old_config.events.insert("logins".to_string(), logins);
        {
            let _lock = lock(&old_config).unwrap();
            append(&old_config, "logins", &events(&["spooled"])).unwrap();
        }

        let config = config(dir.path());
        Block::create(&config, Some(&callbacks())).unwrap();

        assert!(!config.spool_dir().join(file_name("logins")).exists());
    }
}
