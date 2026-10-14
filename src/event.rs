//! Event streams: rows supplied through the callbacks since the previous
//! block. Blocks store them as-is, and the receiver only ever inserts them.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use anyhow::{Result, bail};
use prost_types::Timestamp;

use crate::block::Block;
use crate::callbacks::{Callbacks, RowResult, TableCallbacks};
use crate::cell::display_proto_cells;
use crate::config::{Config, EventConfig, FieldConfig};
use crate::proto::event::Event as ProtoEvent;
use crate::proto::event::Events as ProtoEvents;
use crate::proto::patch::EventBatch as ProtoEventBatch;
use crate::proto::patch::EventBatches as ProtoEventBatches;
use crate::utils::{GENESIS_HASH, indent};

impl fmt::Display for ProtoEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.field_names.join(", "))?;
        for event in &self.events {
            write!(f, "\n  ({})", display_proto_cells(&event.values))?;
        }
        Ok(())
    }
}

impl fmt::Display for ProtoEventBatches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} blocks)", self.batches.len())?;
        for batch in &self.batches {
            write!(f, "\n  '{:.7}...'", batch.block)?;
            if let Some(events) = &batch.events {
                write!(f, " {}", indent(&events.to_string(), "  "))?;
            }
        }
        Ok(())
    }
}

/// Pull the new events of every event stream in `config` through
/// `callbacks`. Streams without new events are left out.
pub fn load(
    config: &Config,
    callbacks: Option<&Callbacks>,
) -> Result<HashMap<String, ProtoEvents>> {
    let mut all_events = HashMap::new();
    for (name, event_config) in &config.events {
        let Some(callbacks) = callbacks else {
            bail!(
                "event stream '{}' is pulled through callbacks, but no callbacks were provided",
                name
            );
        };
        let field_names: Vec<&str> = event_config
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let events = callbacks.with_table(name, &field_names, |bound| {
            load_from_callbacks(name, event_config, bound)
        })?;
        if !events.events.is_empty() {
            all_events.insert(name.clone(), events);
        }
    }
    Ok(all_events)
}

/// Read rows from `callbacks` until the end of the stream. Each row is one
/// event, with one cell per field in declaration order.
fn load_from_callbacks(
    name: &str,
    config: &EventConfig,
    callbacks: &TableCallbacks<'_>,
) -> Result<ProtoEvents> {
    let columns: Vec<(usize, &FieldConfig)> = config.fields.iter().enumerate().collect();
    let mut events = Vec::new();
    let mut row: usize = 0;

    loop {
        match callbacks.read_row(row, &columns)? {
            RowResult::Row(cells) => events.push(ProtoEvent {
                values: cells.into_iter().map(Into::into).collect(),
            }),
            RowResult::SkipRecord => {}
            RowResult::EndOfTable => break,
        }
        row += 1;
    }

    log::debug!(
        "Loaded {} events for event stream '{}' from callback",
        events.len(),
        name
    );

    Ok(ProtoEvents {
        field_names: config
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect(),
        events,
    })
}

/// Append the events block `hash` stored to `all_batches`, as one batch per
/// event stream. Callers append blocks oldest first.
pub fn append_batches(
    all_batches: &mut HashMap<String, ProtoEventBatches>,
    hash: &str,
    events: HashMap<String, ProtoEvents>,
) {
    for (name, events) in events {
        all_batches
            .entry(name)
            .or_default()
            .batches
            .push(ProtoEventBatch {
                block: hash.to_string(),
                events: Some(events),
            });
    }
}

/// Collect the events of every block still stored, walking back from `head`
/// until genesis or a block that is no longer stored (removed by
/// truncation). Used when a patch has no known reference: the receiver may
/// lack any of these events, and it skips the ones it already has by their
/// ID. Also returns the `head` block's creation time, so callers need not
/// load it again.
pub fn collect_chain(
    state_dir: &Path,
    head: &str,
    mode: u32,
) -> (Option<Timestamp>, HashMap<String, ProtoEventBatches>) {
    let mut created = None;
    let mut blocks = Vec::new();
    let mut hash = head.to_string();
    while hash != GENESIS_HASH {
        match Block::load(state_dir, &hash, mode) {
            Ok(block) => {
                if blocks.is_empty() {
                    created = block.created;
                }
                let parent = block.parent.clone();
                blocks.push((hash, block.events));
                hash = parent;
            }
            Err(e) => {
                log::debug!(
                    "Stopped collecting events at block '{:.7}...': {:#}",
                    hash,
                    e
                );
                break;
            }
        }
    }

    let mut all_batches = HashMap::new();
    for (hash, events) in blocks.into_iter().rev() {
        append_batches(&mut all_batches, &hash, events);
    }
    (created, all_batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::ffi::{CString, c_char, c_void};

    use crate::cell::{Cell, Kind};
    use crate::ffi::{
        END_OF_TABLE, FfiCell, FfiCellPayload, SKIP_RECORD, SUCCESS, VALUE_NULL, VALUE_TEXT,
    };
    use crate::patch::Patch;

    /// What the test callback answers for one cell.
    enum TestCell {
        Text(CString),
        Null,
        Skip,
    }

    thread_local! {
        /// Rows the test callback serves, indexed by row and column. Rows
        /// past the end answer `LCH_END_OF_TABLE`.
        static ROWS: RefCell<Vec<Vec<TestCell>>> = const { RefCell::new(Vec::new()) };
    }

    unsafe extern "C" fn test_read_cell(
        _table: *const c_char,
        row: usize,
        col: usize,
        _field: *const c_char,
        out_cell: *mut FfiCell,
        _usr_data: *mut c_void,
    ) -> i32 {
        ROWS.with(|rows| {
            let rows = rows.borrow();
            let Some(cells) = rows.get(row) else {
                return END_OF_TABLE;
            };
            let cell = match &cells[col] {
                TestCell::Text(text) => FfiCell {
                    kind: VALUE_TEXT,
                    payload: FfiCellPayload {
                        text: text.as_ptr(),
                    },
                },
                TestCell::Null => FfiCell {
                    kind: VALUE_NULL,
                    payload: FfiCellPayload { number: 0.0 },
                },
                TestCell::Skip => return SKIP_RECORD,
            };
            unsafe { *out_cell = cell };
            SUCCESS
        })
    }

    fn callbacks() -> Callbacks {
        Callbacks {
            table_begin: None,
            read_cell: Some(test_read_cell),
            destroy_cell: None,
            table_end: None,
            usr_data: std::ptr::null_mut(),
        }
    }

    fn text(value: &str) -> TestCell {
        TestCell::Text(CString::new(value).unwrap())
    }

    /// A config with one `alerts` event stream of two TEXT fields.
    fn config() -> Config {
        let field = |name: &str| FieldConfig {
            name: name.to_string(),
            kind: Kind::Text,
            ..Default::default()
        };
        Config {
            events: HashMap::from([(
                "alerts".to_string(),
                EventConfig {
                    fields: vec![field("message"), field("source")],
                    id_field: "eventid".to_string(),
                },
            )]),
            ..Default::default()
        }
    }

    #[test]
    fn test_load_reads_events_in_order() {
        ROWS.with(|rows| {
            *rows.borrow_mut() = vec![
                vec![text("disk full"), text("sda")],
                vec![TestCell::Skip, TestCell::Null],
                vec![text("out of memory"), TestCell::Null],
            ]
        });

        let events = load(&config(), Some(&callbacks())).unwrap();

        assert_eq!(
            events["alerts"],
            ProtoEvents {
                field_names: vec!["message".to_string(), "source".to_string()],
                events: vec![
                    ProtoEvent {
                        values: vec![
                            Cell::Text("disk full".to_string()).into(),
                            Cell::Text("sda".to_string()).into(),
                        ],
                    },
                    ProtoEvent {
                        values: vec![
                            Cell::Text("out of memory".to_string()).into(),
                            Cell::Null.into(),
                        ],
                    },
                ],
            }
        );
    }

    #[test]
    fn test_load_leaves_out_streams_without_events() {
        ROWS.with(|rows| rows.borrow_mut().clear());
        let events = load(&config(), Some(&callbacks())).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_load_requires_callbacks() {
        let err = load(&config(), None).unwrap_err();
        assert!(
            format!("{:#}", err).contains("no callbacks were provided"),
            "got: {err:#}"
        );
    }

    /// Create a block whose `alerts` stream gets one event with `message`.
    fn create_block(config: &Config, message: &str) -> String {
        ROWS.with(|rows| *rows.borrow_mut() = vec![vec![text(message), TestCell::Null]]);
        Block::create(config, Some(&callbacks())).unwrap()
    }

    /// The block hash and first message of each batch of the `alerts` stream.
    fn batch_summary(patch: &Patch) -> Vec<(String, Cell)> {
        let mut summary = Vec::new();
        for batch in &patch.events["alerts"].batches {
            let events = batch.events.as_ref().unwrap();
            let message = Cell::try_from(&events.events[0].values[0]).unwrap();
            summary.push((batch.block.clone(), message));
        }
        summary
    }

    #[test]
    fn test_patch_carries_events_of_covered_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            work_dir: dir.path().to_path_buf(),
            ..config()
        };
        let first = create_block(&config, "one");
        let second = create_block(&config, "two");
        let third = create_block(&config, "three");

        let patch = Patch::create(&config, &first).unwrap();

        assert_eq!(
            batch_summary(&patch),
            vec![
                (second, Cell::Text("two".to_string())),
                (third, Cell::Text("three".to_string())),
            ]
        );
    }

    #[test]
    fn test_patch_from_genesis_carries_events_of_every_block() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            work_dir: dir.path().to_path_buf(),
            ..config()
        };
        let first = create_block(&config, "one");
        let second = create_block(&config, "two");

        let patch = Patch::create(&config, GENESIS_HASH).unwrap();

        assert_eq!(
            batch_summary(&patch),
            vec![
                (first, Cell::Text("one".to_string())),
                (second, Cell::Text("two".to_string())),
            ]
        );
    }

    #[test]
    fn test_events_display() {
        let events = ProtoEvents {
            field_names: vec!["message".to_string(), "source".to_string()],
            events: vec![ProtoEvent {
                values: vec![
                    Cell::Text("disk full".to_string()).into(),
                    Cell::Null.into(),
                ],
            }],
        };
        assert_eq!(
            events.to_string(),
            "[message, source]\n  (\"disk full\", NULL)"
        );
    }

    #[test]
    fn test_event_batches_display() {
        let batches = ProtoEventBatches {
            batches: vec![ProtoEventBatch {
                block: "0123456789abcdef".to_string(),
                events: Some(ProtoEvents {
                    field_names: vec!["message".to_string()],
                    ..Default::default()
                }),
            }],
        };
        assert_eq!(batches.to_string(), "(1 blocks)\n  '0123456...' [message]");
    }
}
