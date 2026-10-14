//! Event streams: rows supplied through the callbacks since the previous
//! block. Blocks store them as-is, and the receiver only ever inserts them.

use std::collections::HashMap;

use anyhow::{Result, bail};

use crate::callbacks::{Callbacks, RowResult, TableCallbacks};
use crate::config::{Config, EventConfig, FieldConfig};
use crate::proto::event::Event as ProtoEvent;
use crate::proto::event::Events as ProtoEvents;

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

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::ffi::{CString, c_char, c_void};

    use crate::cell::{Cell, Kind};
    use crate::ffi::{
        END_OF_TABLE, FfiCell, FfiCellPayload, SKIP_RECORD, SUCCESS, VALUE_NULL, VALUE_TEXT,
    };

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
}
