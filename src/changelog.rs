//! Change logs: each insert, update and delete of a table with a change log
//! is recorded as an event when a block is created. The receiver writes the
//! events as rows in the log table.

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::Cell;
use crate::config::LogConfig;
use crate::delta::Delta;
use crate::proto::event::Event as ProtoEvent;
use crate::proto::event::Events as ProtoEvents;
use crate::utils::format_iso_timestamp;

/// Record the changes in `delta` as log events, one per delete, insert and
/// update, stamped with the block's creation time. Updates log the values
/// after the change. Returns `None` when there is nothing to log.
pub fn events(log: &LogConfig, delta: &Delta, created: &Timestamp) -> Result<Option<ProtoEvents>> {
    let mut field_names = delta.primary_key_names.clone();
    for name in &delta.subsidiary_value_names {
        if !log.exclude.contains(name) {
            field_names.push(name.clone());
        }
    }
    field_names.push(log.operation_field.clone());
    field_names.push(log.timestamp_field.clone());

    let timestamp = Cell::Text(format_iso_timestamp(created)?);
    let mut events = Vec::new();
    for (key, (value, _)) in &delta.deletes {
        let values = logged_values(log, &delta.subsidiary_value_names, value);
        events.push(event(key, values, &log.operations.delete, &timestamp));
    }
    for (key, (value, _)) in &delta.inserts {
        let values = logged_values(log, &delta.subsidiary_value_names, value);
        events.push(event(key, values, &log.operations.insert, &timestamp));
    }
    for (key, (_, new_value, _)) in &delta.updates {
        let values = logged_values(log, &delta.subsidiary_value_names, new_value);
        events.push(event(key, values, &log.operations.update, &timestamp));
    }

    if events.is_empty() {
        return Ok(None);
    }
    Ok(Some(ProtoEvents {
        field_names,
        events,
    }))
}

/// The cells of `value` whose field is not in `log.exclude`.
/// `subsidiary_value_names` holds the field names, in the same order as
/// `value`.
fn logged_values<'a>(
    log: &LogConfig,
    subsidiary_value_names: &[String],
    value: &'a [Cell],
) -> Vec<&'a Cell> {
    let mut cells = Vec::new();
    for (name, cell) in subsidiary_value_names.iter().zip(value) {
        if !log.exclude.contains(name) {
            cells.push(cell);
        }
    }
    cells
}

/// Build one log event: the key, the logged values, the operation and the
/// timestamp.
fn event(key: &[Cell], values: Vec<&Cell>, operation: &str, timestamp: &Cell) -> ProtoEvent {
    let mut cells = Vec::new();
    for cell in key.iter().chain(values) {
        cells.push(cell.clone().into());
    }
    cells.push(Cell::Text(operation.to_string()).into());
    cells.push(timestamp.clone().into());
    ProtoEvent { values: cells }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use crate::cell::text_proto_cells;

    /// 2026-10-09T08:30:00Z.
    const CREATED: Timestamp = Timestamp {
        seconds: 1_791_534_600,
        nanos: 0,
    };

    fn log_config(exclude: &[&str]) -> LogConfig {
        LogConfig {
            table: "userslog".to_string(),
            id_field: "eventid".to_string(),
            operation_field: "operation".to_string(),
            timestamp_field: "timestamp".to_string(),
            operations: Default::default(),
            exclude: exclude.iter().map(|name| name.to_string()).collect(),
        }
    }

    fn cells(values: &[&str]) -> Vec<Cell> {
        values.iter().map(|&value| Cell::from(value)).collect()
    }

    fn delta() -> Delta {
        Delta {
            primary_key_names: vec!["id".to_string()],
            subsidiary_value_names: vec!["name".to_string(), "email".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_events_one_per_change() {
        let mut delta = delta();
        delta.deletes = HashMap::from([(cells(&["1"]), (cells(&["Alice", "a@x"]), None))]);
        delta.inserts = HashMap::from([(cells(&["2"]), (cells(&["Bob", "b@x"]), None))]);
        delta.updates = HashMap::from([(
            cells(&["3"]),
            (cells(&["Carol", "c@x"]), cells(&["Caroline", "c@x"]), None),
        )]);

        let events = events(&log_config(&[]), &delta, &CREATED).unwrap().unwrap();

        assert_eq!(
            events.field_names,
            vec!["id", "name", "email", "operation", "timestamp"]
        );
        let mut values = Vec::new();
        for event in events.events {
            values.push(event.values);
        }
        let timestamp = "2026-10-09T08:30:00Z";
        assert_eq!(
            values,
            vec![
                text_proto_cells(&["1", "Alice", "a@x", "DELETE", timestamp]),
                text_proto_cells(&["2", "Bob", "b@x", "INSERT", timestamp]),
                text_proto_cells(&["3", "Caroline", "c@x", "UPDATE", timestamp]),
            ]
        );
    }

    #[test]
    fn test_events_leave_out_excluded_fields() {
        let mut delta = delta();
        delta.inserts = HashMap::from([(cells(&["2"]), (cells(&["Bob", "b@x"]), None))]);

        let events = events(&log_config(&["email"]), &delta, &CREATED)
            .unwrap()
            .unwrap();

        assert_eq!(
            events.field_names,
            vec!["id", "name", "operation", "timestamp"]
        );
        assert_eq!(
            events.events[0].values,
            text_proto_cells(&["2", "Bob", "INSERT", "2026-10-09T08:30:00Z"])
        );
    }

    #[test]
    fn test_events_log_update_of_excluded_fields_only() {
        let mut delta = delta();
        delta.updates = HashMap::from([(
            cells(&["3"]),
            (cells(&["Carol", "c@x"]), cells(&["Carol", "carol@x"]), None),
        )]);

        let events = events(&log_config(&["email"]), &delta, &CREATED)
            .unwrap()
            .unwrap();

        assert_eq!(
            events.events[0].values,
            text_proto_cells(&["3", "Carol", "UPDATE", "2026-10-09T08:30:00Z"])
        );
    }

    #[test]
    fn test_events_none_without_changes() {
        let events = events(&log_config(&[]), &delta(), &CREATED).unwrap();
        assert_eq!(events, None);
    }
}
