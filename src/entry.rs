use std::collections::HashMap;
use std::fmt;

use anyhow::Result;

use crate::cell::Cell;
use crate::cell::{decode_proto_cells, display_proto_cells};
use crate::proto::entry::Entry as ProtoEntry;

pub type RecordMap = HashMap<Vec<Cell>, Vec<Cell>>;

/// A row in a table, split into key and value components.
///
/// `Entry` is the domain counterpart to `proto::entry::Entry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<Cell>,
    pub value: Vec<Cell>,
}

impl Entry {
    pub fn new(key: Vec<Cell>, value: Vec<Cell>) -> Self {
        Entry { key, value }
    }
}

impl TryFrom<ProtoEntry> for Entry {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoEntry) -> Result<Self> {
        Ok(Entry {
            key: decode_proto_cells(proto.key)?,
            value: decode_proto_cells(proto.value)?,
        })
    }
}

impl From<Entry> for ProtoEntry {
    fn from(entry: Entry) -> Self {
        ProtoEntry {
            key: entry.key.into_iter().map(Into::into).collect(),
            value: entry.value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<(Vec<Cell>, Vec<Cell>)> for ProtoEntry {
    fn from((key, value): (Vec<Cell>, Vec<Cell>)) -> Self {
        ProtoEntry {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Entry> for (Vec<Cell>, Vec<Cell>) {
    fn from(entry: Entry) -> Self {
        (entry.key, entry.value)
    }
}

impl From<(Vec<Cell>, Vec<Cell>)> for Entry {
    fn from((key, value): (Vec<Cell>, Vec<Cell>)) -> Self {
        Entry { key, value }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.key, self.value)
    }
}

/// Decode a `Vec<ProtoEntry>` into a `HashMap` keyed by each entry's key.
pub fn decode_proto_records(protos: Vec<ProtoEntry>) -> Result<HashMap<Vec<Cell>, Vec<Cell>>> {
    let mut records = HashMap::with_capacity(protos.len());
    for proto in protos {
        let entry = Entry::try_from(proto)?;
        records.insert(entry.key, entry.value);
    }
    Ok(records)
}

impl fmt::Display for ProtoEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}) {}",
            display_proto_cells(&self.key),
            display_proto_cells(&self.value)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_entry_to_tuple() {
        let entry = Entry::new(vec!["k".into()], vec!["v".into()]);
        let (key, value): (Vec<Cell>, Vec<Cell>) = entry.into();
        assert_eq!(key, vec!["k".into()]);
        assert_eq!(value, vec!["v".into()]);
    }

    #[test]
    fn from_tuple_to_entry() {
        let entry: Entry = (vec!["k".into()], vec!["v".into()]).into();
        assert_eq!(entry.key, vec!["k".into()]);
        assert_eq!(entry.value, vec!["v".into()]);
    }

    #[test]
    fn proto_round_trip() {
        let entry = Entry::new(vec![1.0.into(), "a".into()], vec![true.into(), Cell::Null]);
        let proto: ProtoEntry = entry.clone().into();
        let back: Entry = proto.try_into().unwrap();
        assert_eq!(entry, back);
    }
}
