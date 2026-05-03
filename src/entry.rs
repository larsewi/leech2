use std::fmt;

use anyhow::Result;

use crate::proto::entry::Entry as ProtoEntry;
use crate::value::Value;
use crate::value::{decode_proto_values, display_proto_values};

/// A row in a table, split into key and value components.
///
/// `Entry` is the domain counterpart to `proto::entry::Entry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<Value>,
    pub value: Vec<Value>,
}

impl Entry {
    pub fn new(key: Vec<Value>, value: Vec<Value>) -> Self {
        Entry { key, value }
    }
}

impl TryFrom<ProtoEntry> for Entry {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoEntry) -> Result<Self> {
        Ok(Entry {
            key: decode_proto_values(proto.key)?,
            value: decode_proto_values(proto.value)?,
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

impl From<(Vec<Value>, Vec<Value>)> for ProtoEntry {
    fn from((key, value): (Vec<Value>, Vec<Value>)) -> Self {
        ProtoEntry {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Entry> for (Vec<Value>, Vec<Value>) {
    fn from(entry: Entry) -> Self {
        (entry.key, entry.value)
    }
}

impl From<(Vec<Value>, Vec<Value>)> for Entry {
    fn from((key, value): (Vec<Value>, Vec<Value>)) -> Self {
        Entry { key, value }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.key, self.value)
    }
}

impl fmt::Display for ProtoEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}) {}",
            display_proto_values(&self.key),
            display_proto_values(&self.value)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_entry_to_tuple() {
        let entry = Entry::new(vec!["k".into()], vec!["v".into()]);
        let (key, value): (Vec<Value>, Vec<Value>) = entry.into();
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
        let entry = Entry::new(vec![1.0.into(), "a".into()], vec![true.into(), Value::Null]);
        let proto: ProtoEntry = entry.clone().into();
        let back: Entry = proto.try_into().unwrap();
        assert_eq!(entry, back);
    }
}
