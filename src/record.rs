use std::collections::HashMap;
use std::fmt;

use anyhow::Result;

use crate::cell::Cell;
use crate::cell::{decode_proto_cells, display_proto_cells};
use crate::proto::record::Record as ProtoRecord;

pub type RecordMap = HashMap<Vec<Cell>, Vec<Cell>>;

/// One row of a table, split into key and value halves.
///
/// `Record` is the domain counterpart to `proto::record::Record`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<Cell>,
    pub value: Vec<Cell>,
}

impl Record {
    pub fn new(key: Vec<Cell>, value: Vec<Cell>) -> Self {
        Record { key, value }
    }
}

impl TryFrom<ProtoRecord> for Record {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoRecord) -> Result<Self> {
        Ok(Record {
            key: decode_proto_cells(proto.key)?,
            value: decode_proto_cells(proto.value)?,
        })
    }
}

impl From<Record> for ProtoRecord {
    fn from(record: Record) -> Self {
        ProtoRecord {
            key: record.key.into_iter().map(Into::into).collect(),
            value: record.value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<(Vec<Cell>, Vec<Cell>)> for ProtoRecord {
    fn from((key, value): (Vec<Cell>, Vec<Cell>)) -> Self {
        ProtoRecord {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Record> for (Vec<Cell>, Vec<Cell>) {
    fn from(record: Record) -> Self {
        (record.key, record.value)
    }
}

impl From<(Vec<Cell>, Vec<Cell>)> for Record {
    fn from((key, value): (Vec<Cell>, Vec<Cell>)) -> Self {
        Record { key, value }
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.key, self.value)
    }
}

/// Decode a `Vec<ProtoRecord>` into a `HashMap` keyed by each record's key.
pub fn decode_proto_records(protos: Vec<ProtoRecord>) -> Result<HashMap<Vec<Cell>, Vec<Cell>>> {
    let mut records = HashMap::with_capacity(protos.len());
    for proto in protos {
        let record = Record::try_from(proto)?;
        records.insert(record.key, record.value);
    }
    Ok(records)
}

impl fmt::Display for ProtoRecord {
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
    fn from_record_to_tuple() {
        let record = Record::new(vec!["k".into()], vec!["v".into()]);
        let (key, value): (Vec<Cell>, Vec<Cell>) = record.into();
        assert_eq!(key, vec!["k".into()]);
        assert_eq!(value, vec!["v".into()]);
    }

    #[test]
    fn from_tuple_to_record() {
        let record: Record = (vec!["k".into()], vec!["v".into()]).into();
        assert_eq!(record.key, vec!["k".into()]);
        assert_eq!(record.value, vec!["v".into()]);
    }

    #[test]
    fn proto_round_trip() {
        let record = Record::new(
            vec![Cell::number(1.0).unwrap(), "a".into()],
            vec![true.into(), Cell::Null],
        );
        let proto: ProtoRecord = record.clone().into();
        let back: Record = proto.try_into().unwrap();
        assert_eq!(record, back);
    }
}
