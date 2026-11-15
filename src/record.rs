use std::collections::HashMap;
use std::fmt;

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::Cell;
use crate::cell::{decode_proto_cells, display_proto_cells};
use crate::proto::record::Record as ProtoRecord;

/// Records keyed by key, each with an optional change timestamp.
pub type TimestampedRecordMap = HashMap<Vec<Cell>, (Vec<Cell>, Option<Timestamp>)>;

/// One row of a table, split into key and value halves.
///
/// `Record` is the domain counterpart to `proto::record::Record`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<Cell>,
    pub value: Vec<Cell>,
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

impl From<(Vec<Cell>, Vec<Cell>)> for ProtoRecord {
    fn from((key, value): (Vec<Cell>, Vec<Cell>)) -> Self {
        ProtoRecord {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
        }
    }
}

/// Converts an insert or delete entry into a block record. Blocks carry no
/// change timestamps, so the timestamp is dropped.
impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoRecord {
    fn from((key, (value, _)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        (key, value).into()
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

/// Decode a block's inserts or deletes into a map keyed by each record's key.
/// Blocks carry no change timestamps, so every entry gets `None`.
pub fn decode_proto_records_without_timestamps(
    protos: Vec<ProtoRecord>,
) -> Result<TimestampedRecordMap> {
    protos
        .into_iter()
        .map(|proto| {
            let record = Record::try_from(proto)?;
            Ok((record.key, (record.value, None)))
        })
        .collect()
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
    fn proto_round_trip() {
        let key = vec![Cell::number(1.0).unwrap(), "a".into()];
        let value = vec![true.into(), Cell::Null];
        let proto: ProtoRecord = (key.clone(), value.clone()).into();
        let back: Record = proto.try_into().unwrap();
        assert_eq!(back.key, key);
        assert_eq!(back.value, value);
    }

    #[test]
    fn decode_without_timestamps() {
        let proto: ProtoRecord = (vec!["k".into()], vec!["v".into()]).into();
        let records = decode_proto_records_without_timestamps(vec![proto]).unwrap();
        assert_eq!(records[&vec!["k".into()]], (vec!["v".into()], None));
    }
}
