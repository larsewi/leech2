use std::collections::HashMap;

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::{Cell, decode_proto_cells};
use crate::proto::record::Record as ProtoRecord;

pub type InsertMap = HashMap<Vec<Cell>, (Vec<Cell>, Option<Timestamp>)>;

/// A record that was added to a table.
///
/// `Insert` is the domain counterpart to a `proto::record::Record` in a
/// delta's inserts. Unlike [`crate::record::Record`], it carries the record's
/// change timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub key: Vec<Cell>,
    pub value: Vec<Cell>,
    /// Creation time of the last block that changed this record. Only set for
    /// tables that track changes.
    pub change_timestamp: Option<Timestamp>,
}

impl TryFrom<ProtoRecord> for Insert {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoRecord) -> Result<Self> {
        Ok(Insert {
            key: decode_proto_cells(proto.key)?,
            value: decode_proto_cells(proto.value)?,
            change_timestamp: proto.change_timestamp,
        })
    }
}

impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoRecord {
    fn from((key, (value, change_timestamp)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        ProtoRecord {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

/// Decode a `Vec<ProtoRecord>` into an [`InsertMap`] keyed by each record's key.
pub fn decode_proto_inserts(protos: Vec<ProtoRecord>) -> Result<InsertMap> {
    let mut inserts = HashMap::with_capacity(protos.len());
    for proto in protos {
        let insert = Insert::try_from(proto)?;
        inserts.insert(insert.key, (insert.value, insert.change_timestamp));
    }
    Ok(inserts)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cell::text_proto_cells;

    #[test]
    fn test_proto_round_trip_keeps_change_timestamp() {
        let change_timestamp = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let proto = ProtoRecord {
            key: text_proto_cells(&["k"]),
            value: text_proto_cells(&["v"]),
            change_timestamp: Some(change_timestamp),
        };

        let inserts = decode_proto_inserts(vec![proto.clone()]).unwrap();
        let (key, entry) = inserts.into_iter().next().unwrap();
        assert_eq!(ProtoRecord::from((key, entry)), proto);
    }
}
