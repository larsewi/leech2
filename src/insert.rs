use std::collections::HashMap;

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::{Cell, decode_proto_cells};
use crate::proto::insert::Insert as ProtoInsert;

pub type InsertMap = HashMap<Vec<Cell>, (Vec<Cell>, Option<Timestamp>)>;

/// A record that was added to a table.
///
/// `Insert` is the domain counterpart to `proto::insert::Insert`. Unlike
/// [`crate::record::Record`], it carries the record's change timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub key: Vec<Cell>,
    pub value: Vec<Cell>,
    /// Creation time of the last block that changed this record. Only set for
    /// tables that track changes.
    pub change_timestamp: Option<Timestamp>,
}

impl TryFrom<ProtoInsert> for Insert {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoInsert) -> Result<Self> {
        Ok(Insert {
            key: decode_proto_cells(proto.key)?,
            value: decode_proto_cells(proto.value)?,
            change_timestamp: proto.change_timestamp,
        })
    }
}

impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoInsert {
    fn from((key, (value, change_timestamp)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        ProtoInsert {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

/// Decode a `Vec<ProtoInsert>` into an [`InsertMap`] keyed by each insert's key.
pub fn decode_proto_inserts(protos: Vec<ProtoInsert>) -> Result<InsertMap> {
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

    use prost::Message;

    use crate::cell::text_proto_cells;
    use crate::proto::record::Record as ProtoRecord;

    #[test]
    fn test_proto_round_trip_keeps_change_timestamp() {
        let proto = ProtoInsert {
            key: text_proto_cells(&["k"]),
            value: text_proto_cells(&["v"]),
            change_timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
        };

        let inserts = decode_proto_inserts(vec![proto.clone()]).unwrap();
        let (key, entry) = inserts.into_iter().next().unwrap();
        assert_eq!(ProtoInsert::from((key, entry)), proto);
    }

    // Peers built before the Insert message decode delta inserts as Record,
    // so the two must stay wire-compatible.
    #[test]
    fn test_insert_is_wire_compatible_with_record() {
        let insert = ProtoInsert {
            key: text_proto_cells(&["k"]),
            value: text_proto_cells(&["v"]),
            change_timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
        };
        let record = ProtoRecord::decode(insert.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            record,
            ProtoRecord {
                key: text_proto_cells(&["k"]),
                value: text_proto_cells(&["v"]),
            }
        );

        let insert = ProtoInsert::decode(record.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            insert,
            ProtoInsert {
                key: text_proto_cells(&["k"]),
                value: text_proto_cells(&["v"]),
                change_timestamp: None,
            }
        );
    }
}
