use std::collections::HashMap;

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::Cell;
use crate::proto::patch::Insert as ProtoInsert;
use crate::proto::record::Record as ProtoRecord;
use crate::record::Record;

pub type InsertMap = HashMap<Vec<Cell>, (Vec<Cell>, Option<Timestamp>)>;

impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoInsert {
    fn from((key, (value, change_timestamp)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        ProtoInsert {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

/// Decode a block's inserts into an [`InsertMap`] keyed by each record's key.
pub fn decode_proto_inserts(protos: Vec<ProtoRecord>) -> Result<InsertMap> {
    let mut inserts = HashMap::with_capacity(protos.len());
    for proto in protos {
        let record = Record::try_from(proto)?;
        inserts.insert(record.key, (record.value, None));
    }
    Ok(inserts)
}

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;

    use crate::cell::text_proto_cells;

    // Patches from older agents carry inserts as Record, so the two must stay
    // wire-compatible.
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
