use prost_types::Timestamp;

use crate::cell::Cell;
use crate::proto::patch::Insert as ProtoInsert;
use crate::record::TimestampedRecordMap;

pub type InsertMap = TimestampedRecordMap;

impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoInsert {
    fn from((key, (value, change_timestamp)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        ProtoInsert {
            key: key.into_iter().map(Into::into).collect(),
            value: value.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;

    use crate::proto::record::Record as ProtoRecord;

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
