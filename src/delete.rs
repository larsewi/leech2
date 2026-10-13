use std::collections::HashMap;

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::Cell;
use crate::proto::patch::Delete as ProtoDelete;
use crate::proto::record::Record as ProtoRecord;
use crate::record::Record;

pub type DeleteMap = HashMap<Vec<Cell>, (Vec<Cell>, Option<Timestamp>)>;

/// Patches carry only the key of a delete, so the deleted value is dropped.
impl From<(Vec<Cell>, (Vec<Cell>, Option<Timestamp>))> for ProtoDelete {
    fn from((key, (_, change_timestamp)): (Vec<Cell>, (Vec<Cell>, Option<Timestamp>))) -> Self {
        ProtoDelete {
            key: key.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

/// Decode a block's deletes into a [`DeleteMap`] keyed by each record's key.
pub fn decode_proto_deletes(protos: Vec<ProtoRecord>) -> Result<DeleteMap> {
    let mut deletes = HashMap::with_capacity(protos.len());
    for proto in protos {
        let record = Record::try_from(proto)?;
        deletes.insert(record.key, (record.value, None));
    }
    Ok(deletes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;

    use crate::cell::{text_cells, text_proto_cells};

    #[test]
    fn test_delete_from_drops_value() {
        let timestamp = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let delete = ProtoDelete::from((text_cells(&["k"]), (text_cells(&["v"]), Some(timestamp))));
        assert_eq!(
            delete,
            ProtoDelete {
                key: text_proto_cells(&["k"]),
                change_timestamp: Some(timestamp),
            }
        );
    }

    // Patches from agents older than leech2 5.7.0 carry deletes without a
    // change timestamp.
    #[test]
    fn test_delete_without_change_timestamp_decodes() {
        let old = ProtoDelete {
            key: text_proto_cells(&["k"]),
            change_timestamp: None,
        };
        let decoded = ProtoDelete::decode(old.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.change_timestamp, None);
        assert_eq!(decoded.key, text_proto_cells(&["k"]));
    }

    #[test]
    fn test_decode_proto_deletes_has_no_change_timestamp() {
        let deletes = decode_proto_deletes(vec![ProtoRecord {
            key: text_proto_cells(&["k"]),
            value: text_proto_cells(&["v"]),
        }])
        .unwrap();
        assert_eq!(deletes[&text_cells(&["k"])], (text_cells(&["v"]), None));
    }
}
