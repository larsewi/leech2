use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Indent all lines after the first by prepending `prefix`.
pub fn indent(text: &str, prefix: &str) -> String {
    text.replace('\n', &format!("\n{}", prefix))
}

/// Format a protobuf timestamp as a human-readable UTC string.
pub fn format_timestamp(timestamp: &prost_types::Timestamp) -> String {
    chrono::DateTime::from_timestamp(timestamp.seconds, 0).map_or("N/A".into(), |datetime| {
        datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    })
}
