use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Format a list of byte vectors as a parenthesized, comma-separated string.
pub fn format_row(parts: &[Vec<u8>]) -> String {
    let items: Vec<_> = parts.iter().map(|b| String::from_utf8_lossy(b)).collect();
    format!("[{}]", items.join(", "))
}

/// Indent all lines after the first by prepending `prefix`.
pub fn indent(text: &str, prefix: &str) -> String {
    text.replace('\n', &format!("\n{}", prefix))
}

/// Format a protobuf timestamp as a human-readable UTC string.
pub fn format_timestamp(ts: &prost_types::Timestamp) -> String {
    chrono::DateTime::from_timestamp(ts.seconds, 0)
        .map_or("N/A".into(), |dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
}
