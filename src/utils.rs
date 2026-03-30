use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Indent all lines after the first by prepending `prefix`.
///
/// The `Display` trait has no way to pass an indentation level, so nested
/// types format themselves starting at column 0. The parent calls this
/// function on the child's output to shift subsequent lines to the correct
/// depth. The first line is left unchanged because the parent already
/// positions it.
pub fn indent(text: &str, prefix: &str) -> String {
    text.replace('\n', &format!("\n{}", prefix))
}

/// Format a protobuf timestamp as a human-readable UTC string.
pub fn format_timestamp(timestamp: &prost_types::Timestamp) -> String {
    match chrono::DateTime::from_timestamp(timestamp.seconds, 0) {
        Some(datetime) => datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => "N/A".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash() {
        let hash = compute_hash(b"hello");
        assert_eq!(hash, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
    }

    #[test]
    fn test_indent() {
        assert_eq!(indent("a\nb\nc", "  "), "a\n  b\n  c");
    }

    #[test]
    fn test_indent_single_line() {
        assert_eq!(indent("hello", "  "), "hello");
    }

    #[test]
    fn test_format_timestamp() {
        let timestamp = prost_types::Timestamp {
            seconds: 1700000000,
            nanos: 0,
        };
        assert_eq!(format_timestamp(&timestamp), "2023-11-14 22:13:20 UTC");
    }

    #[test]
    fn test_genesis_hash() {
        assert_eq!(GENESIS_HASH.len(), 40);
        assert!(GENESIS_HASH.chars().all(|c| c == '0'));
    }
}
