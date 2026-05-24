use std::time::Duration;

use anyhow::{Result, bail};
use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

/// Parse a duration string into a `Duration`. Supports single-unit (`"30s"`, `"7d"`) and
/// compound (`"1d12h"`, `"1h30m"`) durations.
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).
pub fn parse_duration(s: &str) -> Result<Duration> {
    if s.is_empty() {
        bail!("empty duration string");
    }

    let mut total_seconds: u64 = 0;
    let mut number_start = None;

    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() {
            if number_start.is_none() {
                number_start = Some(i);
            }
        } else {
            let start = number_start.take().ok_or_else(|| {
                anyhow::anyhow!("invalid duration '{}': expected digit before '{}'", s, c)
            })?;
            let value: u64 = s[start..i]
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid duration '{}'", s))?;
            let multiplier = match c {
                's' => 1,
                'm' => SECONDS_PER_MINUTE,
                'h' => SECONDS_PER_HOUR,
                'd' => SECONDS_PER_DAY,
                'w' => SECONDS_PER_WEEK,
                _ => bail!("invalid duration suffix '{}' in '{}'", c, s),
            };
            total_seconds = total_seconds
                .checked_add(
                    value
                        .checked_mul(multiplier)
                        .ok_or_else(|| anyhow::anyhow!("duration overflow in '{}'", s))?,
                )
                .ok_or_else(|| anyhow::anyhow!("duration overflow in '{}'", s))?;
        }
    }

    if number_start.is_some() {
        bail!("invalid duration '{}': trailing digits without suffix", s);
    }

    Ok(Duration::from_secs(total_seconds))
}

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

/// Validate a column / field name. Rejects the empty string and any control
/// character (ASCII C0 / DEL plus the C1 range). A NUL would be treated as
/// a string terminator by some database drivers and tooling; newlines / CR
/// would corrupt log and audit output; other control characters are pure
/// visual deception when the identifier appears alongside other text.
pub fn validate_field_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("field name must not be empty");
    }
    if let Some(c) = name.chars().find(|c| c.is_control()) {
        bail!(
            "field name {:?} contains a control character (U+{:04X})",
            name,
            c as u32
        );
    }
    Ok(())
}

/// Join `handle` and surface any panic payload as a warning under `context`
/// (e.g. `"Background truncation thread"`). Without this, `let _ = handle.join();`
/// silently discards worker-thread panics.
pub fn join_logging_panics(handle: std::thread::JoinHandle<()>, context: &str) {
    if let Err(panic) = handle.join() {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");
        log::warn!("{} panicked: {}", context, message);
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

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("12h").unwrap(), Duration::from_secs(43200));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
    }

    #[test]
    fn test_parse_duration_weeks() {
        assert_eq!(parse_duration("2w").unwrap(), Duration::from_secs(1209600));
    }

    #[test]
    fn test_parse_duration_invalid_suffix() {
        assert!(parse_duration("10x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_number() {
        assert!(parse_duration("abcs").is_err());
    }

    #[test]
    fn test_parse_duration_empty() {
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn test_parse_duration_compound() {
        assert_eq!(
            parse_duration("1d12h").unwrap(),
            Duration::from_secs(SECONDS_PER_DAY + 12 * SECONDS_PER_HOUR)
        );
        assert_eq!(
            parse_duration("1h30m").unwrap(),
            Duration::from_secs(SECONDS_PER_HOUR + 30 * SECONDS_PER_MINUTE)
        );
        assert_eq!(
            parse_duration("1w2d3h4m5s").unwrap(),
            Duration::from_secs(
                SECONDS_PER_WEEK
                    + 2 * SECONDS_PER_DAY
                    + 3 * SECONDS_PER_HOUR
                    + 4 * SECONDS_PER_MINUTE
                    + 5
            )
        );
    }

    #[test]
    fn test_parse_duration_trailing_digits() {
        assert!(parse_duration("30").is_err());
    }
}
