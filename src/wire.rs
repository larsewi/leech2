use std::io::Read;

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::config::Config;
use crate::proto::patch::Patch;

/// Zstd frame magic number (little-endian).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Upper bound on the decompressed size of a patch. A zstd frame can claim a
/// tiny compressed size while expanding to gigabytes (a "decompression bomb").
/// Patches decoded here may arrive from an untrusted peer, so refuse to
/// allocate more than this; the ceiling is far above any realistic patch.
const MAX_DECOMPRESSED_PATCH_SIZE: u64 = 1 << 30; // 1 GiB

/// Encode a Patch to protobuf, optionally compressing with zstd.
pub fn encode_patch(config: &Config, patch: &Patch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    patch.encode(&mut buf)?;

    if !config.compression.enable {
        log::info!(
            "Patch encoded: {} bytes protobuf (compression disabled)",
            buf.len()
        );
        return Ok(buf);
    }

    let compressed = zstd::encode_all(buf.as_slice(), config.compression.level)?;
    log::info!(
        "Patch encoded: {} bytes protobuf, {} bytes compressed ({:.0}% reduction)",
        buf.len(),
        compressed.len(),
        if buf.is_empty() {
            0.0
        } else {
            (1.0 - compressed.len() as f64 / buf.len() as f64) * 100.0
        }
    );
    Ok(compressed)
}

/// Decode a Patch from protobuf, auto-detecting zstd compression.
///
/// If the data starts with the zstd frame magic number, it is decompressed
/// first. Otherwise, it is treated as raw protobuf.
pub fn decode_patch(data: &[u8]) -> Result<Patch> {
    let bytes = if data.starts_with(&ZSTD_MAGIC) {
        decompress_bounded(data, MAX_DECOMPRESSED_PATCH_SIZE)?
    } else {
        data.to_vec()
    };
    let patch = Patch::decode(bytes.as_slice())?;
    Ok(patch)
}

/// Decompress a zstd frame, refusing to produce more than `max` bytes of
/// output so a malicious frame cannot exhaust memory.
fn decompress_bounded(data: &[u8], max: u64) -> Result<Vec<u8>> {
    let decoder =
        zstd::stream::read::Decoder::new(data).context("failed to initialize zstd decoder")?;
    let mut bytes = Vec::new();
    // Read one byte past the limit so output that exactly fills `max` is still
    // accepted while anything larger is detected and rejected.
    decoder
        .take(max + 1)
        .read_to_end(&mut bytes)
        .context("failed to decompress patch")?;
    if bytes.len() as u64 > max {
        bail!("decompressed patch exceeds the maximum allowed size of {max} bytes");
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_corrupted_protobuf() {
        let garbage = b"this is not valid protobuf";
        let result = decode_patch(garbage);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_invalid_zstd() {
        // Starts with zstd magic but the rest is garbage
        let mut data = ZSTD_MAGIC.to_vec();
        data.extend_from_slice(b"not valid zstd content");
        let result = decode_patch(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_empty_input() {
        // Empty protobuf decodes to a default Patch (all fields zero/empty)
        let result = decode_patch(b"");
        assert!(result.is_ok());
        let patch = result.unwrap();
        assert_eq!(patch.head, "");
        assert_eq!(patch.num_blocks, 0);
        assert!(patch.deltas.is_empty());
        assert!(patch.states.is_empty());
    }

    #[test]
    fn test_decompress_bounded_rejects_oversized_output() {
        // A small frame that expands past the cap must be rejected rather than
        // allocated in full.
        let original = vec![0u8; 1_000_000];
        let compressed = zstd::encode_all(original.as_slice(), 0).unwrap();
        assert!(compressed.len() < 1_000_000, "expected high compression");

        let err = decompress_bounded(&compressed, 1024).err().unwrap();
        let msg = format!("{:#}", err);
        assert!(msg.contains("maximum allowed size"), "got: {msg}");
    }

    #[test]
    fn test_decompress_bounded_accepts_output_within_limit() {
        let original = vec![7u8; 1000];
        let compressed = zstd::encode_all(original.as_slice(), 0).unwrap();
        let out = decompress_bounded(&compressed, 1_000_000).unwrap();
        assert_eq!(out, original);
    }
}
