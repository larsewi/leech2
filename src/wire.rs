use prost::Message;

use crate::config;
use crate::proto::patch::Patch;

/// Zstd frame magic number (little-endian).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Encode a Patch to protobuf, optionally compressing with zstd.
pub fn encode_patch(patch: &Patch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    patch.encode(&mut buf)?;

    let config = config::Config::get()?;
    if !config.compression {
        log::info!(
            "Patch encoded: {} bytes protobuf (compression disabled)",
            buf.len()
        );
        return Ok(buf);
    }

    let compressed = zstd::encode_all(buf.as_slice(), config.compression_level)?;
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
pub fn decode_patch(data: &[u8]) -> Result<Patch, Box<dyn std::error::Error>> {
    let bytes = if data.starts_with(&ZSTD_MAGIC) {
        zstd::decode_all(data)?
    } else {
        data.to_vec()
    };
    let patch = Patch::decode(bytes.as_slice())?;
    Ok(patch)
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
        assert_eq!(patch.head_hash, "");
        assert_eq!(patch.num_blocks, 0);
        assert!(patch.payload.is_none());
    }
}
