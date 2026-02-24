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
        log::info!("Patch encoded: {} bytes protobuf (compression disabled)", buf.len());
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
pub fn decode_patch(data: &[u8]) -> Result<Patch, Box<dyn std::error::Error>> {
    let bytes = if data.starts_with(&ZSTD_MAGIC) {
        zstd::decode_all(data)?
    } else {
        data.to_vec()
    };
    let patch = Patch::decode(bytes.as_slice())?;
    Ok(patch)
}
