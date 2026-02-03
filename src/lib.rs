use std::fs;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sha1::{Digest, Sha1};

// Include generated protobuf code
pub mod block {
    include!(concat!(env!("OUT_DIR"), "/block.rs"));
}

use block::Block;

#[unsafe(no_mangle)]
pub extern "C" fn init() {
    env_logger::init();
}

#[unsafe(no_mangle)]
pub extern "C" fn commit() -> i32 {
    let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i32,
        Err(e) => {
            log::error!("commit: failed to get system time: {}", e);
            return -1;
        }
    };

    let block = Block {
        version: 1,
        timestamp,
        parent: "0000000000000000000000000000000000000000".to_string(),
    };

    // Serialize the block to protobuf bytes
    let mut buf = Vec::new();
    if let Err(e) = block.encode(&mut buf) {
        log::error!("commit: failed to encode block: {}", e);
        return -1;
    }

    // Calculate SHA-1 hash of the serialized protobuf
    let mut hasher = Sha1::new();
    hasher.update(&buf);
    let hash = hasher.finalize();
    let hash_hex = format!("{:x}", hash);

    // Create .improved directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(".improved") {
        log::error!("commit: failed to create .improved directory: {}", e);
        return -1;
    }

    // Write the serialized block to .improved/<sha1>
    let path = format!(".improved/{}", hash_hex);
    let mut file = match fs::File::create(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("commit: failed to create block file {}: {}", path, e);
            return -1;
        }
    };

    if let Err(e) = file.write_all(&buf) {
        log::error!("commit: failed to write block to {}: {}", path, e);
        return -1;
    }

    // Update HEAD to point to the new block
    let head_path = ".improved/HEAD";
    let mut head_file = match fs::File::create(head_path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("commit: failed to create HEAD file: {}", e);
            return -1;
        }
    };

    if let Err(e) = head_file.write_all(hash_hex.as_bytes()) {
        log::error!("commit: failed to write HEAD: {}", e);
        return -1;
    }

    log::info!(
        "commit: created block {} (version={}, timestamp={}, parent={})",
        hash_hex,
        block.version,
        block.timestamp,
        block.parent
    );

    0
}
