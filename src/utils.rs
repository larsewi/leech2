use std::time::{SystemTime, UNIX_EPOCH};

use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

pub fn get_timestamp() -> Result<i32, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i32)
        .map_err(|_| "system time before UNIX epoch")
}

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
