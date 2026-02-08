use sha1::{Digest, Sha1};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000";

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}
