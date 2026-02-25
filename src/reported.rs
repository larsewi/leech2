use crate::storage;

const REPORTED_FILE: &str = "REPORTED";

pub fn load() -> Result<Option<String>, Box<dyn std::error::Error>> {
    match storage::load(REPORTED_FILE)? {
        Some(data) => {
            let hash = String::from_utf8(data)?.trim().to_string();
            log::info!("Reported hash is '{:.7}...'", hash);
            Ok(Some(hash))
        }
        None => {
            log::debug!("No REPORTED file found");
            Ok(None)
        }
    }
}

pub fn save(hash: &str) -> Result<(), Box<dyn std::error::Error>> {
    storage::save(REPORTED_FILE, hash.as_bytes())?;
    log::info!("Updated reported to '{:.7}...'", hash);
    Ok(())
}
