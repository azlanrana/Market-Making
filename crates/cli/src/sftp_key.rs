//! Validate SFTP private key path before opening (clearer than libssh2 open errors).

use anyhow::{bail, Result};
use std::path::Path;

/// Fails fast if the key file is missing — common causes: wrong cwd, or
/// `CRYPTO_COM_SFTP_KEY_PATH` left set to a tutorial placeholder.
pub fn ensure_private_key_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.exists() {
        let cwd = std::env::current_dir()
            .map(|d| d.display().to_string())
            .unwrap_or_else(|_| "(unknown)".to_string());
        bail!(
            "SFTP private key path does not exist: {}\n  Current directory: {}\n  Fix: use an absolute path (e.g. .../mm/user080 (1) 2), or cd to mm/ and use ./user080 (1) 2\n  If you exported a placeholder, run: unset CRYPTO_COM_SFTP_KEY_PATH",
            path,
            cwd
        );
    }
    if !p.is_file() {
        bail!("SFTP private key path is not a file: {}", path);
    }
    Ok(())
}
