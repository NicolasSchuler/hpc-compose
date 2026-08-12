use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

pub(super) fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).context(format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .context(format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn sha256_file_matches_known_vector() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("data.bin");
        fs::write(&file, "hello world").expect("write");

        assert_eq!(
            sha256_file(&file).expect("hash"),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }
}
