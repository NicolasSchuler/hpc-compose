use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

pub(super) fn sha256_reader(mut reader: impl Read) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).context(format!("failed to open {}", path.display()))?;
    sha256_reader(file).context(format!("failed to read {}", path.display()))
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

    #[test]
    fn sha256_file_matches_digest_across_multiple_read_buffers() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("large.bin");
        let bytes = (0..(8192 * 3 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&file, &bytes).expect("write");
        let expected = hex::encode(Sha256::digest(&bytes));

        assert_eq!(sha256_file(&file).expect("hash"), expected);
    }

    #[test]
    fn sha256_file_preserves_open_and_read_error_contexts() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("missing.bin");
        let error = sha256_file(&missing).expect_err("missing file should fail");
        assert_eq!(
            error.to_string(),
            format!("failed to open {}", missing.display())
        );

        #[cfg(unix)]
        {
            let unreadable_as_file = tmpdir.path().join("directory.bin");
            fs::create_dir(&unreadable_as_file).expect("create directory");
            let error = sha256_file(&unreadable_as_file).expect_err("directory read should fail");
            assert_eq!(
                error.to_string(),
                format!("failed to read {}", unreadable_as_file.display())
            );
        }
    }
}
