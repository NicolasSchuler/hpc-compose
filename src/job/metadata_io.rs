use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub(super) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let serialized =
        serde_json::to_vec_pretty(value).context("failed to serialize job metadata")?;
    // Atomic, owner-only write via a per-writer unique temp file + rename, so
    // concurrent runs on a shared filesystem never publish (or observe) a torn
    // record, do not collide on a fixed `*.json.tmp` name, and do not expose
    // potentially sensitive command/sweep/config metadata to other users.
    crate::secure_io::write_atomic(path, &serialized, true)
        .context(format!("failed to write {}", path.display()))
}

pub(super) fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).context(format!("failed to parse {}", path.display()))
}

/// Read an optional JSON file, distinguishing "legitimately absent" from "broken".
///
/// A missing file (`NotFound`) is an expected, silent `None`. A corrupt/truncated
/// file or any other IO error is a *degraded* `None`: we emit a single `WARN` line
/// naming the path and error so tracked jobs no longer vanish silently, then return
/// `None` to preserve the caller's fall-through behavior.
pub(super) fn read_json_optional<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    match read_json::<T>(path) {
        Ok(value) => Some(value),
        Err(err) => {
            let is_not_found = err
                .chain()
                .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
                .any(|io_err| io_err.kind() == std::io::ErrorKind::NotFound);
            if !is_not_found {
                crate::diagnostics::warn_with_code(
                    "corrupt_job_record",
                    format!("{}: {err:#}", path.display()),
                );
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Fixture {
        name: String,
        count: u32,
    }

    #[test]
    fn write_json_preserves_exact_pretty_bytes_and_overwrites_atomically() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("nested/metadata.json");

        write_json(
            &path,
            &Fixture {
                name: "first".to_string(),
                count: 123,
            },
        )
        .expect("first write");
        assert_eq!(
            fs::read(&path).expect("first bytes"),
            b"{\n  \"name\": \"first\",\n  \"count\": 123\n}"
        );

        let replacement = Fixture {
            name: "x".to_string(),
            count: 4,
        };
        write_json(&path, &replacement).expect("replacement write");
        assert_eq!(
            fs::read(&path).expect("replacement bytes"),
            b"{\n  \"name\": \"x\",\n  \"count\": 4\n}"
        );
        assert_eq!(
            read_json::<Fixture>(&path).expect("read replacement"),
            replacement
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_json_uses_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("metadata.json");
        write_json(
            &path,
            &Fixture {
                name: "private".to_string(),
                count: 1,
            },
        )
        .expect("write");

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn read_json_optional_distinguishes_absent_and_corrupt_fallthrough() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("missing.json");
        assert_eq!(read_json_optional::<Fixture>(&missing), None);

        let corrupt = tmpdir.path().join("corrupt.json");
        fs::write(&corrupt, b"{ truncated").expect("corrupt fixture");
        assert_eq!(read_json_optional::<Fixture>(&corrupt), None);
    }
}
