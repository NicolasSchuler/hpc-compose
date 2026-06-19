//! Filesystem writes with restricted permissions and atomic replacement.
//!
//! Two concerns motivate this module:
//!
//! * Artifacts that may embed resolved secret values (the rendered sbatch
//!   script and the persisted job-state snapshot) must not be left
//!   group/world readable on shared HPC filesystems. [`write`] with
//!   `restricted = true` forces owner-only `0o600` on Unix.
//! * State files written from concurrent runs on a shared filesystem must not
//!   be observable in a torn, half-written state. [`write_atomic`] writes to a
//!   unique temporary file in the same directory and renames it into place.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Disambiguates concurrent temp files from the same process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Writes `contents` to `path`, truncating any existing file.
///
/// When `restricted` is true the file is created (and re-forced) with
/// owner-only `0o600` permissions on Unix, so secret-bearing artifacts are not
/// readable by other users on shared filesystems. On non-Unix targets
/// `restricted` has no effect.
pub fn write(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
    restricted: bool,
) -> io::Result<()> {
    let path = path.as_ref();
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        if restricted {
            opts.mode(0o600);
        }
        let mut file = opts.open(path)?;
        file.write_all(contents.as_ref())?;
        // Force the mode even if the file pre-existed with looser permissions,
        // since OpenOptions::mode only applies on creation.
        if restricted {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = restricted;
        fs::write(path, contents)
    }
}

/// Atomically writes `contents` to `path`.
///
/// Writes to a unique temporary file in the same directory and renames it over
/// the destination (rename within a directory is atomic on POSIX), so a
/// concurrent reader never observes a partially written file. When `restricted`
/// is true the temporary file — and therefore the destination — is `0o600`.
pub fn write_atomic(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
    restricted: bool,
) -> io::Result<()> {
    let path = path.as_ref();
    // Create the temp file with O_EXCL semantics (`create_new`) so a symlink or
    // entry pre-planted at the temp path by another user on a shared filesystem
    // fails loudly instead of being followed/reused. Retry with a fresh suffix
    // on the rare collision.
    let mut last_err = None;
    for attempt in 0..16 {
        let tmp = unique_temp_path(path, attempt);
        match create_exclusive(&tmp, restricted) {
            Ok(mut file) => {
                use std::io::Write;
                if let Err(err) = file.write_all(contents.as_ref()) {
                    drop(file);
                    let _ = fs::remove_file(&tmp);
                    return Err(err);
                }
                drop(file);
                return fs::rename(&tmp, path).inspect_err(|_| {
                    let _ = fs::remove_file(&tmp);
                });
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                last_err = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not create a unique temporary file",
        )
    }))
}

/// Creates `path` with O_CREAT|O_EXCL (refuses to follow a symlink or reuse an
/// existing entry), at `0o600` when `restricted` and `0o666` (umask-governed)
/// otherwise.
fn create_exclusive(path: &Path, restricted: bool) -> io::Result<fs::File> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(if restricted { 0o600 } else { 0o666 });
    }
    #[cfg(not(unix))]
    {
        let _ = restricted;
    }
    opts.open(path)
}

/// Builds a unique temp path next to `path` (same directory, so the subsequent
/// rename is atomic rather than a cross-device copy). Combines the process id, a
/// per-process atomic counter, a sub-second time component, and the retry
/// attempt for collision resistance; the actual safety against symlink attacks
/// comes from the O_EXCL create in [`create_exclusive`].
fn unique_temp_path(path: &Path, attempt: u32) -> PathBuf {
    let pid = std::process::id();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut name = path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".tmp.{pid}.{n}.{nanos}.{attempt}"));
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_atomic_round_trips_contents() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.json");
        write_atomic(&path, b"{\"a\":1}", false).expect("write");
        assert_eq!(fs::read_to_string(&path).expect("read"), "{\"a\":1}");
        // Overwrite is also atomic and leaves no temp residue.
        write_atomic(&path, b"{\"a\":2}", false).expect("rewrite");
        assert_eq!(fs::read_to_string(&path).expect("read"), "{\"a\":2}");
        let leftover = fs::read_dir(dir.path())
            .expect("read_dir")
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .count();
        assert_eq!(leftover, 0, "no temp files should remain");
    }

    #[cfg(unix)]
    #[test]
    fn restricted_write_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secret.sh");
        // Pre-create world-readable to prove the mode is forced down.
        fs::write(&path, b"old").expect("seed");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("seed mode");
        write(&path, b"secret", true).expect("write");
        let mode = fs::metadata(&path).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "restricted write must be owner-only");
    }
}
