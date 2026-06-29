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
        if restricted {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(contents.as_ref())?;
        // Force the mode even if the file pre-existed with looser permissions,
        // since OpenOptions::mode only applies on creation.
        if restricted {
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
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
    write_atomic_with_mode(
        path.as_ref(),
        contents,
        AtomicFileMode::Restricted(restricted),
    )
}

/// Atomically writes `contents` to `path`, preserving the existing file mode
/// when replacing a regular file on Unix.
///
/// This is useful for user-authored inputs such as compose files: a private
/// `0600` file must stay private after an atomic rewrite, while normal files
/// should keep their original readability. If no regular destination exists,
/// `restricted_if_new` controls the creation mode just like [`write_atomic`].
pub fn write_atomic_preserving_mode(
    path: impl AsRef<Path>,
    contents: impl AsRef<[u8]>,
    restricted_if_new: bool,
) -> io::Result<()> {
    let path = path.as_ref();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Ok(meta) = fs::symlink_metadata(path)
            && meta.file_type().is_file()
        {
            return write_atomic_with_mode(
                path,
                contents,
                AtomicFileMode::Unix(meta.permissions().mode() & 0o777),
            );
        }
    }
    write_atomic(path, contents, restricted_if_new)
}

#[derive(Clone, Copy)]
enum AtomicFileMode {
    Restricted(bool),
    #[cfg(unix)]
    Unix(u32),
}

fn write_atomic_with_mode(
    path: &Path,
    contents: impl AsRef<[u8]>,
    mode: AtomicFileMode,
) -> io::Result<()> {
    // Create the temp file with O_EXCL semantics (`create_new`) so a symlink or
    // entry pre-planted at the temp path by another user on a shared filesystem
    // fails loudly instead of being followed/reused. Retry with a fresh suffix
    // on the rare collision.
    let mut last_err = None;
    for attempt in 0..16 {
        let tmp = unique_temp_path(path, attempt);
        match create_exclusive(&tmp, mode) {
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
fn create_exclusive(path: &Path, mode: AtomicFileMode) -> io::Result<fs::File> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(match mode {
            AtomicFileMode::Restricted(true) => 0o600,
            AtomicFileMode::Restricted(false) => 0o666,
            AtomicFileMode::Unix(mode) => mode,
        });
    }
    #[cfg(not(unix))]
    {
        let AtomicFileMode::Restricted(_) = mode;
    }
    let file = opts.open(path)?;
    force_created_mode(&file, mode)?;
    Ok(file)
}

fn force_created_mode(file: &fs::File, mode: AtomicFileMode) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        match mode {
            AtomicFileMode::Restricted(true) => {
                file.set_permissions(fs::Permissions::from_mode(0o600))
            }
            AtomicFileMode::Restricted(false) => Ok(()),
            AtomicFileMode::Unix(mode) => file.set_permissions(fs::Permissions::from_mode(mode)),
        }
    }
    #[cfg(not(unix))]
    {
        let AtomicFileMode::Restricted(_) = mode;
        let _ = file;
        Ok(())
    }
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

/// Advisory-lock mode for [`with_flock`].
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum LockKind {
    /// Shared (read) lock — multiple holders may coexist.
    Shared,
    /// Exclusive (write) lock — at most one holder.
    Exclusive,
}

/// Runs `f` while holding an advisory `flock(2)` lock on the sidecar file
/// `lock_path`, then releases the lock.
///
/// `lock_path` must be a dedicated lock file (e.g. `<manifest>.json.lock`),
/// never the data file itself: callers that replace their data file via atomic
/// rename would otherwise lock a soon-discarded inode. The lock file is created
/// if absent and is intentionally **never removed** — unlinking it would let a
/// concurrent process lock a different inode and defeat the mutual exclusion.
///
/// The lock is **advisory and best-effort**:
/// * Acquisition is non-blocking with bounded retry up to `timeout`. If the
///   lock cannot be taken in time, `f` runs anyway *without* the lock rather
///   than hanging — a dead holder on a misbehaving filesystem must not wedge
///   the CLI.
/// * On platforms or filesystems where `flock` is unsupported (`ENOTSUP`,
///   `EOPNOTSUPP`, `ENOLCK` — common on some NFS/Lustre mounts) the lock is
///   skipped and `f` runs lock-free.
/// * The lock is released when the descriptor is dropped, including on panic or
///   process death, so it never leaks a stale lock.
///
/// Because `flock` is local-only on many network filesystems, this NARROWS but
/// does not eliminate a cross-node lost-update window; treat it as best effort,
/// not a cross-node guarantee.
///
/// `f` may return any error type; `with_flock` never substitutes an error of its
/// own (every lock failure degrades to running `f` directly), so the closure's
/// `Result` flows straight through.
/// Emits a one-line note when a lock could not be acquired, gated by
/// `HPC_COMPOSE_DEBUG_LOCKS` so the best-effort degradation is observable while
/// debugging without adding noise to normal runs (mirrors the
/// `HPC_COMPOSE_DEBUG_STAGING` convention used by the source-snapshot path).
fn debug_lock_note(lock_path: &Path, detail: &str) {
    if std::env::var_os("HPC_COMPOSE_DEBUG_LOCKS").is_some() {
        eprintln!("hpc-compose: lock {}: {detail}", lock_path.display());
    }
}

pub fn with_flock<T, E>(
    lock_path: &Path,
    kind: LockKind,
    timeout: std::time::Duration,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true).create(true);
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        // If we cannot even open the lock file (e.g. an unwritable directory),
        // degrade to a lock-free run rather than failing the caller's work.
        let file = match opts.open(lock_path) {
            Ok(file) => file,
            Err(_) => return f(),
        };

        let op = match kind {
            LockKind::Shared => libc::LOCK_SH,
            LockKind::Exclusive => libc::LOCK_EX,
        };
        let fd = file.as_raw_fd();
        let start = std::time::Instant::now();
        let mut locked = false;
        loop {
            // SAFETY: `fd` is a valid descriptor owned by `file` for the whole
            // duration of this call.
            let rc = unsafe { libc::flock(fd, op | libc::LOCK_NB) };
            if rc == 0 {
                locked = true;
                break;
            }
            match io::Error::last_os_error().raw_os_error() {
                // Held by someone else: retry with backoff until the deadline,
                // then degrade to a lock-free run.
                Some(code) if code == libc::EWOULDBLOCK => {
                    if start.elapsed() >= timeout {
                        debug_lock_note(
                            lock_path,
                            "timed out waiting for the lock; proceeding unlocked (best-effort, may race a concurrent writer on a shared filesystem)",
                        );
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                // flock unsupported on this filesystem/platform, or any other
                // error: skip locking entirely rather than fail the operation.
                _ => break,
            }
        }

        let result = f();

        if locked {
            // SAFETY: same valid descriptor. Best-effort explicit unlock; the
            // drop below would release it regardless.
            unsafe { libc::flock(fd, libc::LOCK_UN) };
        }
        drop(file);
        result
    }
    #[cfg(not(unix))]
    {
        let _ = (lock_path, kind, timeout);
        f()
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

    #[cfg(unix)]
    #[test]
    fn write_atomic_preserving_mode_keeps_private_destination_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("compose.yaml");
        fs::write(&path, b"old").expect("seed");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("seed mode");

        write_atomic_preserving_mode(&path, b"new", false).expect("rewrite");

        assert_eq!(fs::read(&path).expect("read"), b"new");
        let mode = fs::metadata(&path).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "existing private mode must survive rewrite");
    }

    #[test]
    fn with_flock_runs_closure_and_retains_lock_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lock = dir.path().join("x.lock");
        let value = with_flock(
            &lock,
            LockKind::Exclusive,
            std::time::Duration::from_secs(1),
            || Ok::<_, std::io::Error>(42),
        )
        .expect("with_flock");
        assert_eq!(value, 42);
        assert!(
            lock.exists(),
            "lock file is created and intentionally retained"
        );
    }

    // Proves the exclusive lock serializes a concurrent read-modify-write on the
    // LOCAL filesystem. It deliberately proves nothing about NFS/Lustre, where
    // flock may be local-only or unsupported (see `with_flock` docs).
    #[cfg(unix)]
    #[test]
    fn with_flock_serializes_concurrent_writers() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let counter = Arc::new(dir.path().join("counter"));
        let lock = Arc::new(dir.path().join("counter.lock"));
        fs::write(&*counter, "0").expect("seed");

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let counter = Arc::clone(&counter);
                let lock = Arc::clone(&lock);
                std::thread::spawn(move || {
                    with_flock(
                        &lock,
                        LockKind::Exclusive,
                        std::time::Duration::from_secs(10),
                        || {
                            let current: u64 = fs::read_to_string(&*counter)?
                                .trim()
                                .parse()
                                .expect("parse counter");
                            // Widen the race window so a missing lock corrupts it.
                            std::thread::yield_now();
                            fs::write(&*counter, (current + 1).to_string())?;
                            Ok::<(), std::io::Error>(())
                        },
                    )
                    .expect("with_flock");
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("join");
        }

        let total: u64 = fs::read_to_string(&*counter)
            .expect("read")
            .trim()
            .parse()
            .expect("parse");
        assert_eq!(total, 8, "exclusive flock must serialize read-modify-write");
    }

    #[test]
    #[cfg(unix)]
    fn write_atomic_replaces_symlinked_destination_without_clobbering_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = dir.path().join("outside");
        fs::write(&outside, b"old").expect("seed outside target");
        let dest = dir.path().join("state.json");
        symlink(&outside, &dest).expect("plant a symlink at the destination");

        write_atomic(&dest, b"new", false).expect("atomic write over a symlinked destination");

        // The rename replaced the symlink itself with a regular file ...
        let meta = fs::symlink_metadata(&dest).expect("dest metadata");
        assert!(
            !meta.file_type().is_symlink(),
            "destination must no longer be a symlink after write_atomic"
        );
        assert_eq!(fs::read(&dest).expect("read dest"), b"new");
        // ... and the symlink's former target was never followed/overwritten.
        assert_eq!(fs::read(&outside).expect("read outside"), b"old");
    }
}
