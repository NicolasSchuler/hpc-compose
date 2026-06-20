//! Shared path-resolution utilities.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};

/// Resolves *path* to an absolute path using *base* as the reference
/// directory. Relative paths are joined onto *base*; absolute paths are
/// returned unchanged.
pub fn absolute_path(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize_path(path.to_path_buf())
    } else {
        normalize_path(base.join(path))
    }
}

/// Resolves *path* to an absolute path using the current working directory as
/// the reference directory.
pub fn absolute_path_cwd(path: &Path) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to determine current directory")?;
    Ok(absolute_path(path, &cwd))
}

/// Removes `.` components and resolves `..` components.
pub fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Builtin default cache directory used when neither the spec nor settings
/// specify one: `$HOME/.cache/hpc-compose`, falling back to
/// `./.cache/hpc-compose` when `HOME` is unset.
///
/// This is the single source of truth for the builtin default shared by the
/// context resolver and the output/doctor reporters. It deliberately does NOT
/// expand a leading `~` or honor `XDG_CACHE_HOME`; callers that need
/// spec-relative resolution (or the planner's distinct `"~"` literal fallback)
/// handle that separately.
pub(crate) fn default_cache_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".cache/hpc-compose")
}

/// Filesystem roots that are typically node-local and therefore unsafe for
/// data that must be visible from both the login node and compute nodes.
///
/// `lint` (authoring-time) and `preflight`/`planner` (submission-time) share
/// this single list so their advice never drifts apart.
pub(crate) const NODE_LOCAL_ROOTS: &[&str] = &["/tmp", "/var/tmp", "/private/tmp", "/dev/shm"];

/// Returns `true` when *path* lives under one of the [`NODE_LOCAL_ROOTS`].
pub(crate) fn is_node_local_path(path: &str) -> bool {
    let path = Path::new(path);
    NODE_LOCAL_ROOTS
        .iter()
        .any(|root| path == Path::new(root) || path.starts_with(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_path_keeps_absolute() {
        let result = absolute_path(Path::new("/foo/bar"), Path::new("/base"));
        assert_eq!(result, PathBuf::from("/foo/bar"));
    }

    #[test]
    fn absolute_path_joins_relative() {
        let result = absolute_path(Path::new("baz"), Path::new("/base"));
        assert_eq!(result, PathBuf::from("/base/baz"));
    }

    #[test]
    fn normalize_dots() {
        let result = normalize_path(PathBuf::from("/base/./foo/../bar"));
        assert_eq!(result, PathBuf::from("/base/bar"));
    }

    #[test]
    fn absolute_path_normalizes() {
        let result = absolute_path(Path::new("./foo/../bar"), Path::new("/base"));
        assert_eq!(result, PathBuf::from("/base/bar"));
    }
}
