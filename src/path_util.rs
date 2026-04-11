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
