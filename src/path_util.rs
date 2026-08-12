//! Shared path-resolution utilities.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};

/// Resolves *path* to an absolute path using *base* as the reference
/// directory. Relative paths are joined onto *base*; absolute paths are
/// returned unchanged.
pub(crate) fn absolute_path(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize_path(path.to_path_buf())
    } else {
        normalize_path(base.join(path))
    }
}

/// Resolves *path* to an absolute path using the current working directory as
/// the reference directory.
pub(crate) fn absolute_path_cwd(path: &Path) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to determine current directory")?;
    Ok(absolute_path(path, &cwd))
}

/// Removes `.` components and resolves `..` components.
pub(crate) fn normalize_path(path: PathBuf) -> PathBuf {
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

/// Detects the nearest git root from `start`, or returns `start`.
#[must_use]
pub(crate) fn repo_root_or_cwd(start: &Path) -> PathBuf {
    for dir in start.ancestors() {
        let git = dir.join(".git");
        if git.exists() {
            return dir.to_path_buf();
        }
    }
    start.to_path_buf()
}

/// Builtin default cache directory used when neither the spec nor settings
/// specify one: `$HOME/.cache/hpc-compose`, falling back to
/// `./.cache/hpc-compose` when `HOME` is unset.
///
/// This is the single source of truth for the builtin default shared by the
/// context resolver and cache commands. It deliberately does NOT
/// expand a leading `~` or honor `XDG_CACHE_HOME`; callers that need
/// spec-relative resolution (or the planner's distinct `"~"` literal fallback)
/// handle that separately.
pub(crate) fn default_cache_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".cache/hpc-compose")
}

/// Default path for the generated Slurm batch script, next to the compose
/// file. This is purely lexical: it does not normalize or access the path.
pub(crate) fn default_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.sbatch")
}

/// Default path for the generated local launcher script, next to the compose
/// file. This is purely lexical: it does not normalize or access the path.
pub(crate) fn default_local_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.local.sh")
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

/// Returns a user-facing issue for cache paths that violate cluster policy.
#[must_use]
pub(crate) fn cache_path_policy_issue(path: &Path) -> Option<String> {
    if is_node_local_path(&path.to_string_lossy()) {
        return Some(format!(
            "x-slurm.cache_dir resolves to '{}', which is typically node-local and not shared; choose a shared filesystem path instead",
            path.display()
        ));
    }
    None
}

/// Returns a user-facing issue when a resolved `x-slurm.runtime_root` override
/// points at a node-local path, which would hide per-job logs and state from
/// compute nodes.
#[must_use]
pub(crate) fn runtime_root_policy_issue(path: &Path) -> Option<String> {
    if is_node_local_path(&path.to_string_lossy()) {
        return Some(format!(
            "x-slurm.runtime_root resolves to '{}', which is typically node-local and not shared; choose a shared filesystem path so per-job logs and state stay visible from compute nodes",
            path.display()
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use std::fs;

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

    #[test]
    fn repo_root_or_cwd_preserves_nearest_marker_and_lexical_fallback() {
        let tmp = tempfile::tempdir().expect("tmp");
        let outer = tmp.path().join("outer");
        let inner = outer.join("inner");
        let nested = inner.join("nested");
        fs::create_dir_all(outer.join(".git")).expect("outer git directory");
        fs::create_dir_all(&nested).expect("nested directory");
        fs::write(inner.join(".git"), "gitdir: ../worktree").expect("inner git file");

        assert_eq!(repo_root_or_cwd(&nested), inner);
        assert_eq!(repo_root_or_cwd(&inner), inner);

        fs::remove_file(inner.join(".git")).expect("remove inner git file");
        let segment = outer.join("segment");
        let lexical_start = segment.join("../segment/leaf");
        fs::create_dir_all(&lexical_start).expect("lexical nested directory");
        assert_eq!(repo_root_or_cwd(&lexical_start), segment.join(".."));

        let plain = tmp.path().join("plain/current");
        fs::create_dir_all(&plain).expect("plain directory");
        assert_eq!(repo_root_or_cwd(&plain), plain);

        let root = tmp.path().ancestors().last().expect("filesystem root");
        assert_eq!(repo_root_or_cwd(root), root);
    }

    #[test]
    fn generated_script_paths_preserve_absolute_bare_and_nested_parents() {
        let cases = [
            (
                "/tmp/project/compose.yaml",
                "/tmp/project/hpc-compose.sbatch",
                "/tmp/project/hpc-compose.local.sh",
            ),
            ("compose.yaml", "hpc-compose.sbatch", "hpc-compose.local.sh"),
            (
                "nested/project/compose.yaml",
                "nested/project/hpc-compose.sbatch",
                "nested/project/hpc-compose.local.sh",
            ),
            (
                "nested/../compose.yaml",
                "nested/../hpc-compose.sbatch",
                "nested/../hpc-compose.local.sh",
            ),
        ];

        for (spec, batch, local) in cases {
            assert_eq!(default_script_path(Path::new(spec)), PathBuf::from(batch));
            assert_eq!(
                default_local_script_path(Path::new(spec)),
                PathBuf::from(local)
            );
        }
    }

    #[test]
    fn default_cache_dir_reads_home_with_the_existing_fallback() {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        assert_eq!(default_cache_dir(), home.join(".cache/hpc-compose"));
    }

    #[test]
    fn node_local_policy_preserves_roots_boundaries_and_exact_messages() {
        for path in [
            "/tmp",
            "/tmp/job",
            "/var/tmp/job",
            "/private/tmp/job",
            "/dev/shm/job",
        ] {
            assert!(is_node_local_path(path), "path {path:?}");
        }
        for path in ["tmp/job", "/tmp2/job", "/var/tmp2/job", "/shared/job"] {
            assert!(!is_node_local_path(path), "path {path:?}");
        }

        assert_eq!(
            cache_path_policy_issue(Path::new("/tmp/hpc-compose")),
            Some(
                "x-slurm.cache_dir resolves to '/tmp/hpc-compose', which is typically node-local and not shared; choose a shared filesystem path instead"
                    .to_string()
            )
        );
        assert_eq!(
            runtime_root_policy_issue(Path::new("/tmp/runs")),
            Some(
                "x-slurm.runtime_root resolves to '/tmp/runs', which is typically node-local and not shared; choose a shared filesystem path so per-job logs and state stay visible from compute nodes"
                    .to_string()
            )
        );
        assert_eq!(cache_path_policy_issue(Path::new("/shared/cache")), None);
        assert_eq!(runtime_root_policy_issue(Path::new("/shared/runs")), None);
    }
}
