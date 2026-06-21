//! Best-effort provenance pinned into each tracked submission record so a run
//! self-describes what produced it: the tool version, the git state of the
//! working tree, and the per-service image reference.
//!
//! Capturing provenance is read-only and laptop-side (static-safe): it never
//! contacts a scheduler and never fabricates a git SHA — git state is `None`
//! outside a working tree or when git is unavailable.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

/// Git state captured at submit time; only populated inside a working tree.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitProvenance {
    pub sha: String,
    pub dirty: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Provenance pinned into a tracked submission record. Descriptive only.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobProvenance {
    pub tool_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitProvenance>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub image_refs: BTreeMap<String, String>,
}

/// Collects provenance for a submission: the tool version, the git state of
/// `repo_root` (when it is a working tree), and the per-service image refs.
#[must_use]
pub fn collect_provenance(
    repo_root: &Path,
    tool_version: &str,
    image_refs: BTreeMap<String, String>,
) -> JobProvenance {
    JobProvenance {
        tool_version: tool_version.to_string(),
        git: read_git_provenance(repo_root),
        image_refs,
    }
}

/// Reads the git HEAD SHA, dirty flag, and branch for `repo_root`. Returns
/// `None` on ANY failure (not a git repo, git missing, no commit, command
/// error) so a fabricated SHA is never written.
#[must_use]
pub fn read_git_provenance(repo_root: &Path) -> Option<GitProvenance> {
    let sha = git_output(repo_root, &["rev-parse", "HEAD"])?;
    if sha.trim().is_empty() {
        return None;
    }
    let porcelain = git_output(repo_root, &["status", "--porcelain"])?;
    let branch = git_output(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])
        .map(|branch| branch.trim().to_string())
        .filter(|branch| !branch.is_empty() && branch != "HEAD");
    Some(parse_git_provenance(&sha, &porcelain, branch))
}

/// Pure assembly of a [`GitProvenance`] from raw git outputs, split out so the
/// dirty/branch logic is unit-testable without invoking git.
fn parse_git_provenance(sha: &str, porcelain: &str, branch: Option<String>) -> GitProvenance {
    GitProvenance {
        sha: sha.trim().to_string(),
        dirty: !porcelain.trim().is_empty(),
        branch,
    }
}

fn git_output(repo_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_git_provenance_is_none_outside_a_repo() {
        let tmp = tempfile::tempdir().expect("tmp");
        assert!(read_git_provenance(tmp.path()).is_none());
    }

    #[test]
    fn parse_git_provenance_sets_dirty_from_porcelain() {
        let clean = parse_git_provenance("abc123\n", "", Some("main".into()));
        assert_eq!(clean.sha, "abc123");
        assert!(!clean.dirty);
        assert_eq!(clean.branch.as_deref(), Some("main"));

        let dirty = parse_git_provenance("abc123", " M src/x.rs\n", None);
        assert_eq!(dirty.sha, "abc123");
        assert!(dirty.dirty);
        assert!(dirty.branch.is_none());
    }

    #[test]
    fn collect_provenance_always_sets_tool_version_and_image_refs() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut refs = BTreeMap::new();
        refs.insert("app".to_string(), "docker://x:1".to_string());
        let prov = collect_provenance(tmp.path(), "9.9.9", refs);
        assert_eq!(prov.tool_version, "9.9.9");
        assert_eq!(
            prov.image_refs.get("app").map(String::as_str),
            Some("docker://x:1")
        );
        assert!(prov.git.is_none());
    }
}
