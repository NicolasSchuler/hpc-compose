//! Content-addressed snapshots of a project's working-tree *source* into the CAS.
//!
//! This is the login-node-side half of the dual-mode source-sync design: it
//! captures the working tree (including uncommitted edits) into an immutable,
//! deduplicated directory under `cache_dir/source/<key>` and returns its content
//! hash. The same primitive is what a laptop client would ship to the cluster
//! CAS over SSH — only the transport differs.
//!
//! Unlike the dataset/model store ([`crate::cache::dataset`]), which keys an
//! entry by an immutable upstream `(uri, revision)` and avoids re-reading large
//! trees, a source snapshot has no upstream pin: a *dirty* tree at a given git
//! SHA differs from a clean one. So the snapshot is keyed by the **content hash**
//! of the enumerated file set — identical content (anywhere on disk) dedups to
//! one entry, and any change to a file's path, bytes, or executable bit yields a
//! new hash and a new entry. That makes a dirty tree recoverable and pins the
//! exact source a run used (the hash is recorded into `JobProvenance`).
//!
//! Enumeration prefers git (`git ls-files -z --cached --others
//! --exclude-standard`) so it sees working-tree bytes and honors `.gitignore`
//! (excluding `.git/`, build output, virtualenvs); it falls back to a plain walk
//! that skips `.git/` when the tree is not a git repo or git is unavailable.
//!
//! A `.hpcignore` at the snapshot root (gitignore/dockerignore-style) excludes
//! additional paths on top of `.gitignore`, so a tracked file can still be kept
//! out of the snapshot (e.g. large fixtures, generated docs).

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::cache::dataset::{
    StagedInputAction, StagedInputKind, StagedInputProof, StagedInputSpec, ensure_staged_input,
};

/// The synthetic URI marker recorded in a source snapshot's manifest. A
/// snapshot's identity is its content hash (carried in the spec `revision`), so
/// the uri is a constant: two trees with identical content share one CAS entry
/// regardless of where they were snapshotted, so a laptop and a login node
/// dedup to the same key.
pub const SOURCE_SNAPSHOT_URI: &str = "source-tree";

/// Domain/version tag mixed into every source hash so the hashing scheme can
/// evolve later without silently colliding with snapshots from an older layout.
const SOURCE_HASH_DOMAIN: &[u8] = b"hpc-compose-source-v1\0";

/// The result of staging a working tree into the content-addressed store.
#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    /// The immutable staged directory (`cache_dir/source/<key>`).
    pub dir: PathBuf,
    /// The full hex SHA-256 content hash of the snapshot (also its manifest
    /// `content_digest`); record this into provenance.
    pub content_hash: String,
    /// Whether the snapshot was materialized fresh or an identical one reused.
    pub action: StagedInputAction,
    /// The number of files (and symlinks) captured.
    pub file_count: usize,
}

/// One enumerated working-tree entry, relative to the snapshot root.
struct SourceEntry {
    /// Path relative to the root, always using `/` separators (stable identity).
    rel: String,
    /// Absolute on-disk path to read content/link target from.
    abs: PathBuf,
    /// Whether the on-disk entry is a symlink (captured as a link, not followed).
    is_symlink: bool,
    /// Whether a regular file has any executable bit set (preserved in the hash).
    is_exec: bool,
}

/// Snapshots the working tree at `root` into `cache_dir/source/<key>` and returns
/// the staged directory plus its content hash.
///
/// Identical content reuses the existing entry without copying
/// ([`StagedInputAction::Reused`]); any change to a file's path, bytes, or exec
/// bit produces a new hash and a fresh entry ([`StagedInputAction::Built`]), so
/// distinct (e.g. dirty) states are captured independently and remain
/// retrievable.
///
/// # Errors
///
/// Returns an error when the tree cannot be enumerated or read, or when the
/// snapshot cannot be materialized into the store.
pub fn stage_source(root: &Path, cache_dir: &Path) -> Result<SourceSnapshot> {
    let entries = enumerate_source(root)
        .with_context(|| format!("failed to enumerate source tree at {}", root.display()))?;
    let content_hash = hash_source(&entries)
        .with_context(|| format!("failed to hash source tree at {}", root.display()))?;

    let spec = StagedInputSpec::new(
        StagedInputKind::Source,
        SOURCE_SNAPSHOT_URI,
        Some(content_hash.clone()),
    );
    let digest = content_hash.clone();
    let (dir, action) = ensure_staged_input(cache_dir, &spec, |dest| {
        copy_entries(&entries, dest)?;
        Ok(StagedInputProof {
            content_digest: Some(digest),
        })
    })
    .with_context(|| {
        format!(
            "failed to stage source snapshot into {}",
            cache_dir.display()
        )
    })?;

    Ok(SourceSnapshot {
        dir,
        content_hash,
        action,
        file_count: entries.len(),
    })
}

/// Enumerates the working-tree source file set rooted at `root`, sorted by
/// relative path. Prefers git (working-tree bytes, `.gitignore`-aware) and falls
/// back to a plain walk that skips `.git/`.
fn enumerate_source(root: &Path) -> Result<Vec<SourceEntry>> {
    let rels = match git_listed_files(root) {
        Some(rels) => rels,
        None => walk_files(root)?,
    };
    let ignore = HpcIgnore::load(root);
    let mut entries = Vec::with_capacity(rels.len());
    for rel in rels {
        // Defense in depth: never let an odd path (`..`, absolute) escape the
        // destination directory when the snapshot is copied.
        if !is_safe_rel(&rel) {
            continue;
        }
        // Honor a repo-root .hpcignore (extra excludes on top of .gitignore).
        if ignore.is_ignored(&rel) {
            continue;
        }
        let abs = root.join(&rel);
        // A listed path can be absent on disk (e.g. a tracked file deleted in
        // the working tree); the snapshot reflects the tree as it IS, so skip it.
        let meta = match fs::symlink_metadata(&abs) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        let file_type = meta.file_type();
        if file_type.is_symlink() {
            entries.push(SourceEntry {
                rel,
                abs,
                is_symlink: true,
                is_exec: false,
            });
        } else if file_type.is_file() {
            let is_exec = is_executable(&meta);
            entries.push(SourceEntry {
                rel,
                abs,
                is_symlink: false,
                is_exec,
            });
        }
        // Directories are implied by their files; special files (sockets, fifos,
        // devices) are not source and are skipped.
    }
    entries.sort_by(|a, b| a.rel.cmp(&b.rel));
    entries.dedup_by(|a, b| a.rel == b.rel);
    Ok(entries)
}

/// Lists working-tree files via git, or `None` when `root` is not a git repo or
/// git is unavailable. Tracked files are reported at their current on-disk bytes;
/// untracked-but-not-ignored files are included; `.gitignore`d paths are not.
fn git_listed_files(root: &Path) -> Option<Vec<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let mut rels = Vec::new();
    for chunk in output.stdout.split(|&byte| byte == 0) {
        if chunk.is_empty() {
            continue;
        }
        // `-z` emits raw, unquoted paths. Assume UTF-8: a non-UTF-8 path would
        // change identity under lossy conversion, so skip it rather than guess.
        if let Ok(rel) = std::str::from_utf8(chunk) {
            rels.push(rel.to_string());
        }
    }
    Some(rels)
}

/// Recursively lists files under `root` (relative, `/`-separated), skipping any
/// `.git` directory. The non-git fallback: it does not honor `.gitignore`.
fn walk_files(root: &Path) -> Result<Vec<String>> {
    let mut rels = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let listing =
            fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?;
        for entry in listing {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
            // VCS metadata is never source.
            if entry.file_name() == ".git" {
                continue;
            }
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("failed to read file type for {}", path.display()))?;
            // A symlink (even to a directory) reports as a symlink here, so we
            // never recurse through it — no traversal cycles.
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if let Ok(rel) = path.strip_prefix(root)
                && let Some(rel) = rel.to_str()
            {
                rels.push(rel.replace('\\', "/"));
            }
        }
    }
    Ok(rels)
}

/// Whether a relative path is safe to materialize under the destination: only
/// normal/current-dir components, never `..`, an absolute root, or a prefix.
fn is_safe_rel(rel: &str) -> bool {
    !rel.is_empty()
        && Path::new(rel)
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

/// Computes the deterministic SHA-256 content hash of the enumerated file set.
///
/// Each entry contributes its relative path, a type tag (`f`/`x`/`l`), and its
/// content: regular files mix a length prefix then the bytes (so no two files
/// can run together ambiguously); symlinks mix their target. The domain tag and
/// `NUL` separators make the framing unambiguous.
fn hash_source(entries: &[SourceEntry]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_HASH_DOMAIN);
    for entry in entries {
        hasher.update(entry.rel.as_bytes());
        hasher.update([0]);
        if entry.is_symlink {
            let target = fs::read_link(&entry.abs)
                .with_context(|| format!("failed to read symlink {}", entry.abs.display()))?;
            hasher.update(b"l\0");
            hasher.update(target.to_string_lossy().as_bytes());
            hasher.update([0]);
        } else {
            let bytes = fs::read(&entry.abs)
                .with_context(|| format!("failed to read {}", entry.abs.display()))?;
            hasher.update(if entry.is_exec { b"x\0" } else { b"f\0" });
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(&bytes);
        }
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Copies the enumerated entries into `dest`, recreating directory structure,
/// preserving file permissions (via [`fs::copy`]) and symlinks.
fn copy_entries(entries: &[SourceEntry], dest: &Path) -> Result<()> {
    for entry in entries {
        let target = dest.join(&entry.rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        if entry.is_symlink {
            let link_target = fs::read_link(&entry.abs)
                .with_context(|| format!("failed to read symlink {}", entry.abs.display()))?;
            symlink_file(&link_target, &target)
                .with_context(|| format!("failed to recreate symlink {}", target.display()))?;
        } else {
            fs::copy(&entry.abs, &target).with_context(|| {
                format!(
                    "failed to copy {} -> {}",
                    entry.abs.display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn is_executable(meta: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_meta: &fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn symlink_file(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(not(unix))]
fn symlink_file(_target: &Path, _link: &Path) -> std::io::Result<()> {
    // Source snapshots are only ever materialized on Unix (macOS authoring /
    // Linux login node). This stub keeps the crate compiling elsewhere.
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "symlinks are not supported on this platform",
    ))
}

/// A parsed `.hpcignore`: gitignore/dockerignore-style exclusion patterns applied
/// on top of git's `.gitignore`, so a *tracked* file can still be kept out of the
/// source snapshot. An absent file yields no rules (the snapshot is unchanged).
///
/// Supports `#` comments and blank lines, `!` negation (last matching rule wins),
/// a trailing `/` (directory only), a leading or internal `/` (root-anchored;
/// otherwise the pattern matches a path component at any depth), and `*`/`**`/`?`
/// globs (`**` spans `/`, `*` and `?` do not).
struct HpcIgnore {
    rules: Vec<IgnoreRule>,
}

struct IgnoreRule {
    regex: Regex,
    negated: bool,
    dir_only: bool,
    anchored: bool,
}

impl HpcIgnore {
    /// Loads `<root>/.hpcignore`; a missing or unreadable file yields no rules.
    fn load(root: &Path) -> Self {
        let contents = fs::read_to_string(root.join(".hpcignore")).unwrap_or_default();
        let rules = contents.lines().filter_map(IgnoreRule::parse).collect();
        HpcIgnore { rules }
    }

    /// Whether the `/`-separated relative file path is excluded. Each matching
    /// rule flips the verdict; a `!`-negated rule re-includes (last match wins).
    fn is_ignored(&self, rel: &str) -> bool {
        if self.rules.is_empty() {
            return false;
        }
        let components: Vec<&str> = rel.split('/').filter(|c| !c.is_empty()).collect();
        let mut ignored = false;
        for rule in &self.rules {
            if rule.matches(&components) {
                ignored = !rule.negated;
            }
        }
        ignored
    }
}

impl IgnoreRule {
    fn parse(raw: &str) -> Option<Self> {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (negated, rest) = match line.strip_prefix('!') {
            Some(rest) => (true, rest),
            None => (false, line),
        };
        let dir_only = rest.ends_with('/');
        let trimmed = rest.trim_end_matches('/');
        let anchored = trimmed.starts_with('/') || trimmed.contains('/');
        let pattern = trimmed.trim_start_matches('/');
        if pattern.is_empty() {
            return None;
        }
        let regex = Regex::new(&glob_to_regex(pattern)).ok()?;
        Some(IgnoreRule {
            regex,
            negated,
            dir_only,
            anchored,
        })
    }

    /// Matches against a file's path components. Anchored rules test each path
    /// prefix (root-relative); unanchored rules test each component (basename at
    /// any depth). A directory-only rule never matches the final (file)
    /// component — only an ancestor.
    fn matches(&self, components: &[&str]) -> bool {
        if components.is_empty() {
            return false;
        }
        let last = components.len() - 1;
        if self.anchored {
            for end in 0..components.len() {
                if self.dir_only && end == last {
                    continue;
                }
                if self.regex.is_match(&components[..=end].join("/")) {
                    return true;
                }
            }
        } else {
            for (index, component) in components.iter().enumerate() {
                if self.dir_only && index == last {
                    continue;
                }
                if self.regex.is_match(component) {
                    return true;
                }
            }
        }
        false
    }
}

/// Translates a gitignore-style glob into an anchored regex string. `**` spans
/// path separators; `*` and `?` do not. Other regex metacharacters are escaped.
fn glob_to_regex(glob: &str) -> String {
    let chars: Vec<char> = glob.chars().collect();
    let mut regex = String::from("^");
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '*' => {
                if chars.get(index + 1) == Some(&'*') {
                    regex.push_str(".*");
                    index += 2;
                    if chars.get(index) == Some(&'/') {
                        index += 1;
                    }
                    continue;
                }
                regex.push_str("[^/]*");
            }
            '?' => regex.push_str("[^/]"),
            c => {
                if ".+()[]{}^$|\\".contains(c) {
                    regex.push('\\');
                }
                regex.push(c);
            }
        }
        index += 1;
    }
    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, rel: &str, contents: &[u8]) {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("mkdir");
        }
        fs::write(path, contents).expect("write");
    }

    fn hash_at(root: &Path) -> String {
        hash_source(&enumerate_source(root).expect("enumerate")).expect("hash")
    }

    #[test]
    fn hash_is_deterministic_and_content_addressed_across_dirs() {
        let a = tempfile::tempdir().expect("a");
        let b = tempfile::tempdir().expect("b");
        for root in [a.path(), b.path()] {
            write(root, "src/main.rs", b"fn main() {}");
            write(root, "README.md", b"hi");
        }
        let ha = hash_at(a.path());
        assert_eq!(ha.len(), 64, "full sha-256 hex");
        assert!(ha.chars().all(|c| c.is_ascii_hexdigit()));
        // Identical content in different directories hashes identically.
        assert_eq!(ha, hash_at(b.path()));
        // Deterministic across repeated enumeration of the same tree.
        assert_eq!(ha, hash_at(a.path()));
    }

    #[test]
    fn hash_changes_with_content_and_path() {
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path();
        write(root, "a.txt", b"one");
        let base = hash_at(root);

        write(root, "a.txt", b"two");
        let changed_content = hash_at(root);
        assert_ne!(
            base, changed_content,
            "a content change must alter the hash"
        );

        fs::remove_file(root.join("a.txt")).expect("rm");
        write(root, "b.txt", b"two");
        let changed_path = hash_at(root);
        assert_ne!(
            changed_content, changed_path,
            "a path change must alter the hash"
        );
    }

    #[cfg(unix)]
    #[test]
    fn hash_changes_with_exec_bit() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path();
        write(root, "run.sh", b"echo hi");
        let before = hash_at(root);
        let mut perms = fs::metadata(root.join("run.sh"))
            .expect("meta")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(root.join("run.sh"), perms).expect("chmod");
        assert_ne!(before, hash_at(root), "the exec bit must alter the hash");
    }

    #[test]
    fn stage_source_materializes_copies_tree_then_reuses() {
        let src = tempfile::tempdir().expect("src");
        let cache = tempfile::tempdir().expect("cache");
        write(src.path(), "src/main.rs", b"fn main() {}");
        write(src.path(), "data/note.txt", b"note");

        let first = stage_source(src.path(), cache.path()).expect("first");
        assert_eq!(first.action, StagedInputAction::Built);
        assert_eq!(first.file_count, 2);
        assert!(first.dir.starts_with(cache.path().join("source")));
        assert_eq!(
            fs::read(first.dir.join("src/main.rs")).expect("read main"),
            b"fn main() {}"
        );
        assert_eq!(
            fs::read(first.dir.join("data/note.txt")).expect("read note"),
            b"note"
        );
        // Tracked by a sidecar so `cache list`/`prune` discover it.
        let sidecar = crate::cache::dataset::sidecar_manifest_path_for_suffix(&first.dir, "source");
        assert!(sidecar.is_file(), "source sidecar manifest written");

        // Re-staging identical content reuses the same entry without copying.
        let second = stage_source(src.path(), cache.path()).expect("second");
        assert_eq!(second.action, StagedInputAction::Reused);
        assert_eq!(second.dir, first.dir);
        assert_eq!(second.content_hash, first.content_hash);
    }

    #[test]
    fn stage_source_distinguishes_and_retains_dirty_state() {
        let src = tempfile::tempdir().expect("src");
        let cache = tempfile::tempdir().expect("cache");
        write(src.path(), "a.txt", b"clean");
        let clean = stage_source(src.path(), cache.path()).expect("clean");

        write(src.path(), "a.txt", b"dirty");
        let dirty = stage_source(src.path(), cache.path()).expect("dirty");

        assert_ne!(clean.content_hash, dirty.content_hash);
        assert_ne!(clean.dir, dirty.dir, "distinct content => distinct CAS dir");
        // Both snapshots remain independently retrievable (recoverable history).
        assert_eq!(
            fs::read(clean.dir.join("a.txt")).expect("clean read"),
            b"clean"
        );
        assert_eq!(
            fs::read(dirty.dir.join("a.txt")).expect("dirty read"),
            b"dirty"
        );
    }

    #[cfg(unix)]
    #[test]
    fn stage_source_preserves_symlinks() {
        let src = tempfile::tempdir().expect("src");
        let cache = tempfile::tempdir().expect("cache");
        write(src.path(), "real.txt", b"payload");
        std::os::unix::fs::symlink("real.txt", src.path().join("link.txt")).expect("symlink");

        let snap = stage_source(src.path(), cache.path()).expect("stage");
        let link = snap.dir.join("link.txt");
        assert!(
            fs::symlink_metadata(&link)
                .expect("lstat")
                .file_type()
                .is_symlink(),
            "symlink captured as a link, not dereferenced"
        );
        assert_eq!(
            fs::read_link(&link).expect("readlink"),
            Path::new("real.txt")
        );
    }

    #[test]
    fn walk_files_skips_git_directory() {
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path();
        write(root, "keep.rs", b"x");
        write(root, "nested/mod.rs", b"y");
        write(root, ".git/config", b"[core]");
        let mut rels = walk_files(root).expect("walk");
        rels.sort();
        assert!(rels.contains(&"keep.rs".to_string()));
        assert!(rels.contains(&"nested/mod.rs".to_string()));
        assert!(
            !rels.iter().any(|rel| rel.starts_with(".git")),
            "the .git dir must never be snapshotted: {rels:?}"
        );
    }

    #[test]
    fn is_safe_rel_rejects_traversal_and_absolute() {
        assert!(is_safe_rel("src/main.rs"));
        assert!(is_safe_rel("./a/b"));
        assert!(!is_safe_rel("../escape"));
        assert!(!is_safe_rel("a/../../escape"));
        assert!(!is_safe_rel("/abs/path"));
        assert!(!is_safe_rel(""));
    }

    fn ignore_from(lines: &str) -> HpcIgnore {
        HpcIgnore {
            rules: lines.lines().filter_map(IgnoreRule::parse).collect(),
        }
    }

    #[test]
    fn hpcignore_matches_common_patterns() {
        // Basename glob at any depth.
        let i = ignore_from("*.log");
        assert!(i.is_ignored("a/b/c.log"));
        assert!(i.is_ignored("x.log"));
        assert!(!i.is_ignored("a/b/c.txt"));

        // Directory (slashless) at any depth.
        let i = ignore_from("build/");
        assert!(i.is_ignored("build/out.o"));
        assert!(i.is_ignored("src/build/out.o"));
        assert!(!i.is_ignored("buildx"));

        // Root-anchored (leading slash).
        let i = ignore_from("/secret.txt");
        assert!(i.is_ignored("secret.txt"));
        assert!(!i.is_ignored("sub/secret.txt"));

        // Anchored relative path (internal slash).
        let i = ignore_from("docs/api");
        assert!(i.is_ignored("docs/api/index.html"));
        assert!(!i.is_ignored("src/docs/api/x"));

        // Negation re-includes (last match wins).
        let i = ignore_from("*.log\n!keep.log");
        assert!(i.is_ignored("debug.log"));
        assert!(!i.is_ignored("keep.log"));

        // Comments / blanks are skipped; an empty file ignores nothing.
        let i = ignore_from("# comment\n\n*.tmp");
        assert!(i.is_ignored("a.tmp"));
        assert!(!ignore_from("").is_ignored("anything"));
    }

    #[test]
    fn stage_source_honors_hpcignore() {
        let src = tempfile::tempdir().expect("src");
        let cache = tempfile::tempdir().expect("cache");
        write(src.path(), "keep.rs", b"keep");
        write(src.path(), "big/data.bin", b"excluded");
        write(src.path(), "notes.tmp", b"tmp");
        write(src.path(), ".hpcignore", b"big/\n*.tmp\n");

        let snap = stage_source(src.path(), cache.path()).expect("stage");
        assert!(snap.dir.join("keep.rs").exists());
        assert!(
            !snap.dir.join("big/data.bin").exists(),
            "an .hpcignore'd directory is excluded from the snapshot"
        );
        assert!(
            !snap.dir.join("notes.tmp").exists(),
            "an .hpcignore'd glob is excluded from the snapshot"
        );
        // The .hpcignore file itself is a tracked source file and is kept.
        assert!(snap.dir.join(".hpcignore").exists());
    }
}
