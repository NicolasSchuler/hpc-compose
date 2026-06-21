//! Content-addressed store (CAS) for staged inputs (datasets and models).
//!
//! This module is the *bare* on-laptop/login-node store: it derives a
//! deterministic on-disk key from a spec, lays staged inputs out under
//! `cache_dir/{datasets,models}/<key>`, and provides a write-through
//! [`ensure_staged_input`] that materializes once and reuses thereafter.
//!
//! It performs **zero network I/O**: materialization is an injected closure, so
//! the actual fetch (e.g. an `hf://` download) lives entirely in the caller.
//! Tracking metadata is recorded as a sidecar manifest via
//! [`crate::cache::upsert_dataset_manifest`], so `cache list`/`cache prune`
//! transparently see staged inputs alongside image artifacts.
//!
//! Atomicity: a fresh build materializes into a temporary directory in the same
//! parent, then atomically renames it into place and only then writes the
//! `COMPLETE` sidecar. A staged directory present *without* its sidecar is
//! treated as incomplete (e.g. an interrupted build) and re-materialized.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::cache::{self, CacheEntryKind};
use crate::domain::{artifact_cache_key, short_digest_prefix};

/// The kind of staged input. Used both to pick the on-disk subdirectory and to
/// stamp the sidecar manifest. Reused by downstream `hf://` staging (#11) — do
/// not introduce a parallel kind enum there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedInputKind {
    /// A dataset staged under `cache_dir/datasets/<key>`.
    Dataset,
    /// A model staged under `cache_dir/models/<key>`.
    Model,
}

impl StagedInputKind {
    /// The cache-root subdirectory segment for this kind.
    #[must_use]
    pub fn as_dir_segment(self) -> &'static str {
        match self {
            StagedInputKind::Dataset => "datasets",
            StagedInputKind::Model => "models",
        }
    }

    /// The sidecar-manifest filename suffix for this kind
    /// (`<dir>.dataset.json`/`<dir>.model.json`).
    #[must_use]
    fn sidecar_suffix(self) -> &'static str {
        match self {
            StagedInputKind::Dataset => "dataset",
            StagedInputKind::Model => "model",
        }
    }

    /// The matching cache manifest kind.
    #[must_use]
    fn manifest_kind(self) -> CacheEntryKind {
        match self {
            StagedInputKind::Dataset => CacheEntryKind::Dataset,
            StagedInputKind::Model => CacheEntryKind::Model,
        }
    }
}

/// The keyable description of a staged input. The *spec* (uri + revision +
/// kind), not the file content, is the primary key — for immutable pinned
/// refs this is sufficient and avoids re-reading large trees. `content_digest`
/// is recorded opportunistically after materialization, when known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedInputSpec {
    /// Source URI of the staged input (e.g. `hf://org/model`).
    pub uri: String,
    /// Pinned revision (e.g. a git tag or commit), when the source provides one.
    pub revision: Option<String>,
    /// Whether this is a dataset or a model.
    pub kind: StagedInputKind,
}

impl StagedInputSpec {
    /// Convenience constructor.
    #[must_use]
    pub fn new(kind: StagedInputKind, uri: impl Into<String>, revision: Option<String>) -> Self {
        StagedInputSpec {
            uri: uri.into(),
            revision,
            kind,
        }
    }
}

/// The `hf://` URI scheme prefix recognized by [`parse_hf_uri`].
pub const HF_URI_SCHEME: &str = "hf://";

/// The sentinel file written by the rendered cluster-side download step once a
/// HuggingFace artifact has been fully fetched into its content-addressed
/// directory. A subsequent job that finds this marker skips the download.
pub const HF_COMPLETE_MARKER: &str = ".hpc-compose-hf-complete";

/// A parsed, validated `hf://org/name@rev` reference.
///
/// Produced by [`parse_hf_uri`]. The `revision` is mandatory and must be an
/// immutable pin (a commit-SHA-shaped hex string or an explicit tag); floating
/// refs such as `@main` or a bare branch name are rejected at parse time so a
/// rendered job is reproducible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfArtifactRef {
    /// The HuggingFace repo id (`org/name`), e.g. `meta-llama/Llama-3.1-8B`.
    pub repo: String,
    /// The immutable pinned revision (commit SHA or explicit tag).
    pub revision: String,
    /// Whether this resolves a model or a dataset.
    pub kind: StagedInputKind,
}

impl HfArtifactRef {
    /// Builds the [`StagedInputSpec`] used to derive this ref's cache key/dir.
    #[must_use]
    pub fn staged_input_spec(&self) -> StagedInputSpec {
        StagedInputSpec::new(
            self.kind,
            format!("{HF_URI_SCHEME}{}", self.repo),
            Some(self.revision.clone()),
        )
    }
}

/// Parses and validates an `hf://org/name@rev` URI for the given artifact kind.
///
/// Performs **no** network I/O — this is pure validation/derivation, so it is
/// safe to call at validate/plan/render time on the laptop. The actual download
/// happens cluster-side in the rendered batch script.
///
/// # Errors
///
/// Returns an error when:
/// * the URI does not use the `hf://` scheme,
/// * the repo id is not exactly `org/name` (missing/extra path segments),
/// * the `@rev` pin is missing, or
/// * the revision is a floating ref (`main`, `master`, `HEAD`, or any
///   non-immutable-looking token) rather than an immutable commit SHA / tag.
pub fn parse_hf_uri(raw: &str, kind: StagedInputKind) -> Result<HfArtifactRef> {
    let trimmed = raw.trim();
    let Some(rest) = trimmed.strip_prefix(HF_URI_SCHEME) else {
        anyhow::bail!(
            "must be a HuggingFace URI of the form '{HF_URI_SCHEME}org/name@<immutable-rev>', got '{raw}'"
        );
    };
    if rest.contains("://") {
        anyhow::bail!("'{raw}' is not a valid {HF_URI_SCHEME} URI");
    }

    let Some((repo, revision)) = rest.split_once('@') else {
        anyhow::bail!(
            "{HF_URI_SCHEME} reference '{raw}' must pin an immutable revision with '@<rev>' (e.g. '{HF_URI_SCHEME}org/name@<commit-sha>'); floating refs are not allowed"
        );
    };

    let repo = repo.trim();
    let revision = revision.trim();
    validate_hf_repo(repo).with_context(|| format!("invalid {HF_URI_SCHEME} reference '{raw}'"))?;
    validate_hf_revision(revision)
        .with_context(|| format!("invalid {HF_URI_SCHEME} reference '{raw}'"))?;

    Ok(HfArtifactRef {
        repo: repo.to_string(),
        revision: revision.to_string(),
        kind,
    })
}

/// Validates a HuggingFace repo id (`org/name`).
///
/// Requires exactly two non-empty segments drawn from `[A-Za-z0-9._-]`. The
/// allowlist deliberately excludes `@`, whitespace, and every shell
/// metacharacter, so a validated repo is safe to embed in the rendered batch
/// script and a repo cannot smuggle a second `@rev` past validation.
///
/// # Errors
/// Returns an error when the repo is not exactly `org/name` or contains a
/// disallowed character.
pub fn validate_hf_repo(repo: &str) -> Result<()> {
    let segments: Vec<&str> = repo.split('/').collect();
    if segments.len() != 2 || segments.iter().any(|s| s.is_empty()) {
        anyhow::bail!("HuggingFace repo must be 'org/name', got '{repo}'");
    }
    if !repo
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
    {
        anyhow::bail!(
            "HuggingFace repo '{repo}' may only contain letters, digits, '-', '_', '.', and a single '/'"
        );
    }
    Ok(())
}

/// Validates that a revision is an immutable pin.
///
/// Accepts only a commit-SHA-shaped hex token (>= 7 hex chars) or an explicit
/// version-looking tag (`v?N(.N)*` with an optional `-` pre-release suffix of
/// `[A-Za-z0-9.-]`). Branch names and other floating refs are rejected for
/// reproducibility, and every accepted shape is free of shell metacharacters by
/// construction.
///
/// # Errors
/// Returns an error when the revision is empty or not an immutable pin.
pub fn validate_hf_revision(revision: &str) -> Result<()> {
    if revision.is_empty() {
        anyhow::bail!("HuggingFace revision must be a non-empty immutable pin");
    }
    if !is_immutable_revision(revision) {
        anyhow::bail!(
            "HuggingFace revision '{revision}' is not an immutable pin; use a commit SHA (>= 7 hex chars) or an explicit version tag (e.g. 'v1.2.0'), not a branch/floating ref"
        );
    }
    Ok(())
}

/// Whether a revision is an immutable pin (commit SHA or explicit version tag).
fn is_immutable_revision(revision: &str) -> bool {
    is_commit_sha(revision) || is_version_tag(revision)
}

/// A commit-SHA-shaped token: all hex, at least 7 chars (short or full SHA).
fn is_commit_sha(s: &str) -> bool {
    s.len() >= 7 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// An explicit version-looking tag: optional `v`, a numeric `N(.N)*` core, and
/// an optional `-` pre-release suffix of `[A-Za-z0-9.-]`.
fn is_version_tag(s: &str) -> bool {
    let core = s.strip_prefix('v').unwrap_or(s);
    let (numeric, suffix) = match core.split_once('-') {
        Some((numeric, rest)) => (numeric, Some(rest)),
        None => (core, None),
    };
    if numeric.is_empty()
        || !numeric.chars().next().is_some_and(|c| c.is_ascii_digit())
        || !numeric.chars().all(|c| c.is_ascii_digit() || c == '.')
    {
        return false;
    }
    match suffix {
        None => true,
        Some(suffix) => {
            !suffix.is_empty()
                && suffix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
        }
    }
}

/// Renders the guarded, cluster-side `huggingface-cli download` shell step for a
/// parsed reference, fetching into `dest` (the content-addressed directory on
/// the shared filesystem).
///
/// The emitted step is idempotent: it only downloads when the
/// [`HF_COMPLETE_MARKER`] sentinel is absent, and writes the marker on success
/// so a repeated job reuses the staged artifact. Datasets pass
/// `--repo-type dataset`; models omit it.
///
/// `HF_HOME`/`HF_HUB_CACHE` are referenced only via `${VAR:-default}` guards and
/// `HF_TOKEN` is **never** written into the returned string — it is imported
/// from the job's runtime environment by `huggingface-cli` itself.
#[must_use]
pub fn render_hf_stage_command(reference: &HfArtifactRef, dest: &str, cli_bin: &str) -> String {
    let repo_type_flag = match reference.kind {
        StagedInputKind::Dataset => " --repo-type dataset",
        StagedInputKind::Model => "",
    };
    let bin = shell_single_quote(cli_bin);
    let repo = shell_single_quote(&reference.repo);
    let revision = shell_single_quote(&reference.revision);
    let target = shell_single_quote(dest);
    let marker = shell_single_quote(HF_COMPLETE_MARKER);

    let kind_word = match reference.kind {
        StagedInputKind::Dataset => "dataset",
        StagedInputKind::Model => "model",
    };
    let mut out = String::new();
    // Build the progress line from shell-quoted tokens (and a quoted "->" so it
    // is not a redirection). Never interpolate the raw repo/revision/dest: they
    // are attacker-controlled compose input. Validation also confines repo and
    // revision to a shell-safe allowlist, but quote here too (defense in depth).
    out.push_str(&format!(
        "echo {} {repo}@{revision} {} {target}\n",
        shell_single_quote(&format!("Staging in HuggingFace {kind_word}")),
        shell_single_quote("->"),
    ));
    out.push_str(&format!("HF_STAGE_TARGET={target}\n"));
    out.push_str(&format!("HF_STAGE_MARKER=\"$HF_STAGE_TARGET/\"{marker}\n"));
    out.push_str("if [ ! -e \"$HF_STAGE_MARKER\" ]; then\n");
    out.push_str("  mkdir -p \"$(dirname \"$HF_STAGE_TARGET\")\"\n");
    // flock-serialized (best effort), double-checked, download into a temp dir
    // and atomically rename into place — so concurrent array/sweep tasks sharing
    // the filesystem never corrupt the CAS dir and a partial tree is never
    // observable as complete (the marker is written strictly last).
    out.push_str("  (\n");
    out.push_str("    if command -v flock >/dev/null 2>&1; then flock 9; fi\n");
    out.push_str("    if [ ! -e \"$HF_STAGE_MARKER\" ]; then\n");
    out.push_str(
        "      hf_tmp=\"$(mktemp -d \"$(dirname \"$HF_STAGE_TARGET\")/.hf-stage.XXXXXX\")\"\n",
    );
    out.push_str(&format!(
        "      {bin} download {repo}{repo_type_flag} --revision {revision} --local-dir \"$hf_tmp\"\n"
    ));
    out.push_str("      rm -rf \"$HF_STAGE_TARGET\"\n");
    out.push_str("      mv \"$hf_tmp\" \"$HF_STAGE_TARGET\"\n");
    out.push_str("      touch \"$HF_STAGE_MARKER\"\n");
    out.push_str("    fi\n");
    out.push_str("  ) 9>\"$HF_STAGE_TARGET.lock\"\n");
    out.push_str("fi\n");
    out
}

/// Single-quotes a value for safe embedding in the rendered shell step.
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Whether [`ensure_staged_input`] reused an existing entry or built a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedInputAction {
    /// A complete entry already existed and was reused (no materialization).
    Reused,
    /// The entry was materialized fresh by the injected closure.
    Built,
}

/// Returned by the injected materialize closure to populate the manifest. Kept
/// as a struct (not a bare `Option<String>`) so #11 can extend it without
/// reshaping the closure signature.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagedInputProof {
    /// A content digest the materializer computed, when available.
    pub content_digest: Option<String>,
}

/// Derives the deterministic 16-hex on-disk key for a staged input.
///
/// Reuses [`crate::domain::artifact_cache_key`] + [`short_digest_prefix`] (the
/// same machinery the image cache uses), keyed on the spec — identical
/// (uri, revision, kind) yield the same key; any difference yields a different
/// one. The tool version is intentionally *not* mixed in: a staged input is the
/// upstream content, independent of which tool version fetched it.
#[must_use]
pub fn dataset_cache_key(spec: &StagedInputSpec) -> String {
    let revision = spec.revision.as_deref().unwrap_or("");
    let full = artifact_cache_key(&[
        "staged-input",
        spec.kind.as_dir_segment(),
        &spec.uri,
        revision,
    ]);
    short_digest_prefix(&full).to_string()
}

/// The on-disk directory for a staged input: `cache_dir/{datasets,models}/<key>`.
#[must_use]
pub fn staged_input_dir(cache_dir: &Path, kind: StagedInputKind, key: &str) -> PathBuf {
    cache_dir.join(kind.as_dir_segment()).join(key)
}

/// The sidecar-manifest path for a staged directory, given the kind suffix.
///
/// Returns the `<staged_dir>.{dataset,model}.json` sibling of the directory.
/// Crate-internal: `crate::cache` resolves the sidecar for removal/upsert via
/// this so both modules agree on the layout.
#[must_use]
pub(crate) fn sidecar_manifest_path_for_suffix(staged_dir: &Path, suffix: &str) -> PathBuf {
    let mut name = staged_dir
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".{suffix}.json"));
    staged_dir.with_file_name(name)
}

/// Whether a staged directory has a `COMPLETE` tracking sidecar (and so was
/// fully materialized).
fn is_complete(cache_dir: &Path, kind: StagedInputKind, key: &str) -> bool {
    let dir = staged_input_dir(cache_dir, kind, key);
    sidecar_manifest_path_for_suffix(&dir, kind.sidecar_suffix()).is_file()
}

/// Write-through staged-input store.
///
/// Returns the staged directory and whether it was [`StagedInputAction::Reused`]
/// or [`StagedInputAction::Built`]:
///
/// * If a complete entry (staged dir + sidecar) already exists, the closure is
///   **not** invoked; `last_used_at` is bumped and `Reused` is returned.
/// * Otherwise any partial leftover is cleared, the closure materializes into a
///   fresh temp dir, the temp dir is atomically renamed into place, and the
///   `COMPLETE` sidecar is written last — then `Built` is returned.
///
/// The closure receives the destination directory (already created and empty)
/// and returns a [`StagedInputProof`]. It must perform **all** I/O for the
/// payload itself; this function never touches the network.
///
/// # Errors
///
/// Returns an error when the store directories cannot be provisioned, the
/// closure fails, the atomic rename fails, or the sidecar manifest cannot be
/// written.
pub fn ensure_staged_input(
    cache_dir: &Path,
    spec: &StagedInputSpec,
    materialize: impl FnOnce(&Path) -> Result<StagedInputProof>,
) -> Result<(PathBuf, StagedInputAction)> {
    let kind = spec.kind;
    let key = dataset_cache_key(spec);
    let dir = staged_input_dir(cache_dir, kind, &key);

    if is_complete(cache_dir, kind, &key) {
        cache::touch_dataset_manifest(&dir, kind.manifest_kind())
            .context("failed to refresh staged-input manifest")?;
        return Ok((dir, StagedInputAction::Reused));
    }

    // Clear any partial leftover (the dir exists but its sidecar does not, e.g.
    // an interrupted prior build) before re-materializing.
    if dir.exists() {
        fs::remove_dir_all(&dir).context(format!(
            "failed to clear partial staged dir {}",
            dir.display()
        ))?;
    }
    // Also clear a stale sidecar with no directory.
    let sidecar = sidecar_manifest_path_for_suffix(&dir, kind.sidecar_suffix());
    if sidecar.exists() {
        let _ = fs::remove_file(&sidecar);
    }

    let parent = dir
        .parent()
        .context("staged-input directory has no parent")?;
    fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;

    let temp = unique_temp_dir(&dir);
    fs::create_dir(&temp).context(format!("failed to create temp dir {}", temp.display()))?;

    // Materialize into the temp dir; on any failure, clean it up so a retry
    // starts fresh.
    let proof = match materialize(&temp) {
        Ok(proof) => proof,
        Err(err) => {
            let _ = fs::remove_dir_all(&temp);
            return Err(err.context("staged-input materialization failed"));
        }
    };

    // Atomic publish: rename the fully built temp dir into the final location.
    // (A concurrent builder that won the race leaves a complete dir; treat the
    // resulting AlreadyExists as a benign reuse.)
    match fs::rename(&temp, &dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_dir_all(&temp);
            if is_complete(cache_dir, kind, &key) {
                cache::touch_dataset_manifest(&dir, kind.manifest_kind())
                    .context("failed to refresh staged-input manifest")?;
                return Ok((dir, StagedInputAction::Reused));
            }
            // The destination exists but is not complete: replace it.
            fs::remove_dir_all(&dir).context(format!(
                "failed to replace stale staged dir {}",
                dir.display()
            ))?;
            fs::rename(&temp, &dir)
                .context(format!("failed to publish staged dir {}", dir.display()))?;
        }
        Err(err) => {
            let _ = fs::remove_dir_all(&temp);
            return Err(err).context(format!("failed to publish staged dir {}", dir.display()));
        }
    }

    cache::upsert_dataset_manifest(
        &dir,
        kind.manifest_kind(),
        &key,
        &spec.uri,
        spec.revision.as_deref(),
        proof.content_digest.as_deref(),
    )
    .context("failed to write staged-input manifest")?;

    Ok((dir, StagedInputAction::Built))
}

/// Builds a unique sibling temp-dir path for atomic publish. Same parent as the
/// destination so the subsequent rename is atomic (not a cross-device copy).
fn unique_temp_dir(dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut name = dir
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".staging.{pid}.{nanos}"));
    dir.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_spec() -> StagedInputSpec {
        StagedInputSpec::new(
            StagedInputKind::Dataset,
            "hf://org/cifar10",
            Some("v1".into()),
        )
    }

    #[test]
    fn dataset_cache_key_is_deterministic_and_spec_sensitive() {
        let key = dataset_cache_key(&dataset_spec());
        assert_eq!(key.len(), 16, "on-disk key must be the 16-hex prefix");
        assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic for identical specs.
        assert_eq!(key, dataset_cache_key(&dataset_spec()));

        // Differing revision => different key.
        let other_rev = StagedInputSpec::new(
            StagedInputKind::Dataset,
            "hf://org/cifar10",
            Some("v2".into()),
        );
        assert_ne!(key, dataset_cache_key(&other_rev));

        // Differing kind (same uri/revision) => different key.
        let as_model = StagedInputSpec::new(
            StagedInputKind::Model,
            "hf://org/cifar10",
            Some("v1".into()),
        );
        assert_ne!(key, dataset_cache_key(&as_model));

        // Differing uri => different key.
        let other_uri = StagedInputSpec::new(
            StagedInputKind::Dataset,
            "hf://org/mnist",
            Some("v1".into()),
        );
        assert_ne!(key, dataset_cache_key(&other_uri));
    }

    #[test]
    fn staged_input_dir_layout_is_under_kind_segment() {
        let cache = Path::new("/shared/cache");
        let ds = staged_input_dir(cache, StagedInputKind::Dataset, "abc");
        let md = staged_input_dir(cache, StagedInputKind::Model, "abc");
        assert_eq!(ds, Path::new("/shared/cache/datasets/abc"));
        assert_eq!(md, Path::new("/shared/cache/models/abc"));
        assert_eq!(StagedInputKind::Dataset.as_dir_segment(), "datasets");
        assert_eq!(StagedInputKind::Model.as_dir_segment(), "models");
    }

    #[test]
    fn ensure_staged_input_materializes_then_reuses() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let cache = tmp.path();
        let spec = dataset_spec();

        let mut calls = 0;
        let (dir, action) = ensure_staged_input(cache, &spec, |dest| {
            calls += 1;
            fs::write(dest.join("data.bin"), b"payload").expect("write payload");
            Ok(StagedInputProof {
                content_digest: Some("sha256:deadbeef".into()),
            })
        })
        .expect("first ensure");
        assert_eq!(action, StagedInputAction::Built);
        assert_eq!(calls, 1, "first call materializes");
        assert!(dir.join("data.bin").exists());
        let sidecar = sidecar_manifest_path_for_suffix(&dir, "dataset");
        assert!(sidecar.is_file(), "COMPLETE sidecar written");

        // Sidecar records the spec + proof.
        let manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
        assert_eq!(manifest.kind, CacheEntryKind::Dataset);
        assert_eq!(manifest.uri.as_deref(), Some("hf://org/cifar10"));
        assert_eq!(manifest.revision.as_deref(), Some("v1"));
        assert_eq!(manifest.content_digest.as_deref(), Some("sha256:deadbeef"));
        let first_used = manifest.last_used_at;

        // Second call with the same spec reuses without invoking the closure.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let (dir2, action2) = ensure_staged_input(cache, &spec, |_dest| {
            panic!("must not materialize on reuse");
        })
        .expect("second ensure");
        assert_eq!(action2, StagedInputAction::Reused);
        assert_eq!(dir2, dir);
        let manifest2 = crate::cache::read_staged_manifest_for_test(&sidecar);
        assert!(
            manifest2.last_used_at >= first_used,
            "reuse bumps last_used_at"
        );
    }

    #[test]
    fn ensure_staged_input_recovers_from_partial_materialization() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let cache = tmp.path();
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(cache, StagedInputKind::Dataset, &key);

        // Simulate an interrupted build: the staged dir exists with partial
        // contents but no COMPLETE sidecar.
        fs::create_dir_all(&dir).expect("partial dir");
        fs::write(dir.join("partial.tmp"), b"half").expect("partial file");
        assert!(!sidecar_manifest_path_for_suffix(&dir, "dataset").is_file());

        let mut calls = 0;
        let (built_dir, action) = ensure_staged_input(cache, &spec, |dest| {
            calls += 1;
            assert!(
                !dest.join("partial.tmp").exists(),
                "stale partial contents must not leak into the fresh build"
            );
            fs::write(dest.join("data.bin"), b"payload").expect("write payload");
            Ok(StagedInputProof::default())
        })
        .expect("ensure after partial");
        assert_eq!(action, StagedInputAction::Built);
        assert_eq!(calls, 1, "incomplete entry is re-materialized");
        assert!(built_dir.join("data.bin").exists());
        assert!(!built_dir.join("partial.tmp").exists());
        assert!(sidecar_manifest_path_for_suffix(&built_dir, "dataset").is_file());
    }

    #[test]
    fn parse_hf_uri_accepts_org_name_at_rev() {
        let parsed = parse_hf_uri(
            "hf://meta-llama/Llama-3.1-8B@abc1234def5678",
            StagedInputKind::Model,
        )
        .expect("valid hf uri");
        assert_eq!(parsed.repo, "meta-llama/Llama-3.1-8B");
        assert_eq!(parsed.revision, "abc1234def5678");
        assert_eq!(parsed.kind, StagedInputKind::Model);

        // The derived staged-input spec keys the CAS dir deterministically.
        let spec = parsed.staged_input_spec();
        assert_eq!(spec.uri, "hf://meta-llama/Llama-3.1-8B");
        assert_eq!(spec.revision.as_deref(), Some("abc1234def5678"));
        assert_eq!(spec.kind, StagedInputKind::Model);
    }

    #[test]
    fn parse_hf_uri_rejects_non_hf_scheme() {
        for raw in [
            "s3://bucket/model@abc1234",
            "https://example.com/x@abc1234",
            "/local/path/to/model",
            "meta-llama/Llama-3.1-8B@abc1234",
        ] {
            assert!(
                parse_hf_uri(raw, StagedInputKind::Model).is_err(),
                "expected non-hf scheme to be rejected: {raw}"
            );
        }
    }

    #[test]
    fn parse_hf_uri_requires_immutable_revision() {
        // Missing @rev entirely.
        let err = parse_hf_uri("hf://org/name", StagedInputKind::Model)
            .expect_err("missing rev must error");
        assert!(err.to_string().contains("immutable revision"));

        // Floating / branch refs are hard-rejected: the allowlist accepts only
        // immutable pins, so arbitrary branch names no longer slip through.
        for floating in [
            "main",
            "master",
            "HEAD",
            "latest",
            "dev",
            "release",
            "production",
            "my-feature",
        ] {
            let raw = format!("hf://org/name@{floating}");
            let err = parse_hf_uri(&raw, StagedInputKind::Dataset)
                .expect_err("floating/branch ref must error");
            assert!(
                format!("{err:#}").contains("immutable pin"),
                "branch ref '{floating}' should be rejected: {err:#}"
            );
        }

        // Immutable pins are accepted: short and full commit SHAs, version tags.
        for ok in [
            "abc1234",
            "0123456789abcdef0123456789abcdef01234567",
            "v1.2.0",
            "1.0",
            "v2",
            "v1.0.0-rc.1",
        ] {
            let raw = format!("hf://org/name@{ok}");
            assert!(
                parse_hf_uri(&raw, StagedInputKind::Model).is_ok(),
                "immutable revision '{ok}' should be accepted"
            );
        }

        // A bad repo shape is rejected.
        assert!(parse_hf_uri("hf://only-one-segment@abc1234", StagedInputKind::Model).is_err());
        assert!(parse_hf_uri("hf://a/b/c@abc1234", StagedInputKind::Model).is_err());
        // Shell metacharacters in the repo or revision are rejected (the
        // allowlist closes the injection path before render).
        assert!(parse_hf_uri("hf://org/n$(id)@abc1234", StagedInputKind::Model).is_err());
        assert!(parse_hf_uri("hf://org/name@abc1234$(id)", StagedInputKind::Model).is_err());
    }

    #[test]
    fn render_hf_stage_command_models_vs_datasets() {
        let model = HfArtifactRef {
            repo: "org/model".into(),
            revision: "abc1234".into(),
            kind: StagedInputKind::Model,
        };
        let model_cmd = render_hf_stage_command(&model, "/shared/cache/models/key", "hf-cli");
        assert!(model_cmd.contains("'hf-cli' download 'org/model' --revision 'abc1234'"));
        // Downloads into a temp dir, then atomically renames into place under a
        // best-effort flock so concurrent jobs cannot corrupt the CAS dir.
        assert!(model_cmd.contains("--local-dir \"$hf_tmp\""));
        assert!(model_cmd.contains("mktemp -d"));
        assert!(model_cmd.contains("flock 9"));
        assert!(model_cmd.contains("mv \"$hf_tmp\" \"$HF_STAGE_TARGET\""));
        assert!(!model_cmd.contains("--repo-type"));
        // Guarded by the completion marker, idempotent.
        assert!(model_cmd.contains(HF_COMPLETE_MARKER));
        assert!(model_cmd.contains("if [ ! -e \"$HF_STAGE_MARKER\" ]; then"));
        // Never embeds a secret.
        assert!(!model_cmd.contains("HF_TOKEN"));

        let dataset = HfArtifactRef {
            repo: "org/data".into(),
            revision: "deadbeef".into(),
            kind: StagedInputKind::Dataset,
        };
        let dataset_cmd = render_hf_stage_command(&dataset, "/shared/cache/datasets/key", "hf-cli");
        assert!(dataset_cmd.contains("--repo-type dataset"));
        assert!(dataset_cmd.contains("--revision 'deadbeef'"));
    }

    #[test]
    fn model_kind_uses_models_segment_and_sidecar() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let cache = tmp.path();
        let spec = StagedInputSpec::new(StagedInputKind::Model, "hf://org/llm", Some("rev".into()));
        let (dir, action) = ensure_staged_input(cache, &spec, |dest| {
            fs::write(dest.join("config.json"), b"{}").expect("write config");
            Ok(StagedInputProof::default())
        })
        .expect("ensure model");
        assert_eq!(action, StagedInputAction::Built);
        assert!(
            dir.starts_with(cache.join("models")),
            "model staged under models/ segment"
        );
        assert!(sidecar_manifest_path_for_suffix(&dir, "model").is_file());
    }
}
