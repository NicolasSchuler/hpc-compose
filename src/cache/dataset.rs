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
