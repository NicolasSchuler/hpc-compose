//! Mutable login-node staged-input CAS publication and completion protocol.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{
    STAGED_COMPLETE_MARKER, StagedInputAction, StagedInputKind, StagedInputProof, StagedInputSpec,
    dataset_cache_key, sidecar_manifest_path_for_suffix, staged_input_dir,
};
use crate::cache;

const STAGED_COMPLETION_VERSION: u32 = 1;
static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::cache) struct StagedInputCompletion {
    format_version: u32,
    pub(in crate::cache) kind: StagedInputKind,
    pub(in crate::cache) cache_key: String,
    pub(in crate::cache) uri: String,
    pub(in crate::cache) revision: Option<String>,
    pub(in crate::cache) content_digest: Option<String>,
}

impl StagedInputCompletion {
    fn new(spec: &StagedInputSpec, cache_key: &str, proof: &StagedInputProof) -> Self {
        Self {
            format_version: STAGED_COMPLETION_VERSION,
            kind: spec.kind,
            cache_key: cache_key.to_string(),
            uri: spec.uri.clone(),
            revision: spec.revision.clone(),
            content_digest: proof.content_digest.clone(),
        }
    }

    fn matches(&self, spec: &StagedInputSpec, cache_key: &str) -> bool {
        self.format_version == STAGED_COMPLETION_VERSION
            && self.kind == spec.kind
            && self.cache_key == cache_key
            && self.uri == spec.uri
            && self.revision == spec.revision
    }
}

fn completion_marker_path(staged_dir: &Path) -> PathBuf {
    staged_dir.join(STAGED_COMPLETE_MARKER)
}

pub(super) fn read_staged_completion(staged_dir: &Path) -> Result<Option<StagedInputCompletion>> {
    let marker = completion_marker_path(staged_dir);
    let raw = match fs::read_to_string(&marker) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).context(format!("failed to read {}", marker.display())),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .context(format!("failed to parse {}", marker.display()))
}

fn write_staged_completion(staged_dir: &Path, completion: &StagedInputCompletion) -> Result<()> {
    let marker = completion_marker_path(staged_dir);
    let raw = serde_json::to_vec_pretty(completion)
        .context("failed to serialize staged-input completion record")?;
    crate::secure_io::write_atomic(&marker, raw, false)
        .context(format!("failed to write {}", marker.display()))
}

fn refresh_tracking_manifest(staged_dir: &Path, completion: &StagedInputCompletion) -> Result<()> {
    cache::upsert_dataset_manifest(
        staged_dir,
        completion.kind.manifest_kind(),
        &completion.cache_key,
        &completion.uri,
        completion.revision.as_deref(),
        completion.content_digest.as_deref(),
    )
    .context("failed to refresh staged-input manifest")?;
    Ok(())
}

fn load_valid_completion(
    staged_dir: &Path,
    spec: &StagedInputSpec,
    key: &str,
) -> Result<Option<StagedInputCompletion>> {
    let Some(completion) = read_staged_completion(staged_dir)? else {
        return Ok(None);
    };
    if !completion.matches(spec, key) {
        anyhow::bail!(
            "staged directory {} has completion metadata for a different input",
            staged_dir.display()
        );
    }
    Ok(Some(completion))
}

/// Migrates a pre-completion-record entry whose sibling manifest is still
/// present. This keeps existing caches reusable while ensuring subsequent
/// reuse no longer depends on the sibling sidecar's presence.
fn migrate_legacy_completion(
    staged_dir: &Path,
    spec: &StagedInputSpec,
    key: &str,
) -> Result<Option<StagedInputCompletion>> {
    let sidecar = sidecar_manifest_path_for_suffix(staged_dir, spec.kind.sidecar_suffix());
    let Some(manifest) = cache::read_staged_manifest_if_exists(&sidecar)? else {
        return Ok(None);
    };
    if manifest.kind != spec.kind.manifest_kind()
        || manifest.cache_key != key
        || manifest.uri.as_deref() != Some(spec.uri.as_str())
        || manifest.revision != spec.revision
    {
        anyhow::bail!(
            "legacy staged-input manifest {} does not match its expected cache key",
            sidecar.display()
        );
    }
    let completion = StagedInputCompletion {
        format_version: STAGED_COMPLETION_VERSION,
        kind: spec.kind,
        cache_key: key.to_string(),
        uri: spec.uri.clone(),
        revision: spec.revision.clone(),
        content_digest: manifest.content_digest,
    };
    write_staged_completion(staged_dir, &completion)?;
    Ok(Some(completion))
}

pub(super) fn ensure_staged_input(
    cache_dir: &Path,
    spec: &StagedInputSpec,
    materialize: impl FnOnce(&Path) -> Result<StagedInputProof>,
) -> Result<(PathBuf, StagedInputAction)> {
    let kind = spec.kind;
    let key = dataset_cache_key(spec);
    let dir = staged_input_dir(cache_dir, kind, &key);

    let existing = match load_valid_completion(&dir, spec, &key)? {
        Some(completion) => Some(completion),
        None => migrate_legacy_completion(&dir, spec, &key)?,
    };
    if let Some(completion) = existing {
        refresh_tracking_manifest(&dir, &completion)?;
        return Ok((dir, StagedInputAction::Reused));
    }

    // A final directory without completion metadata may belong to a legacy
    // writer between its directory rename and sidecar write. Never delete or
    // replace it based only on the sibling sidecar being absent.
    if dir.exists() {
        anyhow::bail!(
            "staged directory {} exists without valid completion metadata; refusing to replace a possibly concurrent publication",
            dir.display()
        );
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
    let completion = StagedInputCompletion::new(spec, &key, &proof);
    if let Err(err) = write_staged_completion(&temp, &completion) {
        let _ = fs::remove_dir_all(&temp);
        return Err(err);
    }

    // Atomic publish. Any rename failure caused by a concurrent winner is a
    // benign reuse only after validating the winner's in-directory record.
    match rename_dir_noreplace(&temp, &dir) {
        Ok(()) => {}
        Err(err) if dir.exists() => {
            let _ = fs::remove_dir_all(&temp);
            if let Some(winner) = load_valid_completion(&dir, spec, &key)? {
                refresh_tracking_manifest(&dir, &winner)?;
                return Ok((dir, StagedInputAction::Reused));
            }
            return Err(err).context(format!(
                "failed to publish staged dir {}; destination exists without valid completion metadata",
                dir.display()
            ));
        }
        Err(err) => {
            let _ = fs::remove_dir_all(&temp);
            return Err(err).context(format!("failed to publish staged dir {}", dir.display()));
        }
    }

    // Always derive the rebuildable sidecar from the generation that actually
    // won publication, never from a losing builder's local proof.
    let published = load_valid_completion(&dir, spec, &key)?
        .context("published staged-input directory has no completion record")?;
    refresh_tracking_manifest(&dir, &published)?;

    Ok((dir, StagedInputAction::Built))
}

/// Atomically publishes a staged directory without replacing an existing path.
///
/// hpc-compose runs on macOS login clients and Linux compute environments. Both
/// provide an atomic no-replace rename operation; failing that operation is
/// safer than falling back to a check-then-rename sequence that can clobber a
/// concurrent publication.
#[cfg(target_vendor = "apple")]
fn rename_dir_noreplace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    // SAFETY: both paths are valid NUL-terminated C strings for the duration of
    // the call. RENAME_EXCL makes publication fail when any destination exists.
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn rename_dir_noreplace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    // SAFETY: both paths are valid NUL-terminated C strings for the duration of
    // the call. renameat2 with RENAME_NOREPLACE atomically refuses any existing
    // destination rather than replacing an empty directory.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn rename_dir_noreplace(_source: &Path, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unsupported on this platform",
    ))
}

/// Builds a unique sibling temp-dir path for atomic publish. Same parent as the
/// destination so the subsequent rename is atomic (not a cross-device copy).
fn unique_temp_dir(dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut name = dir
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".staging.{pid}.{counter}.{nanos}"));
    dir.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::cache::CacheEntryKind;

    fn dataset_spec() -> StagedInputSpec {
        StagedInputSpec::new(
            StagedInputKind::Dataset,
            "hf://org/cifar10",
            Some("v1".into()),
        )
    }

    fn staging_directories(cache_dir: &Path, kind: StagedInputKind) -> Vec<PathBuf> {
        let parent = cache_dir.join(kind.as_dir_segment());
        let Ok(entries) = fs::read_dir(parent) else {
            return Vec::new();
        };
        entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".staging."))
            })
            .collect()
    }

    #[test]
    fn ensure_staged_input_refuses_to_replace_unproven_directory() {
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

        let err = ensure_staged_input(cache, &spec, |_dest| {
            panic!("an unproven final directory must not be overwritten");
        })
        .expect_err("unproven directory must fail safely");
        assert!(format!("{err:#}").contains("refusing to replace"));
        assert_eq!(
            fs::read(dir.join("partial.tmp")).expect("original remains"),
            b"half"
        );
    }

    #[test]
    fn missing_tracking_sidecar_is_rebuilt_from_completion_record() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let (dir, _) = ensure_staged_input(tmp.path(), &spec, |dest| {
            fs::write(dest.join("data.bin"), b"payload").expect("payload");
            Ok(StagedInputProof {
                content_digest: Some("sha256:payload".into()),
            })
        })
        .expect("build");
        let sidecar = sidecar_manifest_path_for_suffix(&dir, "dataset");
        fs::remove_file(&sidecar).expect("remove rebuildable sidecar");

        let (_, action) = ensure_staged_input(tmp.path(), &spec, |_dest| {
            panic!("published completion record must be reused");
        })
        .expect("reuse and heal");
        assert_eq!(action, StagedInputAction::Reused);
        let manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
        assert_eq!(manifest.content_digest.as_deref(), Some("sha256:payload"));
    }

    #[test]
    fn matching_legacy_sidecar_migrates_exact_completion_without_materializing() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);
        fs::create_dir_all(&dir).expect("legacy staged dir");
        fs::write(dir.join("legacy.bin"), b"legacy-payload").expect("legacy payload");
        crate::cache::upsert_dataset_manifest(
            &dir,
            CacheEntryKind::Dataset,
            &key,
            &spec.uri,
            spec.revision.as_deref(),
            Some("sha256:legacy"),
        )
        .expect("legacy sidecar");

        let mut materialized = false;
        let (actual_dir, action) = ensure_staged_input(tmp.path(), &spec, |_dest| {
            materialized = true;
            Ok(StagedInputProof::default())
        })
        .expect("migrate legacy entry");

        assert_eq!(actual_dir, dir);
        assert_eq!(action, StagedInputAction::Reused);
        assert!(!materialized, "legacy migration must not rebuild payload");
        assert_eq!(
            fs::read(dir.join("legacy.bin")).expect("preserved legacy payload"),
            b"legacy-payload"
        );
        let completion = fs::read_to_string(dir.join(STAGED_COMPLETE_MARKER))
            .expect("migrated completion record");
        assert_eq!(
            completion,
            format!(
                concat!(
                    "{{\n",
                    "  \"format_version\": 1,\n",
                    "  \"kind\": \"dataset\",\n",
                    "  \"cache_key\": \"{}\",\n",
                    "  \"uri\": \"hf://org/cifar10\",\n",
                    "  \"revision\": \"v1\",\n",
                    "  \"content_digest\": \"sha256:legacy\"\n",
                    "}}"
                ),
                key
            )
        );
        let sidecar = sidecar_manifest_path_for_suffix(&dir, "dataset");
        let manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
        assert_eq!(manifest.content_digest.as_deref(), Some("sha256:legacy"));
    }

    #[test]
    fn mismatched_legacy_sidecar_fails_without_materializing_or_mutating_payload() {
        for field in ["kind", "cache_key", "uri", "revision"] {
            let tmp = tempfile::tempdir().expect("tmpdir");
            let spec = dataset_spec();
            let key = dataset_cache_key(&spec);
            let dir = staged_input_dir(tmp.path(), spec.kind, &key);
            fs::create_dir_all(&dir).expect("legacy staged dir");
            fs::write(dir.join("legacy.bin"), b"legacy-payload").expect("legacy payload");
            crate::cache::upsert_dataset_manifest(
                &dir,
                CacheEntryKind::Dataset,
                &key,
                &spec.uri,
                spec.revision.as_deref(),
                Some("sha256:legacy"),
            )
            .expect("legacy sidecar");
            let sidecar = sidecar_manifest_path_for_suffix(&dir, "dataset");
            let mut manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
            match field {
                "kind" => manifest.kind = CacheEntryKind::Model,
                "cache_key" => manifest.cache_key = "different-key".into(),
                "uri" => manifest.uri = Some("hf://org/other".into()),
                "revision" => manifest.revision = Some("v2".into()),
                _ => unreachable!(),
            }
            fs::write(
                &sidecar,
                serde_json::to_string_pretty(&manifest).expect("serialize mismatched sidecar"),
            )
            .expect("write mismatched sidecar");

            let mut materialized = false;
            let error = ensure_staged_input(tmp.path(), &spec, |_dest| {
                materialized = true;
                Ok(StagedInputProof::default())
            })
            .expect_err("mismatched legacy metadata must fail");

            assert_eq!(
                error.to_string(),
                format!(
                    "legacy staged-input manifest {} does not match its expected cache key",
                    sidecar.display()
                ),
                "field={field}"
            );
            assert!(!materialized, "field={field}");
            assert_eq!(
                fs::read(dir.join("legacy.bin")).expect("preserved legacy payload"),
                b"legacy-payload",
                "field={field}"
            );
            assert!(!dir.join(STAGED_COMPLETE_MARKER).exists(), "field={field}");
        }
    }

    #[test]
    fn completion_json_preserves_null_fields_and_has_no_trailing_newline() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = StagedInputSpec::new(StagedInputKind::Dataset, "local://fixture", None);
        let key = dataset_cache_key(&spec);
        let (dir, action) = ensure_staged_input(tmp.path(), &spec, |dest| {
            fs::write(dest.join("data.bin"), b"payload").expect("payload");
            Ok(StagedInputProof::default())
        })
        .expect("build staged input");
        assert_eq!(action, StagedInputAction::Built);

        let completion =
            fs::read_to_string(dir.join(STAGED_COMPLETE_MARKER)).expect("completion record");
        assert_eq!(
            completion,
            format!(
                concat!(
                    "{{\n",
                    "  \"format_version\": 1,\n",
                    "  \"kind\": \"dataset\",\n",
                    "  \"cache_key\": \"{}\",\n",
                    "  \"uri\": \"local://fixture\",\n",
                    "  \"revision\": null,\n",
                    "  \"content_digest\": null\n",
                    "}}"
                ),
                key
            )
        );
        assert!(!completion.ends_with('\n'));
    }

    #[test]
    fn materializer_failure_preserves_cause_and_removes_staging_directory() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);

        let error = ensure_staged_input(tmp.path(), &spec, |_dest| {
            Err(std::io::Error::other("fixture materializer failed").into())
        })
        .expect_err("materializer failure");

        assert_eq!(
            format!("{error:#}"),
            "staged-input materialization failed: fixture materializer failed"
        );
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        assert!(!dir.exists());
        assert!(staging_directories(tmp.path(), spec.kind).is_empty());
    }

    #[test]
    fn completion_write_failure_removes_staging_directory_and_leaves_no_final_entry() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);
        let mut temp_dir = None;

        let error = ensure_staged_input(tmp.path(), &spec, |dest| {
            temp_dir = Some(dest.to_path_buf());
            fs::create_dir(dest.join(STAGED_COMPLETE_MARKER))
                .expect("completion-marker obstruction");
            Ok(StagedInputProof::default())
        })
        .expect_err("completion write must fail");

        let marker = temp_dir
            .expect("materializer destination")
            .join(STAGED_COMPLETE_MARKER);
        assert_eq!(
            error.to_string(),
            format!("failed to write {}", marker.display())
        );
        assert!(!dir.exists());
        assert!(!marker.parent().expect("staging directory").exists());
        assert!(staging_directories(tmp.path(), spec.kind).is_empty());
    }

    #[test]
    fn sidecar_failure_after_publication_preserves_generation_and_recovers_on_reuse() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);
        let sidecar = sidecar_manifest_path_for_suffix(&dir, "dataset");

        let error = ensure_staged_input(tmp.path(), &spec, |dest| {
            fs::write(dest.join("data.bin"), b"published-payload").expect("payload");
            fs::write(&sidecar, b"{not-json").expect("corrupt sidecar after legacy check");
            Ok(StagedInputProof {
                content_digest: Some("sha256:published".into()),
            })
        })
        .expect_err("sidecar refresh must fail after publication");

        assert_eq!(error.to_string(), "failed to refresh staged-input manifest");
        let detail = format!("{error:#}");
        assert!(
            detail.contains("failed to parse"),
            "unexpected error: {detail}"
        );
        assert!(detail.contains(&sidecar.display().to_string()));
        assert_eq!(
            fs::read(dir.join("data.bin")).expect("published payload survives"),
            b"published-payload"
        );
        assert!(dir.join(STAGED_COMPLETE_MARKER).is_file());

        fs::remove_file(&sidecar).expect("remove corrupt projection");
        let (reused_dir, action) = ensure_staged_input(tmp.path(), &spec, |_dest| {
            panic!("published completion must be reused")
        })
        .expect("recover sidecar from published completion");
        assert_eq!(reused_dir, dir);
        assert_eq!(action, StagedInputAction::Reused);
        let manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
        assert_eq!(manifest.content_digest.as_deref(), Some("sha256:published"));
    }

    #[test]
    fn rename_loss_preserves_unproven_destination_and_cleans_losing_staging_dir() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);

        let error = ensure_staged_input(tmp.path(), &spec, |dest| {
            fs::write(dest.join("loser.bin"), b"loser").expect("losing payload");
            fs::create_dir_all(&dir).expect("concurrent destination");
            fs::write(dir.join("winner.partial"), b"winner").expect("winner payload");
            Ok(StagedInputProof {
                content_digest: Some("sha256:loser".into()),
            })
        })
        .expect_err("unproven concurrent destination must not be reused");

        assert_eq!(
            error.to_string(),
            format!(
                "failed to publish staged dir {}; destination exists without valid completion metadata",
                dir.display()
            )
        );
        assert_eq!(
            fs::read(dir.join("winner.partial")).expect("winner preserved"),
            b"winner"
        );
        assert!(!dir.join(STAGED_COMPLETE_MARKER).exists());
        assert!(staging_directories(tmp.path(), spec.kind).is_empty());
    }

    #[test]
    fn rename_loss_preserves_empty_unproven_destination_and_cleans_losing_staging_dir() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let spec = dataset_spec();
        let key = dataset_cache_key(&spec);
        let dir = staged_input_dir(tmp.path(), spec.kind, &key);

        let error = ensure_staged_input(tmp.path(), &spec, |dest| {
            fs::write(dest.join("loser.bin"), b"loser").expect("losing payload");
            fs::create_dir_all(&dir).expect("concurrent empty destination");
            Ok(StagedInputProof {
                content_digest: Some("sha256:loser".into()),
            })
        })
        .expect_err("an empty concurrent destination must not be replaced");

        assert_eq!(
            error.to_string(),
            format!(
                "failed to publish staged dir {}; destination exists without valid completion metadata",
                dir.display()
            )
        );
        assert!(dir.is_dir(), "concurrent destination is preserved");
        assert_eq!(
            fs::read_dir(&dir).expect("preserved destination").count(),
            0,
            "losing payload must not replace the empty destination"
        );
        assert!(staging_directories(tmp.path(), spec.kind).is_empty());
    }

    #[test]
    fn concurrent_builders_converge_on_one_payload_and_proof() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let tmp = tempfile::tempdir().expect("tmpdir");
        let cache = Arc::new(tmp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for label in ["a", "b"] {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                ensure_staged_input(&cache, &dataset_spec(), |dest| {
                    fs::write(dest.join("winner"), label).expect("write candidate");
                    barrier.wait();
                    Ok(StagedInputProof {
                        content_digest: Some(format!("digest-{label}")),
                    })
                })
                .expect("concurrent ensure")
            }));
        }
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("builder thread"))
            .collect();
        assert_eq!(
            results
                .iter()
                .filter(|(_, action)| *action == StagedInputAction::Built)
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|(_, action)| *action == StagedInputAction::Reused)
                .count(),
            1
        );

        let dir = &results[0].0;
        let winner = fs::read_to_string(dir.join("winner")).expect("winner payload");
        let completion = read_staged_completion(dir)
            .expect("read completion")
            .expect("completion exists");
        let sidecar = sidecar_manifest_path_for_suffix(dir, "dataset");
        let manifest = crate::cache::read_staged_manifest_for_test(&sidecar);
        let expected = format!("digest-{winner}");
        assert_eq!(
            completion.content_digest.as_deref(),
            Some(expected.as_str())
        );
        assert_eq!(manifest.content_digest.as_deref(), Some(expected.as_str()));
    }
}
