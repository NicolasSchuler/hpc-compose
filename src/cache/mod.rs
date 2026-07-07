//! Cache manifest management for imported and prepared image artifacts.

pub mod dataset;
pub mod source;

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::planner::{ImageSource, PreparedImageSpec, registry_host_for_remote};
use crate::prepare::{RuntimePlan, RuntimeService, base_image_path_for_backend};
use crate::time_util::{SECONDS_PER_DAY, unix_timestamp_now};

/// The kind of artifact tracked in the cache manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CacheEntryKind {
    /// A base image imported directly from a remote reference.
    Base,
    /// A prepared runtime image derived from a base image.
    Prepared,
    /// A staged dataset materialized into the content-addressed store.
    Dataset,
    /// A staged model materialized into the content-addressed store.
    Model,
    /// A content-addressed snapshot of a project's working-tree source.
    Source,
    /// A kind produced by a newer (or older) tool version. Kept so that
    /// [`scan_cache`] never fails on a manifest it does not recognize and the
    /// entry still round-trips through deserialize/serialize.
    #[serde(other)]
    Unknown,
}

/// Metadata stored next to a cached artifact.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CacheEntryManifest {
    pub kind: CacheEntryKind,
    pub artifact_path: String,
    pub service_names: Vec<String>,
    pub cache_key: String,
    pub source_image: String,
    pub registry: Option<String>,
    pub prepare_commands: Vec<String>,
    pub prepare_env: Vec<String>,
    pub prepare_root: Option<bool>,
    pub prepare_mounts: Vec<String>,
    pub force_rebuild_due_to_mounts: bool,
    pub created_at: u64,
    pub last_used_at: u64,
    pub tool_version: String,
    /// Source URI of a staged input (e.g. `hf://org/model`). Only set for
    /// `Dataset`/`Model` entries; omitted from image-manifest JSON so existing
    /// `Base`/`Prepared` manifests serialize byte-identically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Pinned revision of a staged input (e.g. a git tag or commit). Only set
    /// for `Dataset`/`Model` entries; omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Content digest recorded after a staged input is materialized, when the
    /// materializer can compute one. Only set for `Dataset`/`Model` entries;
    /// omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
}

/// Result returned by cache-pruning operations.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct CachePruneResult {
    pub removed: Vec<PathBuf>,
}

/// How long [`with_manifest_lock`] waits for the advisory lock before degrading
/// to a lock-free read-modify-write (so a dead holder can never wedge the CLI).
const MANIFEST_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Returns the JSON manifest path stored next to an artifact file.
#[must_use]
pub fn manifest_path_for(artifact_path: &Path) -> PathBuf {
    let filename = artifact_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact.sqsh");
    artifact_path.with_file_name(format!("{filename}.json"))
}

/// Path of the sidecar advisory-lock file for an artifact's manifest
/// (`<manifest>.lock`). Kept separate from the manifest so the manifest itself
/// is still replaced via atomic rename, and excluded from [`scan_cache`] by its
/// non-`.json` extension.
fn manifest_lock_path_for(artifact_path: &Path) -> PathBuf {
    let manifest_path = manifest_path_for(artifact_path);
    let mut name = manifest_path
        .file_name()
        .map(OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".lock");
    manifest_path.with_file_name(name)
}

/// Runs a manifest read-modify-write `f` under an exclusive advisory lock on the
/// manifest's sidecar `.lock` file.
///
/// The cache dir is a shared cluster filesystem where multiple jobs may upsert
/// the same manifest concurrently. [`write_manifest`] already makes each
/// individual write atomic, but the surrounding load-modify-write can still lose
/// an update (two writers both load `N` service names, each appends one, the
/// later write clobbers the earlier). Serializing the whole sequence under the
/// lock closes that window on filesystems where `flock` works. It is
/// best-effort: see [`crate::secure_io::with_flock`] for the
/// degrade-on-unsupported / degrade-on-timeout semantics (no cross-node
/// guarantee on NFS-without-lockd or Lustre-without-`-o flock`).
fn with_manifest_lock<T>(artifact_path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let lock_path = manifest_lock_path_for(artifact_path);
    if let Some(parent) = lock_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    crate::secure_io::with_flock(
        &lock_path,
        crate::secure_io::LockKind::Exclusive,
        MANIFEST_LOCK_TIMEOUT,
        f,
    )
}

/// Creates or updates the manifest for an imported base image.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read, the updated
/// manifest cannot be serialized, or the manifest file cannot be written.
pub fn upsert_base_manifest(
    artifact_path: &Path,
    service_name: &str,
    source: &ImageSource,
    cache_key: &str,
) -> Result<CacheEntryManifest> {
    with_manifest_lock(artifact_path, || {
        let mut manifest = load_manifest_if_exists(artifact_path)?
            .unwrap_or_else(|| new_base_manifest(artifact_path, source, cache_key));
        refresh_manifest_common(
            &mut manifest,
            artifact_path,
            service_name,
            source,
            cache_key,
        );
        write_manifest(&manifest)?;
        Ok(manifest)
    })
}

/// Creates or updates the manifest for a prepared runtime image.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read, the updated
/// manifest cannot be serialized, or the manifest file cannot be written.
pub fn upsert_prepared_manifest(
    artifact_path: &Path,
    service_name: &str,
    source: &ImageSource,
    cache_key: &str,
    prepare: &PreparedImageSpec,
) -> Result<CacheEntryManifest> {
    with_manifest_lock(artifact_path, || {
        let mut manifest = load_manifest_if_exists(artifact_path)?
            .unwrap_or_else(|| new_prepared_manifest(artifact_path, source, cache_key, prepare));
        refresh_manifest_common(
            &mut manifest,
            artifact_path,
            service_name,
            source,
            cache_key,
        );
        refresh_prepare_metadata(&mut manifest, prepare);
        write_manifest(&manifest)?;
        Ok(manifest)
    })
}

/// Refreshes the `last_used_at` timestamp for an existing manifest.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read or the refreshed
/// manifest cannot be written back to disk.
pub fn touch_manifest(artifact_path: &Path) -> Result<()> {
    with_manifest_lock(artifact_path, || {
        let Some(mut manifest) = load_manifest_if_exists(artifact_path)? else {
            return Ok(());
        };
        manifest.last_used_at = unix_timestamp_now();
        write_manifest(&manifest)
    })
}

/// Creates or refreshes the sidecar manifest for a staged dataset/model.
///
/// The `staged_dir` is the materialized content-addressed directory; the
/// manifest lands at the `<staged_dir>.{dataset,model}.json` sidecar. Like the
/// image upserts this serializes its read-modify-write under
/// [`with_manifest_lock`] so concurrent sweeps of the same key do not lose an
/// update, and an existing manifest's `created_at`/`service_names` survive.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read or the refreshed
/// manifest cannot be written.
pub fn upsert_dataset_manifest(
    staged_dir: &Path,
    kind: CacheEntryKind,
    cache_key: &str,
    uri: &str,
    revision: Option<&str>,
    content_digest: Option<&str>,
) -> Result<CacheEntryManifest> {
    let suffix = staged_kind_suffix(&kind);
    let manifest_path = dataset::sidecar_manifest_path_for_suffix(staged_dir, suffix);
    with_manifest_lock(staged_dir, || {
        let now = unix_timestamp_now();
        let mut manifest =
            read_staged_manifest_if_exists(&manifest_path)?.unwrap_or_else(|| CacheEntryManifest {
                kind: kind.clone(),
                artifact_path: staged_dir.display().to_string(),
                service_names: Vec::new(),
                cache_key: cache_key.to_string(),
                source_image: uri.to_string(),
                registry: None,
                prepare_commands: Vec::new(),
                prepare_env: Vec::new(),
                prepare_root: None,
                prepare_mounts: Vec::new(),
                force_rebuild_due_to_mounts: false,
                created_at: now,
                last_used_at: now,
                tool_version: env!("CARGO_PKG_VERSION").to_string(),
                uri: Some(uri.to_string()),
                revision: revision.map(str::to_string),
                content_digest: content_digest.map(str::to_string),
            });
        manifest.kind = kind.clone();
        manifest.artifact_path = staged_dir.display().to_string();
        manifest.cache_key = cache_key.to_string();
        manifest.source_image = uri.to_string();
        manifest.uri = Some(uri.to_string());
        manifest.revision = revision.map(str::to_string);
        if let Some(digest) = content_digest {
            manifest.content_digest = Some(digest.to_string());
        }
        manifest.last_used_at = now;
        manifest.tool_version = env!("CARGO_PKG_VERSION").to_string();
        write_manifest_to(&manifest_path, &manifest)?;
        Ok(manifest)
    })
}

/// Refreshes the `last_used_at` timestamp on a staged-input sidecar manifest.
///
/// Mirrors [`touch_manifest`] for the `<staged_dir>.{dataset,model}.json`
/// sidecar layout. A missing manifest is a no-op.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read or written back.
pub fn touch_dataset_manifest(staged_dir: &Path, kind: CacheEntryKind) -> Result<()> {
    let suffix = staged_kind_suffix(&kind);
    let manifest_path = dataset::sidecar_manifest_path_for_suffix(staged_dir, suffix);
    with_manifest_lock(staged_dir, || {
        let Some(mut manifest) = read_staged_manifest_if_exists(&manifest_path)? else {
            return Ok(());
        };
        manifest.last_used_at = unix_timestamp_now();
        write_manifest_to(&manifest_path, &manifest)
    })
}

fn staged_kind_suffix(kind: &CacheEntryKind) -> &'static str {
    match kind {
        CacheEntryKind::Model => "model",
        CacheEntryKind::Source => "source",
        // The CAS only ever upserts Dataset/Model/Source; default the rest to
        // dataset so a misuse still produces a deterministic, scannable sidecar.
        _ => "dataset",
    }
}

/// Test-only: read a staged-input sidecar manifest at an explicit path.
#[cfg(test)]
pub(crate) fn read_staged_manifest_for_test(manifest_path: &Path) -> CacheEntryManifest {
    let raw = fs::read_to_string(manifest_path).expect("read staged manifest");
    serde_json::from_str(&raw).expect("parse staged manifest")
}

fn read_staged_manifest_if_exists(manifest_path: &Path) -> Result<Option<CacheEntryManifest>> {
    if !manifest_path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(manifest_path)
        .context(format!("failed to read {}", manifest_path.display()))?;
    serde_json::from_str(&raw)
        .map(Some)
        .context(format!("failed to parse {}", manifest_path.display()))
}

/// Reads the manifest stored next to an artifact path.
///
/// # Errors
///
/// Returns an error when the manifest file cannot be read or parsed as JSON.
pub fn read_manifest(artifact_path: &Path) -> Result<CacheEntryManifest> {
    let manifest_path = manifest_path_for(artifact_path);
    let raw = fs::read_to_string(&manifest_path)
        .context(format!("failed to read {}", manifest_path.display()))?;
    serde_json::from_str(&raw).context(format!("failed to parse {}", manifest_path.display()))
}

/// Reads a manifest when it exists and returns `None` when it does not.
///
/// # Errors
///
/// Returns an error when the manifest exists but cannot be read or parsed.
pub fn load_manifest_if_exists(artifact_path: &Path) -> Result<Option<CacheEntryManifest>> {
    let manifest_path = manifest_path_for(artifact_path);
    if !manifest_path.exists() {
        return Ok(None);
    }
    read_manifest(artifact_path).map(Some)
}

/// Scans a cache directory recursively for tracked artifact manifests.
///
/// # Errors
///
/// Returns an error when cache directories cannot be traversed or a discovered
/// manifest cannot be read or parsed.
pub fn scan_cache(cache_dir: &Path) -> Result<Vec<CacheEntryManifest>> {
    let mut manifests = Vec::new();
    if !cache_dir.exists() {
        return Ok(manifests);
    }
    let mut stack = vec![cache_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).context(format!("failed to read {}", dir.display()));
            }
        };
        for entry in entries {
            let entry = entry.context(format!("failed to read entry in {}", dir.display()))?;
            let path = entry.path();
            if entry
                .file_type()
                .context(format!("failed to read file type for {}", path.display()))?
                .is_dir()
            {
                // Never recurse into a staged-input directory: its tracking
                // sidecar is a SIBLING (`<dir>.{dataset,model}.json`), and its
                // contents may include unrelated `*.json` (e.g. a model
                // snapshot's `config.json`, or even a data file literally named
                // `x.dataset.json`) that must not be parsed as a manifest.
                if is_staged_input_dir(&path) {
                    // A cluster-side hf:// download leaves only an in-dir
                    // completion marker (no sibling sidecar); synthesize its
                    // manifest so `cache list`/`prune` see it. A dir WITH a
                    // sidecar is listed via that sidecar file below instead.
                    if let Some(manifest) = staged_input_manifest_from_marker(&path) {
                        manifests.push(manifest);
                    }
                    continue;
                }
                stack.push(path);
                continue;
            }
            let is_manifest = path.extension() == Some(OsStr::new("json"));
            if !is_manifest {
                continue;
            }
            if !looks_like_manifest_path(&path) {
                continue;
            }
            let artifact = artifact_path_from_manifest_path(&path);
            // A staged-input sidecar is only valid when its reconstructed
            // artifact directory actually exists as a sibling: this rejects a
            // stray `<x>.dataset.json` data file inside an unrelated tree.
            if is_staged_input_sidecar(&path) && !artifact.is_dir() {
                continue;
            }
            let raw =
                fs::read_to_string(&path).context(format!("failed to read {}", path.display()))?;
            let mut manifest: CacheEntryManifest = serde_json::from_str(&raw)
                .context(format!("failed to parse {}", path.display()))?;
            manifest.artifact_path = artifact.display().to_string();
            manifests.push(manifest);
        }
    }
    manifests.sort_by(|a, b| a.artifact_path.cmp(&b.artifact_path));
    Ok(manifests)
}

/// Removes cached artifacts whose last-use time is older than the cutoff.
///
/// # Errors
///
/// Returns an error when cache manifests cannot be scanned or when a stale
/// manifest or artifact cannot be removed.
pub fn prune_by_age(cache_dir: &Path, age_days: u64) -> Result<CachePruneResult> {
    let cutoff = unix_timestamp_now().saturating_sub(age_days.saturating_mul(SECONDS_PER_DAY));
    let manifests = scan_cache(cache_dir)?;
    let mut removed = Vec::new();
    for manifest in manifests {
        let last = manifest.last_used_at.max(manifest.created_at);
        if last > cutoff {
            continue;
        }
        remove_manifest_and_artifact(cache_dir, &manifest)?;
        removed.push(PathBuf::from(&manifest.artifact_path));
    }
    Ok(CachePruneResult { removed })
}

/// Removes cached artifacts that are not referenced by the given runtime plan.
///
/// # Errors
///
/// Returns an error when cache manifests cannot be scanned or when an unused
/// manifest or artifact cannot be removed.
pub fn prune_all_unused(cache_dir: &Path, plan: &RuntimePlan) -> Result<CachePruneResult> {
    let manifests = unused_cache_manifests(cache_dir, plan)?;
    let mut removed = Vec::new();
    for manifest in manifests {
        let artifact = PathBuf::from(&manifest.artifact_path);
        remove_manifest_and_artifact(cache_dir, &manifest)?;
        removed.push(artifact);
    }
    Ok(CachePruneResult { removed })
}

/// Plans cached artifact removal for artifacts not referenced by the runtime plan.
///
/// This does not delete anything; it returns the same artifact set
/// [`prune_all_unused`] would remove at the time of the scan.
///
/// # Errors
///
/// Returns an error when cache manifests cannot be scanned.
pub fn plan_prune_all_unused(cache_dir: &Path, plan: &RuntimePlan) -> Result<CachePruneResult> {
    let manifests = unused_cache_manifests(cache_dir, plan)?;
    Ok(CachePruneResult {
        removed: manifests
            .into_iter()
            .map(|manifest| PathBuf::from(manifest.artifact_path))
            .collect(),
    })
}

fn unused_cache_manifests(cache_dir: &Path, plan: &RuntimePlan) -> Result<Vec<CacheEntryManifest>> {
    let referenced = referenced_artifacts(plan);
    let manifests = scan_cache(cache_dir)?;
    let mut unused = Vec::new();
    for manifest in manifests {
        let artifact = PathBuf::from(&manifest.artifact_path);
        if referenced.contains(&artifact) {
            continue;
        }
        unused.push(manifest);
    }
    Ok(unused)
}

/// Returns the artifact paths referenced by a runtime plan.
#[must_use]
pub fn referenced_artifacts(plan: &RuntimePlan) -> HashSet<PathBuf> {
    let mut referenced = HashSet::new();
    for service in &plan.ordered_services {
        if !matches!(service.source, ImageSource::Host)
            && !service.runtime_image.as_os_str().is_empty()
        {
            referenced.insert(service.runtime_image.clone());
        }
        if matches!(service.source, ImageSource::Remote(_)) {
            referenced.insert(base_image_path_for_backend(
                &plan.cache_dir,
                service,
                plan.runtime.backend,
            ));
        }
    }
    // Staged inputs referenced by an hf:// stage_in must not be reclaimed by
    // `prune --unused` while a tracked spec still references them. Mirror the
    // key/dir derivation the renderer uses (render/stage.rs).
    for entry in &plan.slurm.stage_in {
        if let Some(hf) = &entry.hf {
            let kind = hf.as_staged_input_kind();
            let spec = dataset::StagedInputSpec::new(kind, hf.uri(), Some(hf.revision.clone()));
            let key = dataset::dataset_cache_key(&spec);
            referenced.insert(dataset::staged_input_dir(&plan.cache_dir, kind, &key));
        }
    }
    referenced
}

/// Computes the cache key used for one service artifact kind.
#[must_use]
pub fn cache_key_for_service(service: &RuntimeService, kind: CacheEntryKind) -> String {
    match kind {
        CacheEntryKind::Base => {
            let source = image_source_string(&service.source);
            format!("base:{}:{}", source, env!("CARGO_PKG_VERSION"))
        }
        CacheEntryKind::Prepared => {
            let Some(prepare) = &service.prepare else {
                return String::new();
            };
            let mut parts = vec![
                "prepared".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
                image_source_string(&service.source),
            ];
            parts.extend(prepare.commands.iter().cloned());
            parts.extend(prepare.mounts.iter().cloned());
            parts.extend(prepare.env.iter().map(format_env_entry));
            parts.push(format!("root={}", prepare.root));
            parts.join("|")
        }
        // Staged inputs are keyed by `dataset::dataset_cache_key`, not by a
        // service; there is no image cache key for them.
        CacheEntryKind::Dataset
        | CacheEntryKind::Model
        | CacheEntryKind::Source
        | CacheEntryKind::Unknown => String::new(),
    }
}

/// Returns the remote registry hostname associated with an image source.
#[must_use]
pub fn parse_remote_registry(source: &ImageSource) -> Option<String> {
    let ImageSource::Remote(remote) = source else {
        return None;
    };
    Some(registry_host_for_remote(remote))
}

fn remove_manifest_and_artifact(cache_dir: &Path, manifest: &CacheEntryManifest) -> Result<()> {
    let artifact = Path::new(&manifest.artifact_path);
    let manifest_path = sidecar_manifest_path_for_kind(artifact, &manifest.kind);
    // Staged inputs (`Dataset`/`Model`) are directories; image artifacts are
    // single files. `symlink_metadata` avoids following a dangling symlink.
    match fs::symlink_metadata(artifact) {
        Ok(meta) if meta.is_dir() => {
            fs::remove_dir_all(artifact)
                .context(format!("failed to remove {}", artifact.display()))?;
        }
        Ok(_) => {
            fs::remove_file(artifact)
                .context(format!("failed to remove {}", artifact.display()))?;
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).context(format!("failed to stat {}", artifact.display()));
        }
    }
    if manifest_path.exists() {
        fs::remove_file(&manifest_path)
            .context(format!("failed to remove {}", manifest_path.display()))?;
    }
    // The whole entry is going away, so reap its advisory-lock sidecar too.
    // Best-effort: a concurrent upsert recreating it is benign (it re-locks a
    // fresh file), and a leftover lock is harmless if removal races.
    let _ = fs::remove_file(manifest_lock_path_for(artifact));
    prune_empty_parents(cache_dir, artifact.parent());
    Ok(())
}

/// Resolves the sidecar-manifest path for an artifact given its kind. Image
/// entries (`Base`/`Prepared`/`Unknown`) use the `<artifact>.json` sibling;
/// staged inputs use the `<staged_dir>.{dataset,model}.json` sidecar so the
/// directory artifact itself stays free of metadata.
fn sidecar_manifest_path_for_kind(artifact: &Path, kind: &CacheEntryKind) -> PathBuf {
    match kind {
        CacheEntryKind::Dataset => dataset::sidecar_manifest_path_for_suffix(artifact, "dataset"),
        CacheEntryKind::Model => dataset::sidecar_manifest_path_for_suffix(artifact, "model"),
        CacheEntryKind::Source => dataset::sidecar_manifest_path_for_suffix(artifact, "source"),
        CacheEntryKind::Base | CacheEntryKind::Prepared | CacheEntryKind::Unknown => {
            manifest_path_for(artifact)
        }
    }
}

/// Collision-safe removal of now-empty parent dirs after an artifact entry is
/// reaped. Walks upward from `start` calling `rmdir` (never recursive), stops at
/// (and never removes) `cache_dir`, and swallows `DirectoryNotEmpty`/`NotFound`
/// so a concurrent writer repopulating a dir, or a racing reaper, is benign
/// (TOCTOU-safe: we rely on rmdir's atomic empty-check, not a prior is_empty).
fn prune_empty_parents(cache_dir: &Path, start: Option<&Path>) {
    let cache_dir = crate::path_util::normalize_path(cache_dir.to_path_buf());
    let mut current = match start {
        Some(path) => crate::path_util::normalize_path(path.to_path_buf()),
        None => return,
    };
    loop {
        if current == cache_dir || !current.starts_with(&cache_dir) {
            return;
        }
        match fs::remove_dir(&current) {
            Ok(()) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                ) =>
            {
                return;
            }
            Err(_) => return,
        }
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => return,
        }
    }
}

fn image_source_string(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path.display().to_string(),
        ImageSource::LocalSif(path) => path.display().to_string(),
        ImageSource::Remote(remote) => remote.clone(),
        ImageSource::Host => "host".to_string(),
    }
}

fn looks_like_manifest_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.ends_with(".sqsh.json")
                || name.ends_with(".squashfs.json")
                || name.ends_with(".sif.json")
                || name.ends_with(".dataset.json")
                || name.ends_with(".model.json")
                || name.ends_with(".source.json")
        })
        .unwrap_or(false)
}

/// Whether `path`'s file name is a staged-input sidecar
/// (`<dir>.dataset.json`/`<dir>.model.json`).
fn is_staged_input_sidecar(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.ends_with(".dataset.json")
                || name.ends_with(".model.json")
                || name.ends_with(".source.json")
        })
        .unwrap_or(false)
}

/// Whether `dir` is a staged-input store directory, detected by either a
/// sibling `<dir>.{dataset,model}.json` tracking sidecar (laptop-side store) or
/// the in-dir [`dataset::HF_COMPLETE_MARKER`] (cluster-side hf:// download).
fn is_staged_input_dir(dir: &Path) -> bool {
    dataset::sidecar_manifest_path_for_suffix(dir, "dataset").is_file()
        || dataset::sidecar_manifest_path_for_suffix(dir, "model").is_file()
        || dataset::sidecar_manifest_path_for_suffix(dir, "source").is_file()
        || dir.join(dataset::HF_COMPLETE_MARKER).is_file()
}

/// Synthesizes a cache manifest for a cluster-staged hf:// directory that has
/// only the in-dir completion marker (no sibling sidecar), so `cache list` and
/// `prune` track it. Returns `None` for a dir that has a sidecar (it is listed
/// via that sidecar file instead, avoiding a double count) or that is not a
/// recognized `datasets`/`models` staged dir.
fn staged_input_manifest_from_marker(dir: &Path) -> Option<CacheEntryManifest> {
    if dataset::sidecar_manifest_path_for_suffix(dir, "dataset").is_file()
        || dataset::sidecar_manifest_path_for_suffix(dir, "model").is_file()
    {
        return None;
    }
    let marker = dir.join(dataset::HF_COMPLETE_MARKER);
    if !marker.is_file() {
        return None;
    }
    let kind = match dir.parent().and_then(|p| p.file_name()?.to_str()) {
        Some("datasets") => CacheEntryKind::Dataset,
        Some("models") => CacheEntryKind::Model,
        _ => return None,
    };
    let used = fs::metadata(&marker)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs());
    Some(CacheEntryManifest {
        kind,
        artifact_path: dir.display().to_string(),
        service_names: Vec::new(),
        cache_key: dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string(),
        source_image: String::new(),
        registry: None,
        prepare_commands: Vec::new(),
        prepare_env: Vec::new(),
        prepare_root: None,
        prepare_mounts: Vec::new(),
        force_rebuild_due_to_mounts: false,
        created_at: used,
        last_used_at: used,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        uri: None,
        revision: None,
        content_digest: None,
    })
}

fn artifact_path_from_manifest_path(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    // Staged-input sidecars are `<staged_dir>.{dataset,model}.json` siblings of
    // the staged DIRECTORY: stripping the full suffix reconstructs that
    // directory, not a phantom `<staged_dir>.dataset` file. Image manifests are
    // `<artifact>.json` and strip only the trailing `.json`.
    let artifact_filename = filename
        .strip_suffix(".dataset.json")
        .or_else(|| filename.strip_suffix(".model.json"))
        .or_else(|| filename.strip_suffix(".source.json"))
        .or_else(|| filename.strip_suffix(".json"))
        .unwrap_or(filename);
    path.with_file_name(artifact_filename)
}

/// Monotonic counter making each temp manifest name unique within this process.
/// Writes the cache manifest atomically.
///
/// The `cache_dir` lives on a shared cluster filesystem where multiple jobs may
/// write the same manifest concurrently. A plain `fs::write` truncates then
/// rewrites in place, so a crash or a concurrent reader can observe a torn,
/// half-written JSON file that then fails to parse and breaks later runs. We
/// delegate to [`crate::secure_io::write_atomic`], which writes to a unique,
/// O_EXCL temp file in the same directory and renames it over the destination
/// (atomic on POSIX, and resistant to symlink/pre-existing-entry attacks in the
/// shared directory).
///
/// Note: this makes each individual write atomic. The surrounding
/// read-modify-write in the `upsert_*`/`touch_manifest` path is serialized
/// separately by [`with_manifest_lock`] to also close the lost-update window
/// (best-effort where `flock` is supported); the manifest that lands is always a
/// complete, valid one regardless.
fn write_manifest(manifest: &CacheEntryManifest) -> Result<()> {
    let artifact = Path::new(&manifest.artifact_path);
    write_manifest_to(&manifest_path_for(artifact), manifest)
}

/// Atomically writes `manifest` to an explicit sidecar path. Used directly for
/// staged-input sidecars whose path (`<dir>.{dataset,model}.json`) does not
/// follow the `<artifact>.json` derivation [`write_manifest`] uses.
fn write_manifest_to(manifest_path: &Path, manifest: &CacheEntryManifest) -> Result<()> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let raw =
        serde_json::to_string_pretty(manifest).context("failed to serialize cache manifest")?;
    crate::secure_io::write_atomic(manifest_path, raw.as_bytes(), false)
        .context(format!("failed to write {}", manifest_path.display()))
}

fn format_env_entry((key, value): &(String, String)) -> String {
    format!("{key}={value}")
}

fn new_base_manifest(
    artifact_path: &Path,
    source: &ImageSource,
    cache_key: &str,
) -> CacheEntryManifest {
    CacheEntryManifest {
        kind: CacheEntryKind::Base,
        artifact_path: artifact_path.display().to_string(),
        service_names: Vec::new(),
        cache_key: cache_key.to_string(),
        source_image: image_source_string(source),
        registry: parse_remote_registry(source),
        prepare_commands: Vec::new(),
        prepare_env: Vec::new(),
        prepare_root: None,
        prepare_mounts: Vec::new(),
        force_rebuild_due_to_mounts: false,
        created_at: unix_timestamp_now(),
        last_used_at: unix_timestamp_now(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        uri: None,
        revision: None,
        content_digest: None,
    }
}

fn new_prepared_manifest(
    artifact_path: &Path,
    source: &ImageSource,
    cache_key: &str,
    prepare: &PreparedImageSpec,
) -> CacheEntryManifest {
    let mut manifest = new_base_manifest(artifact_path, source, cache_key);
    manifest.kind = CacheEntryKind::Prepared;
    refresh_prepare_metadata(&mut manifest, prepare);
    manifest
}

fn refresh_manifest_common(
    manifest: &mut CacheEntryManifest,
    artifact_path: &Path,
    service_name: &str,
    source: &ImageSource,
    cache_key: &str,
) {
    merge_service_name(&mut manifest.service_names, service_name);
    manifest.last_used_at = unix_timestamp_now();
    manifest.artifact_path = artifact_path.display().to_string();
    manifest.cache_key = cache_key.to_string();
    manifest.source_image = image_source_string(source);
    manifest.registry = parse_remote_registry(source);
    manifest.tool_version = env!("CARGO_PKG_VERSION").to_string();
}

fn refresh_prepare_metadata(manifest: &mut CacheEntryManifest, prepare: &PreparedImageSpec) {
    manifest.prepare_commands = prepare.commands.clone();
    manifest.prepare_env = prepare.env.iter().map(format_env_entry).collect();
    manifest.prepare_root = Some(prepare.root);
    manifest.prepare_mounts = prepare.mounts.clone();
    manifest.force_rebuild_due_to_mounts = prepare.force_rebuild;
}

fn merge_service_name(service_names: &mut Vec<String>, service_name: &str) {
    if service_names
        .iter()
        .any(|existing| existing == service_name)
    {
        return;
    }
    service_names.push(service_name.to_string());
    service_names.sort();
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::planner::{ExecutionSpec, ImageSource, PreparedImageSpec, ServicePlacement};
    use crate::prepare::{RuntimeService, base_image_path_for_backend};
    use crate::spec::{ServiceFailurePolicy, ServiceSlurmConfig};

    fn runtime_service() -> RuntimeService {
        RuntimeService {
            name: "svc".into(),
            runtime_image: PathBuf::from("/shared/cache/prepared/svc.sqsh"),
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: Some(PreparedImageSpec {
                commands: vec!["apt-get update".into()],
                mounts: vec!["/host:/mnt".into()],
                env: vec![("A".into(), "B".into())],
                root: true,
                force_rebuild: true,
            }),
            source: ImageSource::Remote("docker://registry.scc.kit.edu#proj/app:latest".into()),
        }
    }

    #[test]
    fn writes_and_reads_prepared_manifest() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = tmpdir.path().join("prepared.sqsh");
        fs::write(&artifact, "x").expect("artifact");
        let service = runtime_service();
        upsert_prepared_manifest(
            &artifact,
            &service.name,
            &service.source,
            "cache-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("manifest");
        let manifest = read_manifest(&artifact).expect("read");
        assert_eq!(manifest.kind, CacheEntryKind::Prepared);
        assert!(manifest.service_names.contains(&"svc".to_string()));
        assert_eq!(manifest.registry.as_deref(), Some("registry.scc.kit.edu"));
    }

    #[test]
    fn concurrent_writes_never_produce_torn_manifest() {
        use std::sync::Arc;
        use std::thread;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = Arc::new(tmpdir.path().join("shared.sqsh"));
        fs::write(artifact.as_ref(), "x").expect("artifact");
        let source = ImageSource::Remote("docker://redis:7".into());

        // Many threads hammer the same manifest path; an atomic rename guarantees
        // every reader (and the final state) always sees a complete JSON file.
        let mut handles = Vec::new();
        for index in 0..16 {
            let artifact = Arc::clone(&artifact);
            let source = source.clone();
            handles.push(thread::spawn(move || {
                for round in 0..20 {
                    upsert_base_manifest(
                        &artifact,
                        &format!("svc-{index}-{round}"),
                        &source,
                        "shared-key",
                    )
                    .expect("upsert");
                    // Concurrently read the manifest back; with a non-atomic write
                    // this would intermittently observe a torn, unparseable file.
                    if manifest_path_for(&artifact).exists() {
                        read_manifest(&artifact).expect("manifest parses cleanly");
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread");
        }

        let manifest = read_manifest(&artifact).expect("final manifest parses");
        assert_eq!(manifest.kind, CacheEntryKind::Base);
        assert_eq!(manifest.cache_key, "shared-key");
        assert_eq!(manifest.registry.as_deref(), Some("registry-1.docker.io"));
        assert!(!manifest.service_names.is_empty());

        // No temp files should be left behind in the cache dir.
        let leftovers: Vec<_> = fs::read_dir(tmpdir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".tmp."))
            })
            .collect();
        assert!(leftovers.is_empty(), "stray temp manifests: {leftovers:?}");
    }

    // The advisory lock around the read-modify-write must serialize concurrent
    // upserts so that EVERY distinct service name survives. Without the lock,
    // interleaved load/modify/write sequences silently drop names. (Proves
    // local-FS behavior only; flock may be a no-op on some networked mounts.)
    #[test]
    fn concurrent_upserts_do_not_lose_service_names() {
        use std::sync::Arc;
        use std::thread;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = Arc::new(tmpdir.path().join("shared.sqsh"));
        fs::write(artifact.as_ref(), "x").expect("artifact");
        let source = ImageSource::Remote("docker://redis:7".into());

        const WRITERS: usize = 24;
        let mut handles = Vec::new();
        for index in 0..WRITERS {
            let artifact = Arc::clone(&artifact);
            let source = source.clone();
            handles.push(thread::spawn(move || {
                upsert_base_manifest(&artifact, &format!("svc-{index:02}"), &source, "shared-key")
                    .expect("upsert");
            }));
        }
        for handle in handles {
            handle.join().expect("thread");
        }

        let manifest = read_manifest(&artifact).expect("final manifest");
        let expected: Vec<String> = (0..WRITERS).map(|i| format!("svc-{i:02}")).collect();
        assert_eq!(
            manifest.service_names, expected,
            "every concurrent upsert's service name must survive (no lost updates)"
        );
    }

    #[test]
    fn prune_empty_parents_stops_at_cache_root_and_nonempty_dirs() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path();

        // An empty nested chain is removed entirely, but never the cache root.
        let nested = cache.join("base/registry/img");
        fs::create_dir_all(&nested).expect("nested");
        prune_empty_parents(cache, Some(&nested));
        assert!(!nested.exists());
        assert!(!cache.join("base/registry").exists());
        assert!(!cache.join("base").exists());
        assert!(cache.exists(), "cache root must never be removed");

        // A non-empty parent halts the walk.
        let occupied = cache.join("keep/here");
        fs::create_dir_all(&occupied).expect("occupied");
        fs::write(cache.join("keep/file"), "x").expect("file");
        prune_empty_parents(cache, Some(&occupied));
        assert!(!occupied.exists());
        assert!(cache.join("keep").exists(), "non-empty parent kept");
    }

    #[test]
    fn prune_unused_removes_non_referenced_artifacts() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let keep = tmpdir.path().join("keep.sqsh");
        let drop = tmpdir.path().join("drop.sqsh");
        fs::write(&keep, "k").expect("keep");
        fs::write(&drop, "d").expect("drop");
        let service = runtime_service();
        upsert_prepared_manifest(
            &keep,
            &service.name,
            &service.source,
            "keep-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("keep manifest");
        upsert_prepared_manifest(
            &drop,
            "other",
            &service.source,
            "drop-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("drop manifest");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().to_path_buf(),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: crate::spec::SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                runtime_image: keep.clone(),
                ..service
            }],
        };
        let result = prune_all_unused(tmpdir.path(), &plan).expect("prune");
        assert_eq!(result.removed, vec![drop.clone()]);
        assert!(keep.exists());
        assert!(!drop.exists());
    }

    #[test]
    fn scan_cache_derives_artifact_path_from_manifest_location() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = tmpdir.path().join("safe.sqsh");
        let outside = tmpdir.path().join("outside.sqsh");
        fs::write(&artifact, "safe").expect("artifact");
        fs::write(&outside, "outside").expect("outside");
        let manifest = CacheEntryManifest {
            kind: CacheEntryKind::Prepared,
            artifact_path: outside.display().to_string(),
            service_names: vec!["svc".into()],
            cache_key: "safe-key".into(),
            source_image: "docker://redis:7".into(),
            registry: Some("registry-1.docker.io".into()),
            prepare_commands: Vec::new(),
            prepare_env: Vec::new(),
            prepare_root: Some(true),
            prepare_mounts: Vec::new(),
            force_rebuild_due_to_mounts: false,
            created_at: 1,
            last_used_at: 1,
            tool_version: env!("CARGO_PKG_VERSION").into(),
            uri: None,
            revision: None,
            content_digest: None,
        };
        fs::write(
            manifest_path_for(&artifact),
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");

        let pruned = prune_by_age(tmpdir.path(), 0).expect("prune");
        assert_eq!(pruned.removed, vec![artifact.clone()]);
        assert!(!artifact.exists());
        assert!(outside.exists());
    }

    #[test]
    fn base_manifest_touch_and_load_cover_additional_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = tmpdir.path().join("base.sqsh");
        fs::write(&artifact, "x").expect("artifact");
        let source = ImageSource::Remote("docker://redis:7".into());
        assert_eq!(
            manifest_path_for(&artifact),
            tmpdir.path().join("base.sqsh.json")
        );

        let first = upsert_base_manifest(&artifact, "svc-a", &source, "base-key").expect("first");
        assert_eq!(first.kind, CacheEntryKind::Base);
        assert_eq!(first.registry.as_deref(), Some("registry-1.docker.io"));

        let second = upsert_base_manifest(&artifact, "svc-b", &source, "base-key").expect("second");
        assert!(second.service_names.contains(&"svc-a".to_string()));
        assert!(second.service_names.contains(&"svc-b".to_string()));

        touch_manifest(&artifact).expect("touch");
        let loaded = load_manifest_if_exists(&artifact)
            .expect("load")
            .expect("manifest");
        assert_eq!(loaded.cache_key, "base-key");
        touch_manifest(&tmpdir.path().join("missing-touch.sqsh")).expect("touch missing");
        assert!(
            load_manifest_if_exists(&tmpdir.path().join("missing.sqsh"))
                .expect("missing")
                .is_none()
        );
    }

    #[test]
    fn scan_cache_and_prune_by_age_cover_empty_and_recent_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        assert!(scan_cache(tmpdir.path()).expect("empty").is_empty());
        assert!(
            scan_cache(&tmpdir.path().join("does-not-exist"))
                .expect("missing dir")
                .is_empty()
        );
        let recent_artifact = tmpdir.path().join("recent.sqsh");
        fs::write(&recent_artifact, "x").expect("recent");
        let service = runtime_service();
        upsert_prepared_manifest(
            &recent_artifact,
            &service.name,
            &service.source,
            "recent-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("manifest");

        let scanned = scan_cache(tmpdir.path()).expect("scan");
        assert_eq!(scanned.len(), 1);
        let pruned = prune_by_age(tmpdir.path(), 1).expect("prune");
        assert!(pruned.removed.is_empty());
        assert!(recent_artifact.exists());

        let nested = tmpdir.path().join("nested");
        fs::create_dir_all(&nested).expect("nested");
        fs::write(nested.join("note.txt"), "ignore").expect("note");
        let old_artifact = nested.join("old.sqsh");
        fs::write(&old_artifact, "x").expect("old");
        upsert_prepared_manifest(
            &old_artifact,
            &service.name,
            &service.source,
            "old-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("old manifest");
        let old_manifest_path = manifest_path_for(&old_artifact);
        let mut old_manifest: CacheEntryManifest = serde_json::from_str(
            &fs::read_to_string(&old_manifest_path).expect("read old manifest"),
        )
        .expect("parse old manifest");
        old_manifest.created_at = 1;
        old_manifest.last_used_at = 1;
        fs::write(
            &old_manifest_path,
            serde_json::to_vec_pretty(&old_manifest).expect("serialize old manifest"),
        )
        .expect("rewrite old manifest");

        let scanned = scan_cache(tmpdir.path()).expect("scan nested");
        assert_eq!(scanned.len(), 2);
        let pruned = prune_by_age(tmpdir.path(), 0).expect("prune old");
        assert!(pruned.removed.contains(&old_artifact));
        assert!(!old_artifact.exists());
        assert!(!old_manifest_path.exists());
    }

    #[test]
    fn cache_key_registry_and_reference_helpers_cover_remaining_branches() {
        let service = runtime_service();
        let base_key = cache_key_for_service(&service, CacheEntryKind::Base);
        let prepared_key = cache_key_for_service(&service, CacheEntryKind::Prepared);
        assert!(base_key.starts_with("base:docker://"));
        assert!(prepared_key.contains("prepared"));
        assert_eq!(
            parse_remote_registry(&ImageSource::Remote("docker://redis:7".into())),
            Some("registry-1.docker.io".into())
        );
        assert_eq!(
            parse_remote_registry(&ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh"))),
            None
        );

        let local_service = RuntimeService {
            name: "local".into(),
            runtime_image: PathBuf::from("/tmp/local.sqsh"),
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh")),
        };
        assert_eq!(
            cache_key_for_service(&local_service, CacheEntryKind::Prepared),
            ""
        );

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/shared/cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: crate::spec::SlurmConfig::default(),
            ordered_services: vec![service.clone(), local_service.clone()],
        };
        let referenced = referenced_artifacts(&plan);
        let expected = HashSet::from([
            service.runtime_image.clone(),
            base_image_path_for_backend(&plan.cache_dir, &service, plan.runtime.backend),
            local_service.runtime_image.clone(),
        ]);
        assert_eq!(referenced, expected);
    }

    #[test]
    fn read_and_scan_errors_are_reported() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = tmpdir.path().join("broken.sqsh");
        let manifest = manifest_path_for(&artifact);
        let read_err = read_manifest(&artifact).expect_err("missing manifest");
        assert!(read_err.to_string().contains("failed to read"));
        fs::write(&manifest, "{not json").expect("broken manifest");
        let err = read_manifest(&artifact).expect_err("broken");
        assert!(err.to_string().contains("failed to parse"));
        let err = scan_cache(tmpdir.path()).expect_err("scan broken");
        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn scan_cache_ignores_unrelated_json_files() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_dir = tmpdir.path().join("runtime/job-123");
        fs::create_dir_all(&runtime_dir).expect("runtime dir");
        fs::write(runtime_dir.join("state.json"), "{\"status\":\"ok\"}").expect("state");

        let artifact = tmpdir.path().join("keep.sqsh");
        fs::write(&artifact, "x").expect("artifact");
        let service = runtime_service();
        upsert_prepared_manifest(
            &artifact,
            &service.name,
            &service.source,
            "keep-key",
            service.prepare.as_ref().expect("prepare"),
        )
        .expect("manifest");

        let scanned = scan_cache(tmpdir.path()).expect("scan");
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].artifact_path, artifact.display().to_string());
    }

    // --- staged-input (CAS) manifest integration ---

    use crate::cache::dataset::{
        StagedInputKind, StagedInputProof, StagedInputSpec, dataset_cache_key, ensure_staged_input,
        staged_input_dir,
    };

    fn seed_staged(cache_dir: &Path, kind: StagedInputKind, uri: &str) -> PathBuf {
        let spec = StagedInputSpec::new(kind, uri, Some("v1".into()));
        let (dir, _action) = ensure_staged_input(cache_dir, &spec, |dest| {
            fs::write(dest.join("payload.bin"), b"data").expect("payload");
            // A model snapshot's config.json must never be mistaken for a manifest.
            fs::write(dest.join("config.json"), b"{}").expect("config");
            Ok(StagedInputProof {
                content_digest: Some("sha256:abc".into()),
            })
        })
        .expect("seed staged");
        dir
    }

    #[test]
    fn scan_cache_discovers_dataset_and_model_sidecars() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path();
        let ds_dir = seed_staged(cache, StagedInputKind::Dataset, "hf://org/cifar10");
        let md_dir = seed_staged(cache, StagedInputKind::Model, "hf://org/llm");

        let scanned = scan_cache(cache).expect("scan");
        assert_eq!(scanned.len(), 2, "both staged sidecars discovered");

        let dataset = scanned
            .iter()
            .find(|m| m.kind == CacheEntryKind::Dataset)
            .expect("dataset entry");
        // The artifact path points at the staged DIRECTORY, not a phantom file.
        assert_eq!(dataset.artifact_path, ds_dir.display().to_string());
        assert!(Path::new(&dataset.artifact_path).is_dir());
        assert_eq!(dataset.uri.as_deref(), Some("hf://org/cifar10"));
        assert_eq!(dataset.revision.as_deref(), Some("v1"));
        assert_eq!(dataset.content_digest.as_deref(), Some("sha256:abc"));

        let model = scanned
            .iter()
            .find(|m| m.kind == CacheEntryKind::Model)
            .expect("model entry");
        assert_eq!(model.artifact_path, md_dir.display().to_string());
        assert!(Path::new(&model.artifact_path).is_dir());

        // The inner config.json inside a staged dir must NOT be parsed as a manifest.
        assert!(
            !scanned
                .iter()
                .any(|m| m.artifact_path.ends_with("config.json")),
            "scan_cache must not descend into staged-input dirs"
        );
    }

    #[test]
    fn cache_entry_kind_unknown_round_trips() {
        // An unrecognized kind from a newer tool deserializes to Unknown and
        // re-serializes without panicking.
        let raw = r#"{
            "kind": "some_future_kind",
            "artifact_path": "/cache/x.sqsh",
            "service_names": [],
            "cache_key": "k",
            "source_image": "docker://redis:7",
            "registry": null,
            "prepare_commands": [],
            "prepare_env": [],
            "prepare_root": null,
            "prepare_mounts": [],
            "force_rebuild_due_to_mounts": false,
            "created_at": 1,
            "last_used_at": 1,
            "tool_version": "9.9.9"
        }"#;
        let manifest: CacheEntryManifest = serde_json::from_str(raw).expect("parse unknown kind");
        assert_eq!(manifest.kind, CacheEntryKind::Unknown);
        let reserialized = serde_json::to_string(&manifest).expect("reserialize");
        let round: CacheEntryManifest =
            serde_json::from_str(&reserialized).expect("re-parse unknown kind");
        assert_eq!(round.kind, CacheEntryKind::Unknown);
    }

    #[test]
    fn cache_manifest_optional_fields_omitted_when_none() {
        // A Base manifest with no staged-input fields must serialize byte-for-byte
        // identically to the pre-CAS layout (no uri/revision/content_digest keys).
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let artifact = tmpdir.path().join("base.sqsh");
        let manifest = new_base_manifest(
            &artifact,
            &ImageSource::Remote("docker://redis:7".into()),
            "base-key",
        );
        let json = serde_json::to_string_pretty(&manifest).expect("serialize base manifest");
        assert!(
            !json.contains("\"uri\""),
            "uri key must be omitted when None"
        );
        assert!(
            !json.contains("\"revision\""),
            "revision key must be omitted when None"
        );
        assert!(
            !json.contains("\"content_digest\""),
            "content_digest key must be omitted when None"
        );
        // And it still parses (no required-field break for old/new readers).
        let parsed: CacheEntryManifest = serde_json::from_str(&json).expect("reparse");
        assert_eq!(parsed.kind, CacheEntryKind::Base);
        assert_eq!(parsed.uri, None);
    }

    #[test]
    fn old_base_manifest_without_new_fields_still_loads() {
        // Simulate an on-disk manifest written by a pre-CAS tool version: it has
        // none of the new optional fields. It must deserialize cleanly.
        let raw = r#"{
            "kind": "base",
            "artifact_path": "/cache/base/redis.sqsh",
            "service_names": ["svc"],
            "cache_key": "base:docker://redis:7:0.1.0",
            "source_image": "docker://redis:7",
            "registry": "registry-1.docker.io",
            "prepare_commands": [],
            "prepare_env": [],
            "prepare_root": null,
            "prepare_mounts": [],
            "force_rebuild_due_to_mounts": false,
            "created_at": 100,
            "last_used_at": 200,
            "tool_version": "0.1.0"
        }"#;
        let manifest: CacheEntryManifest = serde_json::from_str(raw).expect("parse old manifest");
        assert_eq!(manifest.kind, CacheEntryKind::Base);
        assert_eq!(manifest.uri, None);
        assert_eq!(manifest.revision, None);
        assert_eq!(manifest.content_digest, None);
    }

    #[test]
    fn prune_by_age_and_prune_all_unused_remove_staged_input_dirs() {
        // prune --age 0 removes a staged dir via remove_dir_all and reaps the
        // now-empty kind-segment parent (but never the cache root).
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path();
        let ds_dir = seed_staged(cache, StagedInputKind::Dataset, "hf://org/cifar10");
        assert!(ds_dir.is_dir());
        let sidecar = dataset::sidecar_manifest_path_for_suffix(&ds_dir, "dataset");
        assert!(sidecar.is_file());

        let pruned = prune_by_age(cache, 0).expect("prune age 0");
        assert_eq!(pruned.removed, vec![ds_dir.clone()]);
        assert!(!ds_dir.exists(), "staged dir removed via remove_dir_all");
        assert!(!sidecar.exists(), "sidecar removed");
        assert!(
            !cache.join("datasets").exists(),
            "empty kind-segment parent pruned"
        );
        assert!(cache.exists(), "cache root never removed");

        // prune --all-unused removes a staged dir not referenced by the plan.
        let md_dir = seed_staged(cache, StagedInputKind::Model, "hf://org/llm");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: cache.to_path_buf(),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: crate::spec::SlurmConfig::default(),
            ordered_services: Vec::new(),
        };
        let pruned = prune_all_unused(cache, &plan).expect("prune unused");
        assert_eq!(pruned.removed, vec![md_dir.clone()]);
        assert!(!md_dir.exists());
    }

    #[test]
    fn prune_all_unused_retains_hf_referenced_staged_dir() {
        // A staged dir referenced by an hf:// stage_in must survive --all-unused.
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path();
        let referenced = seed_staged(cache, StagedInputKind::Model, "hf://org/keep");
        let orphan = seed_staged(cache, StagedInputKind::Model, "hf://org/drop");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: cache.to_path_buf(),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: crate::spec::SlurmConfig {
                stage_in: vec![crate::spec::StageInConfig {
                    from: None,
                    to: "/weights".into(),
                    mode: crate::spec::StageMode::Copy,
                    hf: Some(crate::spec::HfStageSource {
                        repo: "org/keep".into(),
                        revision: "v1".into(),
                        kind: crate::spec::HfStageKind::Model,
                    }),
                }],
                ..crate::spec::SlurmConfig::default()
            },
            ordered_services: Vec::new(),
        };
        let pruned = prune_all_unused(cache, &plan).expect("prune unused");
        assert_eq!(
            pruned.removed,
            vec![orphan.clone()],
            "only the unreferenced staged dir is reaped"
        );
        assert!(referenced.is_dir(), "hf-referenced staged dir retained");
        assert!(!orphan.exists());
    }

    #[test]
    fn scan_cache_lists_cluster_marker_only_staged_dir() {
        // A cluster-side hf:// download leaves only the in-dir completion marker
        // (no sibling sidecar); it must still be visible to `cache list`/`prune`.
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache = tmpdir.path();
        let model_dir = cache.join("models").join("deadbeefcafe0000");
        fs::create_dir_all(&model_dir).expect("model dir");
        fs::write(model_dir.join(dataset::HF_COMPLETE_MARKER), b"").expect("marker");
        // The payload's own config.json must NOT be mistaken for a manifest.
        fs::write(model_dir.join("config.json"), b"{}").expect("inner config");

        let manifests = scan_cache(cache).expect("scan");
        assert_eq!(manifests.len(), 1, "one synthesized entry: {manifests:?}");
        assert_eq!(manifests[0].kind, CacheEntryKind::Model);
        assert_eq!(manifests[0].artifact_path, model_dir.display().to_string());
    }

    #[test]
    fn staged_input_key_matches_module_layout() {
        let cache = Path::new("/shared/cache");
        let spec = StagedInputSpec::new(StagedInputKind::Dataset, "hf://org/x", None);
        let key = dataset_cache_key(&spec);
        assert_eq!(
            staged_input_dir(cache, StagedInputKind::Dataset, &key),
            cache.join("datasets").join(&key)
        );
    }

    #[test]
    fn scan_cache_and_prune_track_source_snapshots() {
        // A source snapshot staged via the CAS is discovered by scan_cache as a
        // Source entry pointing at the staged DIRECTORY, and is prunable. Guards
        // the discovery/label touch points patched for CacheEntryKind::Source.
        let tmp = tempfile::tempdir().expect("tmp");
        let work = tmp.path().join("work");
        let cache = tmp.path().join("cache");
        fs::create_dir_all(work.join("src")).expect("work dir");
        fs::write(work.join("src/main.rs"), b"fn main() {}").expect("src");

        let snap = crate::cache::source::stage_source(&work, &cache).expect("stage");

        let scanned = scan_cache(&cache).expect("scan");
        assert_eq!(scanned.len(), 1, "one source entry discovered: {scanned:?}");
        assert_eq!(scanned[0].kind, CacheEntryKind::Source);
        assert_eq!(scanned[0].artifact_path, snap.dir.display().to_string());
        assert!(Path::new(&scanned[0].artifact_path).is_dir());
        // The inner source file must NOT be parsed as (or descended into for) a manifest.
        assert!(
            !scanned.iter().any(|m| m.artifact_path.ends_with("main.rs")),
            "scan must not descend into the source snapshot dir"
        );

        let pruned = prune_by_age(&cache, 0).expect("prune");
        assert_eq!(pruned.removed, vec![snap.dir.clone()]);
        assert!(!snap.dir.exists(), "source snapshot dir removed");
        assert!(
            !dataset::sidecar_manifest_path_for_suffix(&snap.dir, "source").exists(),
            "source sidecar removed"
        );
    }
}
