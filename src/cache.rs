//! Cache manifest management for imported and prepared image artifacts.

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheEntryKind {
    /// A base image imported directly from a remote reference.
    Base,
    /// A prepared runtime image derived from a base image.
    Prepared,
}

/// Metadata stored next to a cached artifact.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            let raw =
                fs::read_to_string(&path).context(format!("failed to read {}", path.display()))?;
            let mut manifest: CacheEntryManifest = serde_json::from_str(&raw)
                .context(format!("failed to parse {}", path.display()))?;
            manifest.artifact_path = artifact_path_from_manifest_path(&path)
                .display()
                .to_string();
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
    let referenced = referenced_artifacts(plan);
    let manifests = scan_cache(cache_dir)?;
    let mut removed = Vec::new();
    for manifest in manifests {
        let artifact = PathBuf::from(&manifest.artifact_path);
        if referenced.contains(&artifact) {
            continue;
        }
        remove_manifest_and_artifact(cache_dir, &manifest)?;
        removed.push(artifact);
    }
    Ok(CachePruneResult { removed })
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
    let manifest_path = manifest_path_for(artifact);
    if artifact.exists() {
        fs::remove_file(artifact).context(format!("failed to remove {}", artifact.display()))?;
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
        })
        .unwrap_or(false)
}

fn artifact_path_from_manifest_path(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let artifact_filename = filename.strip_suffix(".json").unwrap_or(filename);
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
    let manifest_path = manifest_path_for(artifact);
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let raw =
        serde_json::to_string_pretty(manifest).context("failed to serialize cache manifest")?;
    crate::secure_io::write_atomic(&manifest_path, raw.as_bytes(), false)
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
}
