//! Cache manifest management for imported and prepared image artifacts.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::planner::{ImageSource, PreparedImageSpec, registry_host_for_remote};
use crate::prepare::{RuntimePlan, RuntimeService, base_image_path_for_backend};

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

/// Returns the JSON manifest path stored next to an artifact file.
#[must_use]
pub fn manifest_path_for(artifact_path: &Path) -> PathBuf {
    let filename = artifact_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact.sqsh");
    artifact_path.with_file_name(format!("{filename}.json"))
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
    let mut manifest =
        load_manifest_if_exists(artifact_path)?.unwrap_or_else(|| CacheEntryManifest {
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
        });
    merge_service_name(&mut manifest.service_names, service_name);
    manifest.last_used_at = unix_timestamp_now();
    manifest.artifact_path = artifact_path.display().to_string();
    manifest.cache_key = cache_key.to_string();
    manifest.source_image = image_source_string(source);
    manifest.registry = parse_remote_registry(source);
    manifest.tool_version = env!("CARGO_PKG_VERSION").to_string();
    write_manifest(&manifest)?;
    Ok(manifest)
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
    let mut manifest =
        load_manifest_if_exists(artifact_path)?.unwrap_or_else(|| CacheEntryManifest {
            kind: CacheEntryKind::Prepared,
            artifact_path: artifact_path.display().to_string(),
            service_names: Vec::new(),
            cache_key: cache_key.to_string(),
            source_image: image_source_string(source),
            registry: parse_remote_registry(source),
            prepare_commands: prepare.commands.clone(),
            prepare_env: prepare.env.iter().map(format_env_entry).collect(),
            prepare_root: Some(prepare.root),
            prepare_mounts: prepare.mounts.clone(),
            force_rebuild_due_to_mounts: prepare.force_rebuild,
            created_at: unix_timestamp_now(),
            last_used_at: unix_timestamp_now(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
        });
    merge_service_name(&mut manifest.service_names, service_name);
    manifest.last_used_at = unix_timestamp_now();
    manifest.artifact_path = artifact_path.display().to_string();
    manifest.cache_key = cache_key.to_string();
    manifest.source_image = image_source_string(source);
    manifest.registry = parse_remote_registry(source);
    manifest.prepare_commands = prepare.commands.clone();
    manifest.prepare_env = prepare.env.iter().map(format_env_entry).collect();
    manifest.prepare_root = Some(prepare.root);
    manifest.prepare_mounts = prepare.mounts.clone();
    manifest.force_rebuild_due_to_mounts = prepare.force_rebuild;
    manifest.tool_version = env!("CARGO_PKG_VERSION").to_string();
    write_manifest(&manifest)?;
    Ok(manifest)
}

/// Refreshes the `last_used_at` timestamp for an existing manifest.
///
/// # Errors
///
/// Returns an error when an existing manifest cannot be read or the refreshed
/// manifest cannot be written back to disk.
pub fn touch_manifest(artifact_path: &Path) -> Result<()> {
    let Some(mut manifest) = load_manifest_if_exists(artifact_path)? else {
        return Ok(());
    };
    manifest.last_used_at = unix_timestamp_now();
    write_manifest(&manifest)
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
            let manifest: CacheEntryManifest = serde_json::from_str(&raw)
                .context(format!("failed to parse {}", path.display()))?;
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
    let cutoff = unix_timestamp_now().saturating_sub(age_days.saturating_mul(24 * 60 * 60));
    let manifests = scan_cache(cache_dir)?;
    let mut removed = Vec::new();
    for manifest in manifests {
        let last = manifest.last_used_at.max(manifest.created_at);
        if last > cutoff {
            continue;
        }
        remove_manifest_and_artifact(&manifest)?;
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
        remove_manifest_and_artifact(&manifest)?;
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

fn remove_manifest_and_artifact(manifest: &CacheEntryManifest) -> Result<()> {
    let artifact = Path::new(&manifest.artifact_path);
    let manifest_path = manifest_path_for(artifact);
    if artifact.exists() {
        fs::remove_file(artifact).context(format!("failed to remove {}", artifact.display()))?;
    }
    if manifest_path.exists() {
        fs::remove_file(&manifest_path)
            .context(format!("failed to remove {}", manifest_path.display()))?;
    }
    Ok(())
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

fn write_manifest(manifest: &CacheEntryManifest) -> Result<()> {
    let artifact = Path::new(&manifest.artifact_path);
    let manifest_path = manifest_path_for(artifact);
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let raw =
        serde_json::to_string_pretty(manifest).context("failed to serialize cache manifest")?;
    fs::write(&manifest_path, raw)
        .context(format!("failed to write {}", manifest_path.display()))?;
    Ok(())
}

fn format_env_entry((key, value): &(String, String)) -> String {
    format!("{key}={value}")
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

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
