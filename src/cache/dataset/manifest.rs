//! Rebuildable staged-input tracking sidecars.

use std::path::{Path, PathBuf};

use anyhow::Result;

use super::StagedInputKind;
use crate::cache::{self, CacheEntryKind, CacheEntryManifest};
use crate::time_util::unix_timestamp_now;

const STAGED_INPUT_KINDS: [StagedInputKind; 3] = [
    StagedInputKind::Dataset,
    StagedInputKind::Model,
    StagedInputKind::Source,
];

pub(in crate::cache) fn upsert(
    staged_dir: &Path,
    kind: CacheEntryKind,
    cache_key: &str,
    uri: &str,
    revision: Option<&str>,
    content_digest: Option<&str>,
) -> Result<CacheEntryManifest> {
    let manifest_path = tracking_sidecar_path(staged_dir, &kind);
    cache::with_manifest_lock(staged_dir, || {
        let now = unix_timestamp_now();
        let mut manifest = read_if_exists(&manifest_path)?.unwrap_or_else(|| CacheEntryManifest {
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
        manifest.content_digest = content_digest.map(str::to_string);
        manifest.last_used_at = now;
        manifest.tool_version = env!("CARGO_PKG_VERSION").to_string();
        cache::write_manifest_to(&manifest_path, &manifest)?;
        Ok(manifest)
    })
}

pub(in crate::cache) fn touch(staged_dir: &Path, kind: CacheEntryKind) -> Result<()> {
    let manifest_path = tracking_sidecar_path(staged_dir, &kind);
    cache::with_manifest_lock(staged_dir, || {
        let Some(mut manifest) = read_if_exists(&manifest_path)? else {
            return Ok(());
        };
        manifest.last_used_at = unix_timestamp_now();
        cache::write_manifest_to(&manifest_path, &manifest)
    })
}

pub(super) fn read_if_exists(manifest_path: &Path) -> Result<Option<CacheEntryManifest>> {
    cache::read_manifest_file_if_exists(manifest_path)
}

pub(super) fn sidecar_path_for_staged_kind(staged_dir: &Path, kind: StagedInputKind) -> PathBuf {
    let mut name = staged_dir
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(sidecar_filename_suffix(kind));
    staged_dir.with_file_name(name)
}

pub(in crate::cache) fn sidecar_path_for_cache_kind(
    staged_dir: &Path,
    kind: &CacheEntryKind,
) -> Option<PathBuf> {
    staged_kind_for_cache_kind(kind).map(|kind| sidecar_path_for_staged_kind(staged_dir, kind))
}

pub(in crate::cache) fn is_sidecar_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            STAGED_INPUT_KINDS
                .into_iter()
                .any(|kind| name.ends_with(sidecar_filename_suffix(kind)))
        })
}

pub(in crate::cache) fn has_sidecar(staged_dir: &Path) -> bool {
    STAGED_INPUT_KINDS
        .into_iter()
        .any(|kind| sidecar_path_for_staged_kind(staged_dir, kind).is_file())
}

pub(in crate::cache) fn artifact_path_from_sidecar(path: &Path) -> Option<PathBuf> {
    let filename = path.file_name()?.to_str()?;
    STAGED_INPUT_KINDS.into_iter().find_map(|kind| {
        filename
            .strip_suffix(sidecar_filename_suffix(kind))
            .map(|artifact| path.with_file_name(artifact))
    })
}

#[cfg(test)]
pub(in crate::cache) fn sidecar_manifest_path_for_suffix(
    staged_dir: &Path,
    suffix: &str,
) -> PathBuf {
    let mut name = staged_dir
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(format!(".{suffix}.json"));
    staged_dir.with_file_name(name)
}

fn tracking_sidecar_path(staged_dir: &Path, kind: &CacheEntryKind) -> PathBuf {
    // The CAS only upserts Dataset/Model/Source. Preserve the established
    // dataset fallback so unsupported use remains deterministic and scannable.
    let staged_kind = staged_kind_for_cache_kind(kind).unwrap_or(StagedInputKind::Dataset);
    sidecar_path_for_staged_kind(staged_dir, staged_kind)
}

fn staged_kind_for_cache_kind(kind: &CacheEntryKind) -> Option<StagedInputKind> {
    match kind {
        CacheEntryKind::Dataset => Some(StagedInputKind::Dataset),
        CacheEntryKind::Model => Some(StagedInputKind::Model),
        CacheEntryKind::Source => Some(StagedInputKind::Source),
        CacheEntryKind::Base | CacheEntryKind::Prepared | CacheEntryKind::Unknown => None,
    }
}

fn sidecar_filename_suffix(kind: StagedInputKind) -> &'static str {
    match kind {
        StagedInputKind::Dataset => ".dataset.json",
        StagedInputKind::Model => ".model.json",
        StagedInputKind::Source => ".source.json",
    }
}
