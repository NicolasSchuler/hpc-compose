//! Shared-cache rendezvous records for cross-job service discovery.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::time_util;

const RENDEZVOUS_DIR_NAME: &str = "rendezvous";
const LATEST_FILE_NAME: &str = "latest.json";
const RECORD_SCHEMA_VERSION: u32 = 1;

/// One provider registration stored in the shared cache.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct RendezvousRecord {
    pub schema_version: u32,
    pub name: String,
    pub job_id: String,
    #[serde(default)]
    pub service: Option<String>,
    pub host: String,
    pub port: u16,
    pub protocol: String,
    #[serde(default)]
    pub path: Option<String>,
    pub url: String,
    pub registered_at: u64,
    pub ttl_seconds: u64,
    pub cache_dir: PathBuf,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// User or renderer input for a provider registration.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RendezvousRegisterRequest {
    pub name: String,
    pub job_id: String,
    pub service: Option<String>,
    pub host: String,
    pub port: u16,
    pub protocol: String,
    pub path: Option<String>,
    pub ttl_seconds: u64,
    pub metadata: BTreeMap<String, String>,
}

/// Summary returned by pruning expired records.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct RendezvousPruneReport {
    pub cache_dir: PathBuf,
    pub removed: Vec<PathBuf>,
}

/// Returns the current Unix timestamp in seconds.
#[must_use]
pub fn unix_timestamp_now() -> u64 {
    time_util::unix_timestamp_now()
}

/// Validates a rendezvous name accepted in spec and CLI inputs.
///
/// # Errors
///
/// Returns an error when the value is empty or cannot be represented as one
/// safe path component.
pub fn validate_name(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("rendezvous name must not be empty");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("rendezvous name must contain only ASCII letters, digits, '.', '_', or '-'");
    }
    Ok(())
}

/// Converts a rendezvous name into the stable uppercase environment token.
#[must_use]
pub fn env_token(name: &str) -> String {
    let mut token = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() {
            token.push((byte as char).to_ascii_uppercase());
        } else {
            token.push('_');
        }
    }
    if token.is_empty() {
        "_".to_string()
    } else {
        token
    }
}

/// Returns `<cache_dir>/rendezvous`.
#[must_use]
pub fn root_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join(RENDEZVOUS_DIR_NAME)
}

/// Returns the directory for one rendezvous name.
///
/// # Errors
///
/// Returns an error when the name is invalid.
pub fn entry_dir(cache_dir: &Path, name: &str) -> Result<PathBuf> {
    validate_name(name)?;
    Ok(root_dir(cache_dir).join(name))
}

/// Returns the atomic latest pointer path for one rendezvous name.
///
/// # Errors
///
/// Returns an error when the name is invalid.
pub fn latest_path(cache_dir: &Path, name: &str) -> Result<PathBuf> {
    Ok(entry_dir(cache_dir, name)?.join(LATEST_FILE_NAME))
}

/// Builds a record from validated registration input.
///
/// # Errors
///
/// Returns an error when the input is invalid.
pub fn build_record(
    cache_dir: &Path,
    request: RendezvousRegisterRequest,
    now: u64,
) -> Result<RendezvousRecord> {
    validate_name(&request.name)?;
    if request.job_id.trim().is_empty() {
        bail!("rendezvous job id must not be empty");
    }
    if request.host.trim().is_empty() {
        bail!("rendezvous host must not be empty");
    }
    if request.port == 0 {
        bail!("rendezvous port must be at least 1");
    }
    if request.protocol.trim().is_empty() {
        bail!("rendezvous protocol must not be empty");
    }
    if request.ttl_seconds == 0 {
        bail!("rendezvous ttl must be at least 1 second");
    }
    let path = request.path.filter(|path| !path.is_empty());
    if let Some(path) = path.as_deref()
        && !path.starts_with('/')
    {
        bail!("rendezvous path must be empty or start with '/'");
    }
    let url = format!(
        "{}://{}:{}{}",
        request.protocol,
        request.host,
        request.port,
        path.as_deref().unwrap_or("")
    );
    Ok(RendezvousRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        name: request.name,
        job_id: request.job_id,
        service: request.service,
        host: request.host,
        port: request.port,
        protocol: request.protocol,
        path,
        url,
        registered_at: now,
        ttl_seconds: request.ttl_seconds,
        cache_dir: cache_dir.to_path_buf(),
        metadata: request.metadata,
    })
}

/// Registers a provider and updates its latest pointer atomically.
///
/// # Errors
///
/// Returns an error when validation or filesystem writes fail.
pub fn register(cache_dir: &Path, record: &RendezvousRecord) -> Result<PathBuf> {
    let dir = entry_dir(cache_dir, &record.name)?;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let record_path = dir.join(format!("{}.json", record_file_token(record)));
    write_json_atomic(&record_path, record)?;
    write_json_atomic(&dir.join(LATEST_FILE_NAME), record)?;
    Ok(record_path)
}

/// Resolves the latest non-expired record for `name`.
///
/// # Errors
///
/// Returns an error when the cache record cannot be parsed.
pub fn resolve(cache_dir: &Path, name: &str, now: u64) -> Result<Option<RendezvousRecord>> {
    resolve_with_liveness(cache_dir, name, now, |_| Ok(true))
}

/// Resolves the latest record and checks scheduler liveness through `is_live`.
///
/// # Errors
///
/// Returns an error when the cache record cannot be parsed or liveness fails.
pub fn resolve_with_liveness<F>(
    cache_dir: &Path,
    name: &str,
    now: u64,
    mut is_live: F,
) -> Result<Option<RendezvousRecord>>
where
    F: FnMut(&str) -> Result<bool>,
{
    let path = latest_path(cache_dir, name)?;
    if !path.exists() {
        return Ok(None);
    }
    let record: RendezvousRecord = read_json(&path)?;
    if record.cache_dir != cache_dir {
        return Ok(None);
    }
    if record.is_expired(now) {
        return Ok(None);
    }
    if !is_live(&record.job_id)? {
        return Ok(None);
    }
    Ok(Some(record))
}

/// Lists latest non-expired records under the cache rendezvous root.
///
/// # Errors
///
/// Returns an error when the root cannot be read.
pub fn list(cache_dir: &Path, now: u64) -> Result<Vec<RendezvousRecord>> {
    let root = root_dir(cache_dir);
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in
        fs::read_dir(&root).with_context(|| format!("failed to read {}", root.display()))?
    {
        let entry = entry?;
        if !entry
            .file_type()
            .with_context(|| format!("failed to stat {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let latest = entry.path().join(LATEST_FILE_NAME);
        if !latest.exists() {
            continue;
        }
        let record: RendezvousRecord = read_json(&latest)?;
        if record.cache_dir == cache_dir && !record.is_expired(now) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(records)
}

/// Builds a report of expired latest and historical records in a rendezvous cache.
///
/// # Errors
///
/// Returns an error when directory reads fail.
pub fn build_prune_report(cache_dir: &Path, now: u64) -> Result<RendezvousPruneReport> {
    let root = root_dir(cache_dir);
    let mut removed = Vec::new();
    if !root.is_dir() {
        return Ok(RendezvousPruneReport {
            cache_dir: cache_dir.to_path_buf(),
            removed,
        });
    }
    for entry in
        fs::read_dir(&root).with_context(|| format!("failed to read {}", root.display()))?
    {
        let entry = entry?;
        if !entry
            .file_type()
            .with_context(|| format!("failed to stat {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        for file in fs::read_dir(entry.path())
            .with_context(|| format!("failed to read {}", entry.path().display()))?
        {
            let file = file?;
            let path = file.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(record) = read_json::<RendezvousRecord>(&path) else {
                continue;
            };
            if record.cache_dir == cache_dir && record.is_expired(now) {
                removed.push(path);
            }
        }
    }
    removed.sort();
    Ok(RendezvousPruneReport {
        cache_dir: cache_dir.to_path_buf(),
        removed,
    })
}

/// Executes a rendezvous prune report generated by [`build_prune_report`].
///
/// # Errors
///
/// Returns an error when an expired record cannot be removed.
pub fn run_prune_report(report: &RendezvousPruneReport) -> Result<()> {
    let now = unix_timestamp_now();
    for path in &report.removed {
        let Ok(record) = read_json::<RendezvousRecord>(path) else {
            continue;
        };
        if record.cache_dir == report.cache_dir && record.is_expired(now) {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    Ok(())
}

/// Removes expired latest and historical records from a rendezvous cache.
///
/// # Errors
///
/// Returns an error when directory reads or removals fail.
pub fn prune(cache_dir: &Path, now: u64) -> Result<RendezvousPruneReport> {
    let report = build_prune_report(cache_dir, now)?;
    run_prune_report(&report)?;
    Ok(report)
}

/// Removes `latest.json` only when `job_id` owns the current latest record.
///
/// # Errors
///
/// Returns an error when the record cannot be parsed or removed.
pub fn deregister_if_owner(cache_dir: &Path, name: &str, job_id: &str) -> Result<bool> {
    let path = latest_path(cache_dir, name)?;
    if !path.exists() {
        return Ok(false);
    }
    let record: RendezvousRecord = read_json(&path)?;
    if record.job_id != job_id {
        return Ok(false);
    }
    fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    Ok(true)
}

/// Reaps every rendezvous record under `cache_dir` owned by `job_id`: removes
/// each per-name record file (including `latest.json`) whose `job_id` and
/// `cache_dir` match. Other jobs' records and live `latest.json` pointers are
/// left intact (owner-guarded). Used by `down`/`cancel` so a job's historical
/// `<token>.json` records do not accumulate until TTL expiry.
///
/// # Errors
///
/// Returns an error only when the rendezvous root cannot be traversed;
/// individual unparseable record files are skipped.
pub fn reap_job_records(cache_dir: &Path, job_id: &str) -> Result<Vec<PathBuf>> {
    let root = root_dir(cache_dir);
    let mut removed = Vec::new();
    if !root.is_dir() {
        return Ok(removed);
    }
    for entry in
        fs::read_dir(&root).with_context(|| format!("failed to read {}", root.display()))?
    {
        let entry = entry?;
        if !entry
            .file_type()
            .with_context(|| format!("failed to stat {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        for file in fs::read_dir(entry.path())
            .with_context(|| format!("failed to read {}", entry.path().display()))?
        {
            let file = file?;
            let path = file.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(record) = read_json::<RendezvousRecord>(&path) else {
                continue;
            };
            if record.cache_dir == cache_dir && record.job_id == job_id {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove {}", path.display()))?;
                removed.push(path);
            }
        }
    }
    removed.sort();
    Ok(removed)
}

impl RendezvousRecord {
    /// Returns true if `now` falls outside the record TTL.
    #[must_use]
    pub fn is_expired(&self, now: u64) -> bool {
        now.saturating_sub(self.registered_at) >= self.ttl_seconds
    }
}

fn record_file_token(record: &RendezvousRecord) -> String {
    let service = record
        .service
        .as_deref()
        .map(tokenize_component)
        .unwrap_or_else(|| "manual".to_string());
    format!("{}-{service}", tokenize_component(&record.job_id))
}

fn tokenize_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let payload =
        serde_json::to_vec_pretty(value).context("failed to serialize rendezvous JSON")?;
    for attempt in 0_u32..100 {
        let tmp = unique_temp_path(path, attempt);
        match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(mut file) => {
                file.write_all(&payload)
                    .with_context(|| format!("failed to write {}", tmp.display()))?;
                file.flush()
                    .with_context(|| format!("failed to flush {}", tmp.display()))?;
                return fs::rename(&tmp, path).with_context(|| {
                    format!("failed to rename {} to {}", tmp.display(), path.display())
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("failed to create {}", tmp.display()));
            }
        }
    }
    bail!(
        "failed to allocate a unique temporary rendezvous path for {}",
        path.display()
    )
}

fn unique_temp_path(path: &Path, attempt: u32) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rendezvous.json");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(
        ".{file_name}.{}.{}.{}.tmp",
        std::process::id(),
        nanos,
        attempt
    ))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::*;

    fn request(name: &str, job_id: &str, ttl_seconds: u64) -> RendezvousRegisterRequest {
        RendezvousRegisterRequest {
            name: name.to_string(),
            job_id: job_id.to_string(),
            service: Some("server".to_string()),
            host: "node-a".to_string(),
            port: 8000,
            protocol: "http".to_string(),
            path: Some("/v1".to_string()),
            ttl_seconds,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn name_validation_rejects_path_components() {
        assert!(validate_name("model-server_1.0").is_ok());
        assert!(validate_name("../bad").is_err());
        assert!(validate_name("bad/name").is_err());
        assert!(validate_name("").is_err());
    }

    #[test]
    fn env_token_is_stable_and_uppercase() {
        assert_eq!(env_token("model-server.v1"), "MODEL_SERVER_V1");
        assert_eq!(env_token("a b"), "A_B");
    }

    #[test]
    fn register_updates_latest_and_keeps_history() {
        let dir = tempdir().expect("tempdir");
        let first = build_record(dir.path(), request("model", "101", 60), 10).expect("first");
        let second = build_record(dir.path(), request("model", "102", 60), 11).expect("second");

        let first_path = register(dir.path(), &first).expect("register first");
        let second_path = register(dir.path(), &second).expect("register second");
        let latest = resolve(dir.path(), "model", 12)
            .expect("resolve")
            .expect("record");

        assert!(first_path.exists());
        assert!(second_path.exists());
        assert_eq!(latest.job_id, "102");
    }

    #[test]
    fn resolve_filters_expired_and_dead_records() {
        let dir = tempdir().expect("tempdir");
        let record = build_record(dir.path(), request("model", "101", 5), 10).expect("record");
        register(dir.path(), &record).expect("register");

        assert!(resolve(dir.path(), "model", 16).expect("resolve").is_none());
        let live = resolve_with_liveness(dir.path(), "model", 11, |_| Ok(false))
            .expect("resolve with liveness");
        assert!(live.is_none());
    }

    #[test]
    fn deregister_only_removes_owner_latest() {
        let dir = tempdir().expect("tempdir");
        let record = build_record(dir.path(), request("model", "101", 60), 10).expect("record");
        register(dir.path(), &record).expect("register");

        assert!(!deregister_if_owner(dir.path(), "model", "999").expect("not owner"));
        assert!(resolve(dir.path(), "model", 11).expect("resolve").is_some());
        assert!(deregister_if_owner(dir.path(), "model", "101").expect("owner"));
        assert!(resolve(dir.path(), "model", 11).expect("resolve").is_none());
    }

    #[test]
    fn reap_job_records_removes_only_owned_records() {
        let dir = tempdir().expect("tempdir");
        let first = build_record(dir.path(), request("model", "101", 60), 10).expect("first");
        let second = build_record(dir.path(), request("model", "102", 60), 11).expect("second");
        register(dir.path(), &first).expect("register first");
        register(dir.path(), &second).expect("register second");

        // Reaping 101 removes only its records; 102 and the live latest survive.
        let removed = reap_job_records(dir.path(), "101").expect("reap 101");
        assert!(!removed.is_empty());
        let latest = resolve(dir.path(), "model", 12)
            .expect("resolve")
            .expect("record");
        assert_eq!(latest.job_id, "102");

        // Reaping 102 (the current latest owner) removes its records and latest.
        let removed = reap_job_records(dir.path(), "102").expect("reap 102");
        assert!(!removed.is_empty());
        assert!(resolve(dir.path(), "model", 13).expect("resolve").is_none());
    }

    #[test]
    fn prune_removes_expired_records() {
        let dir = tempdir().expect("tempdir");
        let record = build_record(dir.path(), request("model", "101", 5), 10).expect("record");
        register(dir.path(), &record).expect("register");

        let report = prune(dir.path(), 16).expect("prune");
        assert_eq!(report.removed.len(), 2);
        assert!(list(dir.path(), 16).expect("list").is_empty());
    }

    #[test]
    fn run_prune_report_revalidates_expiry_before_removing() {
        let dir = tempdir().expect("tempdir");
        let now = unix_timestamp_now();
        let expired =
            build_record(dir.path(), request("model", "101", 5), now - 10).expect("expired");
        let historical = register(dir.path(), &expired).expect("register");
        let report = build_prune_report(dir.path(), now).expect("report");
        assert!(report.removed.contains(&historical));

        let refreshed =
            build_record(dir.path(), request("model", "101", 600), now).expect("refreshed");
        write_json_atomic(&historical, &refreshed).expect("refresh historical");

        run_prune_report(&report).expect("run stale report");
        assert!(historical.exists());
        assert_eq!(
            read_json::<RendezvousRecord>(&historical)
                .expect("historical")
                .ttl_seconds,
            600
        );
    }

    #[test]
    fn timestamp_helper_is_unix_seconds() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let now = unix_timestamp_now();
        assert!(now >= before);
    }
}
