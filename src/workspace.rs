//! hpc-workspace (`ws_*`) lifecycle integration.
//!
//! Many HPC sites (KIT HoreKa/HAICORE style) manage expiring scratch
//! directories with the hpc-workspace tools: `ws_allocate <name> [days]`
//! creates an expiring directory, `ws_find <name>` prints its path,
//! `ws_extend <name> <days>` renews it, `ws_release <name>` frees it, and
//! `ws_list` shows workspaces with their remaining lifetime. This module
//! backs the `workspace` command group with:
//!
//! * thin wrappers around the five tools (each binary overridable so fake
//!   tools can stand in during tests),
//! * a tolerant line-based `ws_list` parser (field sets vary across
//!   hpc-workspace versions and sites),
//! * the persisted per-profile state file
//!   `.hpc-compose/workspace-state.toml` next to `settings.toml`, and
//! * the release guard that refuses to free a workspace still referenced by
//!   tracked job records.
//!
//! Resolved workspace paths deliberately live in the state file rather than
//! `settings.toml` or `cluster.toml`: profiles can target different clusters,
//! and `cluster.toml` is one-per-repo.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::time_util::{SECONDS_PER_DAY, SECONDS_PER_HOUR};

/// File name of the persisted workspace state, stored in the same directory
/// as `settings.toml`.
pub const WORKSPACE_STATE_FILE_NAME: &str = "workspace-state.toml";

/// Relative location of the workspace state file from a repo root, used when
/// no settings file exists yet.
pub const WORKSPACE_STATE_RELATIVE_PATH: &str = ".hpc-compose/workspace-state.toml";

/// Fallback allocation/renewal duration in days when neither the CLI flag nor
/// the settings `duration_days` is set.
pub const DEFAULT_WORKSPACE_DURATION_DAYS: u32 = 30;

/// State-file map key used when no settings profile is selected.
pub const DEFAULT_STATE_PROFILE_KEY: &str = "default";

const WORKSPACE_STATE_SCHEMA_VERSION: u32 = 1;

/// Persisted resolved workspace facts, keyed by settings profile name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceState {
    /// State-file schema version (currently 1).
    #[serde(default = "default_state_schema_version")]
    pub version: u32,
    /// Facts per settings profile; key [`DEFAULT_STATE_PROFILE_KEY`] when no
    /// profile is selected.
    #[serde(default)]
    pub profiles: BTreeMap<String, WorkspaceStateEntry>,
}

impl Default for WorkspaceState {
    fn default() -> Self {
        Self {
            version: WORKSPACE_STATE_SCHEMA_VERSION,
            profiles: BTreeMap::new(),
        }
    }
}

fn default_state_schema_version() -> u32 {
    WORKSPACE_STATE_SCHEMA_VERSION
}

/// Resolved facts recorded for one profile's workspace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStateEntry {
    /// Workspace name passed to the `ws_*` tools.
    pub name: String,
    /// Path printed by `ws_find`.
    pub path: PathBuf,
    /// Absolute expiry time (unix seconds) computed from `ws_list`'s
    /// remaining time; `None` when `ws_list` was unavailable or did not
    /// report a parseable remaining time.
    #[serde(default)]
    pub expiry_epoch: Option<u64>,
    /// Extensions still available per `ws_list`; `None` when not reported.
    #[serde(default)]
    pub extensions_remaining: Option<u32>,
    /// Unix time this entry was last refreshed from the `ws_*` tools.
    pub last_checked: u64,
}

/// Returns the workspace state file path for a resolved context: next to the
/// discovered `settings.toml` when one exists, otherwise under
/// `<repo-root-or-cwd>/.hpc-compose/`.
#[must_use]
pub fn workspace_state_path(settings_path: Option<&Path>, cwd: &Path) -> PathBuf {
    match settings_path.and_then(Path::parent) {
        Some(dir) => dir.join(WORKSPACE_STATE_FILE_NAME),
        None => crate::context::repo_root_or_cwd(cwd).join(WORKSPACE_STATE_RELATIVE_PATH),
    }
}

/// Loads the workspace state file, returning an empty default when the file
/// does not exist yet.
///
/// The state file is a regenerable cache of `ws_*` facts, so an unparsable
/// file or an unknown schema version must never brick the very commands
/// whose refresh would repair it (and `release` must never fail *after*
/// `ws_release` ran because of it): both cases warn on stderr and fall back
/// to the empty default, which the caller's refresh rewrites in canonical
/// form.
///
/// # Errors
///
/// Returns an error only when an existing file cannot be read at all (for
/// example, permission denied) — a genuine I/O failure, not stale content.
pub fn load_workspace_state(path: &Path) -> Result<WorkspaceState> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorkspaceState::default());
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to read workspace state {}", path.display()));
        }
    };
    match toml::from_str::<WorkspaceState>(&raw) {
        Ok(state) if state.version == WORKSPACE_STATE_SCHEMA_VERSION => Ok(state),
        Ok(state) => {
            eprintln!(
                "warning: workspace state {} has unsupported schema version {} (expected {}); \
                 treating it as empty — it is a regenerable cache and will be rebuilt",
                path.display(),
                state.version,
                WORKSPACE_STATE_SCHEMA_VERSION
            );
            Ok(WorkspaceState::default())
        }
        Err(err) => {
            eprintln!(
                "warning: workspace state {} is unreadable ({err}); treating it as empty — \
                 it is a regenerable cache and will be rebuilt",
                path.display()
            );
            Ok(WorkspaceState::default())
        }
    }
}

/// Atomically writes the workspace state file, creating parent directories.
///
/// # Errors
///
/// Returns an error when serialization or the atomic write fails.
pub fn save_workspace_state(path: &Path, state: &WorkspaceState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = toml::to_string_pretty(state).context("failed to serialize workspace state")?;
    crate::secure_io::write_atomic(path, rendered.as_bytes(), false)
        .with_context(|| format!("failed to write {}", path.display()))
}

/// Records an observation into the state map under `profile_key`.
pub fn record_observation(
    state: &mut WorkspaceState,
    profile_key: &str,
    observation: &WorkspaceObservation,
    now: u64,
) {
    state.profiles.insert(
        profile_key.to_string(),
        WorkspaceStateEntry {
            name: observation.name.clone(),
            path: observation.path.clone(),
            expiry_epoch: observation.expiry_epoch,
            extensions_remaining: observation.extensions_remaining,
            last_checked: now,
        },
    );
}

/// Paths of the five hpc-workspace executables used by the `workspace`
/// commands. Each is overridable per invocation via the matching
/// `--ws-*-bin` flag (following the `--sbatch-bin` pattern), which is also
/// what lets fake-tool test fixtures stand in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceTools {
    /// `ws_find` — prints an existing workspace's path.
    pub ws_find: String,
    /// `ws_allocate` — creates an expiring workspace.
    pub ws_allocate: String,
    /// `ws_extend` — renews an existing workspace.
    pub ws_extend: String,
    /// `ws_release` — frees a workspace.
    pub ws_release: String,
    /// `ws_list` — lists workspaces with remaining lifetime.
    pub ws_list: String,
}

fn run_tool(bin: &str, args: &[&str]) -> Result<std::process::Output> {
    Command::new(bin).args(args).output().with_context(|| {
        format!(
            "failed to run `{bin} {}`: are the hpc-workspace tools installed and on PATH \
             (override the path with the matching --ws-*-bin flag)?",
            args.join(" ")
        )
    })
}

fn ensure_success(bin: &str, args: &[&str], output: &std::process::Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    bail!(
        "`{bin} {}` exited with {}{}",
        args.join(" "),
        output.status,
        if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        }
    );
}

/// Runs `ws_find <name>` and returns the workspace path when it exists.
///
/// A zero exit with a non-empty trimmed stdout means the workspace exists; a
/// non-zero exit means "not found" (hpc-workspace's contract).
///
/// # Errors
///
/// Returns an error only when the tool cannot be executed at all (for
/// example, not installed / not on PATH).
pub fn find_workspace(tools: &WorkspaceTools, name: &str) -> Result<Option<PathBuf>> {
    let output = run_tool(&tools.ws_find, &[name])?;
    if !output.status.success() {
        return Ok(None);
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if path.is_empty() {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(path)))
}

/// Runs `ws_allocate <name> <days>` to create a workspace.
///
/// Callers must guard with [`find_workspace`] first and only allocate when
/// the workspace is missing: re-allocating an existing workspace errors on
/// some hpc-workspace versions and silently extends on others, so
/// hpc-compose never relies on either behavior.
///
/// # Errors
///
/// Returns an error when the tool cannot be executed or exits non-zero.
pub fn allocate_workspace(tools: &WorkspaceTools, name: &str, days: u32) -> Result<()> {
    let days = days.to_string();
    let args = [name, days.as_str()];
    let output = run_tool(&tools.ws_allocate, &args)?;
    ensure_success(&tools.ws_allocate, &args, &output)
}

/// Runs `ws_extend <name> <days>` to renew a workspace.
///
/// # Errors
///
/// Returns an error when the tool cannot be executed or exits non-zero.
pub fn extend_workspace(tools: &WorkspaceTools, name: &str, days: u32) -> Result<()> {
    let days = days.to_string();
    let args = [name, days.as_str()];
    let output = run_tool(&tools.ws_extend, &args)?;
    ensure_success(&tools.ws_extend, &args, &output)
}

/// Runs `ws_release <name>` to free a workspace.
///
/// # Errors
///
/// Returns an error when the tool cannot be executed or exits non-zero.
pub fn release_workspace(tools: &WorkspaceTools, name: &str) -> Result<()> {
    let args = [name];
    let output = run_tool(&tools.ws_release, &args)?;
    ensure_success(&tools.ws_release, &args, &output)
}

/// Runs `ws_list` and parses its output tolerantly into per-workspace
/// entries.
///
/// # Errors
///
/// Returns an error when the tool cannot be executed or exits non-zero.
pub fn list_workspaces(tools: &WorkspaceTools) -> Result<Vec<WsListEntry>> {
    let output = run_tool(&tools.ws_list, &[])?;
    ensure_success(&tools.ws_list, &[], &output)?;
    Ok(parse_ws_list(&String::from_utf8_lossy(&output.stdout)))
}

/// One workspace block parsed from `ws_list` output.
///
/// Every field except the id is optional: field sets vary across
/// hpc-workspace versions and sites, and the parser tolerates missing or
/// unknown lines rather than failing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WsListEntry {
    /// Workspace id from the `id:` line that starts a block.
    pub id: String,
    /// `workspace directory` field.
    pub directory: Option<PathBuf>,
    /// Remaining lifetime in seconds computed from the `remaining time`
    /// field; `None` when absent or unparseable.
    pub remaining_seconds: Option<u64>,
    /// Raw `remaining time` value as printed by `ws_list`.
    pub remaining_display: Option<String>,
    /// Raw `expiration date` value (locale-formatted, deliberately not
    /// parsed; carried as a display fallback).
    pub expiration_display: Option<String>,
    /// `available extensions` field.
    pub available_extensions: Option<u32>,
}

/// Parses `ws_list` output: blocks start at `id:` lines (the key before the
/// first `:` is trimmed and compared case-insensitively, so `Id : name`
/// variants are recognized too); within a block, known fields are matched on
/// their lowercase key prefix. Unknown lines — including arbitrary non-ASCII
/// or lossy-decoded noise — are skipped and missing fields stay `None`; the
/// parser never slices at fixed byte offsets, so it cannot panic on
/// multi-byte UTF-8 input.
#[must_use]
pub fn parse_ws_list(raw: &str) -> Vec<WsListEntry> {
    let mut entries = Vec::new();
    let mut current: Option<WsListEntry> = None;
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key.eq_ignore_ascii_case("id") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            current = Some(WsListEntry {
                id: value.to_string(),
                ..WsListEntry::default()
            });
            continue;
        }
        let Some(entry) = current.as_mut() else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        let key = key.to_ascii_lowercase();
        if key.starts_with("workspace directory") {
            entry.directory = Some(PathBuf::from(value));
        } else if key.starts_with("remaining time") {
            entry.remaining_display = Some(value.to_string());
            entry.remaining_seconds = parse_remaining_time_seconds(value);
        } else if key.starts_with("expiration date") {
            entry.expiration_display = Some(value.to_string());
        } else if key.starts_with("available extensions") {
            entry.available_extensions = value.parse().ok();
        }
    }
    if let Some(entry) = current.take() {
        entries.push(entry);
    }
    entries
}

/// Parses a tolerant `N days M hours ...` remaining-time phrase into seconds.
///
/// Recognizes `day`/`hour`/`minute`/`second` units (singular or plural, with
/// trailing punctuation tolerated); unknown tokens are skipped. Returns
/// `None` when no duration token matched, so callers can fall back to the
/// raw display string.
#[must_use]
pub fn parse_remaining_time_seconds(raw: &str) -> Option<u64> {
    let tokens: Vec<&str> = raw.split_whitespace().collect();
    let mut total: u64 = 0;
    let mut matched = false;
    for pair in tokens.windows(2) {
        let Ok(value) = pair[0].trim_matches(',').parse::<u64>() else {
            continue;
        };
        let unit = pair[1]
            .trim_matches(|c: char| !c.is_ascii_alphabetic())
            .to_ascii_lowercase();
        let unit_seconds = if unit.starts_with("day") {
            SECONDS_PER_DAY
        } else if unit.starts_with("hour") {
            SECONDS_PER_HOUR
        } else if unit.starts_with("min") {
            60
        } else if unit.starts_with("sec") {
            1
        } else {
            continue;
        };
        total = total.saturating_add(value.saturating_mul(unit_seconds));
        matched = true;
    }
    matched.then_some(total)
}

/// Live facts about one workspace: the `ws_find` path plus best-effort
/// expiry details from `ws_list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceObservation {
    /// Workspace name.
    pub name: String,
    /// Path printed by `ws_find`.
    pub path: PathBuf,
    /// Absolute expiry time (unix seconds) computed as `now` plus the parsed
    /// remaining time; `None` when unavailable.
    pub expiry_epoch: Option<u64>,
    /// Raw remaining-time string from `ws_list`.
    pub remaining_display: Option<String>,
    /// Raw expiration-date string from `ws_list`; display fallback when the
    /// remaining time could not be parsed.
    pub expiry_display: Option<String>,
    /// Extensions still available per `ws_list`.
    pub extensions_remaining: Option<u32>,
}

/// Looks up one workspace: `ws_find` decides existence and yields the path;
/// `ws_list` fills expiry details best-effort (a missing or failing `ws_list`
/// degrades the extra fields to `None` instead of failing the lookup).
///
/// # Errors
///
/// Returns an error only when `ws_find` itself cannot be executed.
pub fn observe_workspace(
    tools: &WorkspaceTools,
    name: &str,
    now: u64,
) -> Result<Option<WorkspaceObservation>> {
    let Some(path) = find_workspace(tools, name)? else {
        return Ok(None);
    };
    let entry = list_workspaces(tools)
        .ok()
        .and_then(|entries| entries.into_iter().find(|entry| entry.id == name));
    let (expiry_epoch, remaining_display, expiry_display, extensions_remaining) = match entry {
        Some(entry) => (
            entry
                .remaining_seconds
                .map(|seconds| now.saturating_add(seconds)),
            entry.remaining_display,
            entry.expiration_display,
            entry.available_extensions,
        ),
        None => (None, None, None, None),
    };
    Ok(Some(WorkspaceObservation {
        name: name.to_string(),
        path,
        expiry_epoch,
        remaining_display,
        expiry_display,
        extensions_remaining,
    }))
}

/// Returns the sorted, deduplicated job ids of tracked submission records
/// whose `cache_dir` or `runtime_root` lies under `workspace_path` (lexical
/// component-wise prefix check).
///
/// `workspace release` refuses to free a workspace while any tracked job
/// still keeps cache or runtime state under it.
#[must_use]
pub fn job_ids_blocking_release(
    records: &[crate::job::SubmissionRecord],
    workspace_path: &Path,
) -> Vec<String> {
    let mut ids: Vec<String> = records
        .iter()
        .filter(|record| {
            record.cache_dir.starts_with(workspace_path)
                || record
                    .runtime_root
                    .as_deref()
                    .is_some_and(|root| root.starts_with(workspace_path))
        })
        .map(|record| record.job_id.clone())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_WS_LIST: &str = "\
id: hpc-compose-cache
     workspace directory  : /hkfs/work/workspace/scratch/ab1234-hpc-compose-cache
     remaining time       : 29 days 23 hours
     creation time        : Thu Jul  3 10:00:00 2026
     expiration date      : Mon Aug  3 10:00:00 2026
     available extensions : 3
";

    #[test]
    fn parse_ws_list_reads_a_full_block() {
        let entries = parse_ws_list(FULL_WS_LIST);
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.id, "hpc-compose-cache");
        assert_eq!(
            entry.directory.as_deref(),
            Some(Path::new(
                "/hkfs/work/workspace/scratch/ab1234-hpc-compose-cache"
            ))
        );
        assert_eq!(
            entry.remaining_seconds,
            Some(29 * SECONDS_PER_DAY + 23 * SECONDS_PER_HOUR)
        );
        assert_eq!(entry.remaining_display.as_deref(), Some("29 days 23 hours"));
        assert_eq!(
            entry.expiration_display.as_deref(),
            Some("Mon Aug  3 10:00:00 2026")
        );
        assert_eq!(entry.available_extensions, Some(3));
    }

    #[test]
    fn parse_ws_list_handles_multiple_blocks_and_minimal_fields() {
        let raw = "\
id: first
     workspace directory  : /scratch/u-first
id: second
";
        let entries = parse_ws_list(raw);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "first");
        assert_eq!(
            entries[0].directory.as_deref(),
            Some(Path::new("/scratch/u-first"))
        );
        assert_eq!(entries[0].remaining_seconds, None);
        assert_eq!(entries[0].available_extensions, None);
        assert_eq!(entries[1].id, "second");
        assert_eq!(entries[1].directory, None);
    }

    #[test]
    fn parse_ws_list_tolerates_variant_casing_unknown_fields_and_preamble() {
        // Site/version variants: preamble text before any block, different
        // casing, unknown fields, and a filesystem suffix on known keys.
        let raw = "\
Workspaces on filesystem scratch:

Id: ws-a
     Workspace directory (scratch) : /scratch/ws-a
     Remaining Time       : 1 day, 2 hours
     some future field    : whatever
     available extensions : not-a-number
";
        let entries = parse_ws_list(raw);
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.id, "ws-a");
        assert_eq!(entry.directory.as_deref(), Some(Path::new("/scratch/ws-a")));
        assert_eq!(
            entry.remaining_seconds,
            Some(SECONDS_PER_DAY + 2 * SECONDS_PER_HOUR)
        );
        // Unparseable extension counts degrade to None instead of failing.
        assert_eq!(entry.available_extensions, None);
    }

    #[test]
    fn parse_ws_list_never_panics_on_multibyte_utf8_lines() {
        // Regression: the old parser sliced `trimmed[..3]`, which panics when
        // byte 3 falls inside a multi-byte character ('€' spans bytes 1-3
        // here). Such lines must be skipped as unknown noise, not panic.
        let raw = "\
a€rie: not a block
id: ok
     workspace directory  : /scratch/ok
€€: more noise
";
        let entries = parse_ws_list(raw);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "ok");
        assert_eq!(
            entries[0].directory.as_deref(),
            Some(Path::new("/scratch/ok"))
        );
    }

    #[test]
    fn parse_ws_list_tolerates_lossy_decoded_noise() {
        // from_utf8_lossy replaces invalid bytes with U+FFFD; interleaved
        // replacement-character noise must not derail block parsing.
        let raw = format!(
            "{r}{r}garbage {r}\nid: survivor\n     remaining time       : 2 days\n{r}: {r}\n",
            r = char::REPLACEMENT_CHARACTER
        );
        let entries = parse_ws_list(&raw);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "survivor");
        assert_eq!(entries[0].remaining_seconds, Some(2 * SECONDS_PER_DAY));
    }

    #[test]
    fn parse_ws_list_accepts_space_before_the_id_colon() {
        // Block detection is as tolerant as field matching: the key before
        // the first ':' is trimmed and compared case-insensitively.
        let raw = "\
Id : foo
     workspace directory  : /scratch/foo
";
        let entries = parse_ws_list(raw);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "foo");
        assert_eq!(
            entries[0].directory.as_deref(),
            Some(Path::new("/scratch/foo"))
        );
    }

    #[test]
    fn parse_remaining_time_covers_variants() {
        assert_eq!(
            parse_remaining_time_seconds("29 days 23 hours"),
            Some(29 * SECONDS_PER_DAY + 23 * SECONDS_PER_HOUR)
        );
        assert_eq!(
            parse_remaining_time_seconds("3 days"),
            Some(3 * SECONDS_PER_DAY)
        );
        assert_eq!(
            parse_remaining_time_seconds("23 hours"),
            Some(23 * SECONDS_PER_HOUR)
        );
        assert_eq!(
            parse_remaining_time_seconds("1 day, 2 hours, 5 minutes"),
            Some(SECONDS_PER_DAY + 2 * SECONDS_PER_HOUR + 5 * 60)
        );
        assert_eq!(parse_remaining_time_seconds("0 days 0 hours"), Some(0));
        assert_eq!(parse_remaining_time_seconds("42 seconds"), Some(42));
        // No recognizable duration token: callers fall back to the raw string.
        assert_eq!(parse_remaining_time_seconds("expired"), None);
        assert_eq!(parse_remaining_time_seconds(""), None);
    }

    #[test]
    fn workspace_state_round_trips_and_falls_back_on_stale_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workspace-state.toml");

        // Missing file loads as the empty default.
        let mut state = load_workspace_state(&path).expect("default state");
        assert_eq!(state, WorkspaceState::default());

        state.profiles.insert(
            "dev".to_string(),
            WorkspaceStateEntry {
                name: "ws".to_string(),
                path: PathBuf::from("/scratch/u-ws"),
                expiry_epoch: Some(1_800_000_000),
                extensions_remaining: Some(2),
                last_checked: 1_799_000_000,
            },
        );
        save_workspace_state(&path, &state).expect("save");
        let reloaded = load_workspace_state(&path).expect("reload");
        assert_eq!(reloaded, state);

        // Stale content must never brick a command: an unknown schema version
        // or an unparsable file is a regenerable cache and falls back to the
        // empty default (with a stderr warning) instead of erroring.
        fs::write(&path, "version = 999\n").expect("future version");
        assert_eq!(
            load_workspace_state(&path).expect("future version tolerated"),
            WorkspaceState::default()
        );
        fs::write(&path, "not [valid toml").expect("corrupt state");
        assert_eq!(
            load_workspace_state(&path).expect("corrupt state tolerated"),
            WorkspaceState::default()
        );
    }

    #[test]
    fn workspace_state_path_prefers_settings_location() {
        let settings = PathBuf::from("/repo/.hpc-compose/settings.toml");
        assert_eq!(
            workspace_state_path(Some(&settings), Path::new("/elsewhere")),
            PathBuf::from("/repo/.hpc-compose/workspace-state.toml")
        );
        // Without settings, fall back to the repo root/cwd convention.
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            workspace_state_path(None, dir.path()),
            dir.path().join(WORKSPACE_STATE_RELATIVE_PATH)
        );
    }

    fn record_with(
        job_id: &str,
        cache_dir: &str,
        runtime_root: Option<&str>,
    ) -> crate::job::SubmissionRecord {
        let mut value = serde_json::json!({
            "schema_version": 3,
            "backend": "slurm",
            "kind": "main",
            "job_id": job_id,
            "submitted_at": 0,
            "compose_file": "/repo/compose.yaml",
            "submit_dir": "/repo",
            "script_path": "/repo/run.sbatch",
            "cache_dir": cache_dir,
            "batch_log": "/repo/logs/x.out",
            "service_logs": {}
        });
        if let Some(root) = runtime_root {
            value["runtime_root"] = serde_json::json!(root);
        }
        serde_json::from_value(value).expect("record")
    }

    #[test]
    fn job_ids_blocking_release_matches_cache_dir_and_runtime_root_prefixes() {
        let workspace = Path::new("/scratch/u-ws");
        let records = vec![
            record_with("111", "/scratch/u-ws/hpc-compose-cache", None),
            record_with("222", "/other/cache", Some("/scratch/u-ws/runtime")),
            record_with("333", "/other/cache", None),
            // Same job id twice must dedup; sibling prefix must not match.
            record_with("111", "/scratch/u-ws/hpc-compose-cache", None),
            record_with("444", "/scratch/u-ws-other/cache", None),
        ];
        assert_eq!(
            job_ids_blocking_release(&records, workspace),
            vec!["111".to_string(), "222".to_string()]
        );
        assert!(job_ids_blocking_release(&[], workspace).is_empty());
    }
}
