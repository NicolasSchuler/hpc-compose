use std::env;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{HoldOnExit, OutputFormat, WatchMode};
use hpc_compose::cluster::{discover_cluster_profile_path, load_cluster_profile};
use hpc_compose::context::ResolvedContext;
#[cfg(test)]
use hpc_compose::job::build_submission_record_with_backend;
use hpc_compose::job::{
    RequestedWalltime, SchedulerOptions, SubmissionBackend, SubmissionKind, SubmissionRecord,
    SubmissionRecordBuildOptions, build_status_snapshot,
    build_submission_record_with_backend_and_options, build_submission_record_with_options,
    find_submission_record_in_repo, jobs_dir_for, latest_canary_record_path_for,
    latest_notebook_record_path_for, latest_record_path_for, latest_run_record_path_for,
    load_submission_record, metadata_root_for, parse_log_since_duration, pid_is_running,
    remove_submission_record, runtime_job_root_for_record, scan_job_records, state_path_for_record,
    wait_for_job_start, watch_submission, write_submission_record,
};
use hpc_compose::planner::{ImageSource, ServicePlacementMode};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, prepare_runtime_plan_with_reporter};
use hpc_compose::render::{
    LocalRenderOptions, RenderOptions, log_file_name_for_service, render_local_script_with_options,
    render_script_with_options,
};
use hpc_compose::runtime_plan::{RuntimePlan, base_image_path_for_backend};
use hpc_compose::spec::{
    MetricsCollector, MetricsConfig, ServiceFailureMode, SignalShellTarget, parse_slurm_time_limit,
};
use hpc_compose::when::{
    MonitorOptions, RealMonitorRuntime, WhenConditionSummary, WhenConditions, monitor_until_ready,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::commands::load;
use crate::output;
use crate::progress::{PrepareProgress, ProgressReporter};
use crate::watch_ui;

pub(crate) mod notebook;
pub(crate) mod notebook_promote;
mod resources;
pub(crate) use notebook::NotebookKind;
pub(crate) use resources::ResourceCliOptions;

// Command-family submodules. The launch/submit core and cross-cutting helpers
// stay in this facade for now; children import only the specific helpers they
// need, keeping their dependency boundaries visible.
mod checkpoints;
pub(crate) mod debug;
mod dev;
pub(crate) mod exec;
pub(crate) mod experiment;
pub(crate) mod experiment_bundle;
pub(crate) mod germinate;
mod inspect;
mod lifecycle;
pub(crate) mod pull;
pub(crate) mod reach;
mod remote;
pub(crate) mod rendezvous_cmd;
mod ssh_hint;
pub(crate) mod sweep;

pub(crate) use checkpoints::*;
pub(crate) use debug::*;
pub(crate) use dev::*;
pub(crate) use exec::*;
pub(crate) use experiment::*;
pub(crate) use experiment_bundle::*;
pub(crate) use germinate::*;
pub(crate) use inspect::*;
pub(crate) use lifecycle::*;
pub(crate) use pull::*;
pub(crate) use reach::*;
pub(crate) use remote::*;
pub(crate) use rendezvous_cmd::*;
pub(crate) use sweep::*;

/// Bundle of the four preparation-related flags shared across the runtime
/// launch command family (`up`, `germinate`, `test`, `dev`, `tmux`, `when`,
/// `alloc`, `run`, and the internal `launch`/prepare helpers).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PrepareFlags {
    pub keep_failed_prep: bool,
    pub skip_prepare: bool,
    pub force_rebuild: bool,
    pub no_preflight: bool,
}

/// The eight boolean toggles that drive the internal `launch` core. Bundling
/// them into a named struct keeps the constructor sites self-documenting instead
/// of an unlabeled positional-bool run.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LaunchOptions {
    pub watch: bool,
    pub watch_queue: bool,
    pub local: bool,
    pub allow_resume_changes: bool,
    pub resume_diff_only: bool,
    pub dry_run: bool,
    pub print_endpoints: bool,
    pub quiet: bool,
}

/// The eight boolean toggles of the public `up` command. `up` forwards to
/// `launch` and inverts `detach` into `launch`'s `watch`; naming the fields
/// makes that inversion explicit at the constructor site.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UpOptions {
    pub local: bool,
    pub allow_resume_changes: bool,
    pub resume_diff_only: bool,
    pub dry_run: bool,
    pub detach: bool,
    pub watch_queue: bool,
    pub print_endpoints: bool,
    pub quiet: bool,
}

/// The three boolean toggles of the `when` command.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WhenOptions {
    pub allow_resume_changes: bool,
    pub detach: bool,
    pub quiet: bool,
}

/// Per-run overrides for runtime metrics sampling, from `up --metrics-interval`
/// / `up --no-metrics`. They override the compose's `x-slurm.metrics` for this
/// invocation only (and, for `up --remote`, are forwarded to the login node).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetricsOverrides {
    /// Disable metrics sampling for this run (`--no-metrics`).
    pub disable: bool,
    /// Override the sampling interval and enable metrics (`--metrics-interval`).
    pub interval_seconds: Option<u64>,
}

impl MetricsOverrides {
    /// Validates the requested overrides (the CLI already rejects combining the
    /// two flags; this guards the interval bound).
    pub(crate) fn validate(&self) -> Result<()> {
        if self.interval_seconds == Some(0) {
            bail!("up --metrics-interval must be at least 1");
        }
        Ok(())
    }

    /// Applies the overrides to a runtime plan's metrics configuration.
    pub(crate) fn apply(&self, plan: &mut RuntimePlan) {
        if self.disable {
            if let Some(metrics) = plan.slurm.metrics.as_mut() {
                metrics.enabled = Some(false);
            }
            return;
        }
        if let Some(interval) = self.interval_seconds {
            let metrics = plan
                .slurm
                .metrics
                .get_or_insert_with(MetricsConfig::default);
            metrics.enabled = Some(true);
            metrics.interval_seconds = Some(interval);
            if metrics.collectors.is_empty() {
                metrics.collectors = vec![
                    MetricsCollector::Gpu,
                    MetricsCollector::Slurm,
                    MetricsCollector::Cpu,
                ];
            }
        }
    }
}

static DEV_SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn watch_with_fallback(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    service: Option<&str>,
    lines: usize,
    mode: WatchMode,
    hold_on_exit: HoldOnExit,
    prefs: watch_ui::WatchPrefs,
) -> Result<hpc_compose::job::WatchOutcome> {
    match mode {
        WatchMode::Line => return watch_submission(record, service, options, lines),
        WatchMode::Tui => {
            return watch_ui::run_watch_ui(record, options, service, lines, hold_on_exit, prefs)
                .context("watch UI requested with --watch-mode tui but could not be started");
        }
        WatchMode::Auto => {}
    }
    if watch_ui::can_use_watch_ui() {
        match watch_ui::run_watch_ui(record, options, service, lines, hold_on_exit, prefs) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                hpc_compose::diagnostics::warn(format!(
                    "live watch UI unavailable ({err}); falling back to line mode"
                ));
            }
        }
    }
    watch_submission(record, service, options, lines)
}

fn latest_record_path(record: &SubmissionRecord) -> PathBuf {
    match record.kind {
        SubmissionKind::Main => latest_record_path_for(&record.compose_file),
        SubmissionKind::Run => latest_run_record_path_for(&record.compose_file),
        SubmissionKind::Canary => latest_canary_record_path_for(&record.compose_file),
        SubmissionKind::Notebook => latest_notebook_record_path_for(&record.compose_file),
        SubmissionKind::SweepTrial => {
            jobs_dir_for(&record.compose_file).join(format!("{}.json", record.job_id))
        }
    }
}

fn default_run_script_path(compose_file: &Path, service_name: &str) -> PathBuf {
    let parent = compose_file.parent().unwrap_or_else(|| Path::new("."));
    let service_token = log_file_name_for_service(service_name)
        .trim_end_matches(".log")
        .to_string();
    parent.join(format!("hpc-compose-run-{service_token}.sbatch"))
}

fn default_ephemeral_run_script_path(cwd: &Path, local: bool) -> PathBuf {
    if local {
        cwd.join("hpc-compose-run.local.sh")
    } else {
        cwd.join("hpc-compose-run.sbatch")
    }
}

fn tracked_cached_artifacts(plan: &RuntimePlan) -> Vec<PathBuf> {
    let mut seen = std::collections::BTreeSet::new();
    let mut artifacts = Vec::new();
    for service in &plan.ordered_services {
        let mut candidates = vec![service.runtime_image.clone()];
        if matches!(service.source, ImageSource::Remote(_)) {
            candidates.push(base_image_path_for_backend(
                &plan.cache_dir,
                service,
                plan.runtime.backend,
            ));
        }
        for candidate in candidates {
            if candidate.starts_with(&plan.cache_dir) && seen.insert(candidate.clone()) {
                artifacts.push(candidate);
            }
        }
    }
    artifacts
}

/// Collects best-effort submit-time provenance (tool version, git state of the
/// repo containing the compose context, and per-service image refs) and, inside a
/// git working tree, snapshots the working-tree source into the content-addressed
/// store and pins its hash. Contacts no scheduler. Always `Some` on submit paths.
pub(crate) fn collect_submit_provenance(
    cwd: &Path,
    plan: &RuntimePlan,
) -> Option<hpc_compose::job::JobProvenance> {
    let repo_root = hpc_compose::context::repo_root_or_cwd(cwd);
    let provenance = hpc_compose::job::collect_provenance(
        &repo_root,
        env!("CARGO_PKG_VERSION"),
        image_refs_from_plan(plan),
    );
    Some(attach_submit_source_snapshot(
        provenance,
        &repo_root,
        &plan.cache_dir,
    ))
}

/// Pins the working-tree source snapshot's content hash into `provenance` when it
/// describes a git working tree (mirroring `provenance.git`: a non-git tree has
/// no reproducible source identity to pin). Reuses the content-addressed store
/// ([`hpc_compose::cache::source::stage_source`]); identical source dedups, so a
/// sweep's trials share one entry. Best-effort: a staging failure is reported and
/// leaves the hash unset rather than failing the submit.
pub(crate) fn attach_submit_source_snapshot(
    mut provenance: hpc_compose::job::JobProvenance,
    repo_root: &Path,
    cache_dir: &Path,
) -> hpc_compose::job::JobProvenance {
    if provenance.git.is_none() {
        return provenance;
    }
    match hpc_compose::cache::source::stage_source(repo_root, cache_dir) {
        Ok(snapshot) => provenance.source_content_hash = Some(snapshot.content_hash),
        Err(err) => hpc_compose::diagnostics::warn(format!(
            "failed to snapshot source for provenance: {err:#}"
        )),
    }
    provenance
}

fn image_refs_from_plan(plan: &RuntimePlan) -> std::collections::BTreeMap<String, String> {
    plan.ordered_services
        .iter()
        .map(|service| (service.name.clone(), image_source_label(&service.source)))
        .collect()
}

/// Stringifies an image source as launched (remote ref or local artifact path).
fn image_source_label(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => path.display().to_string(),
        ImageSource::Remote(remote) => remote.clone(),
        ImageSource::Host => "host".to_string(),
    }
}

#[derive(Debug)]
struct UpInvocationLock {
    path: Option<PathBuf>,
}

struct UpInvocationReclaimLock {
    path: PathBuf,
}

impl Drop for UpInvocationLock {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_file(path);
        }
    }
}

impl Drop for UpInvocationReclaimLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Computes the deterministic lock-file path for a compose file's `up`
/// invocation lock. The name is the SHA-256 of the canonical compose path so
/// distinct specs never collide on the same lock.
fn up_invocation_lock_path(compose_file: &Path) -> PathBuf {
    let canonical = fs::canonicalize(compose_file).unwrap_or_else(|_| compose_file.to_path_buf());
    let mut digest = Sha256::new();
    digest.update(canonical.to_string_lossy().as_bytes());
    let hash = hex::encode(digest.finalize());
    metadata_root_for(compose_file)
        .join("locks")
        .join(format!("{hash}.up.lock"))
}

fn up_invocation_reclaim_lock_path(lock_path: &Path) -> PathBuf {
    let file_name = lock_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "up.lock".to_string());
    lock_path.with_file_name(format!("{file_name}.reclaim"))
}

fn normalize_up_invocation_host(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('.');
    if trimmed.is_empty()
        || trimmed == "127.0.0.1"
        || trimmed == "::1"
        || trimmed.eq_ignore_ascii_case("localhost")
    {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn current_up_invocation_host() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buffer = [0u8; 256];
        let rc =
            unsafe { libc::gethostname(buffer.as_mut_ptr().cast::<libc::c_char>(), buffer.len()) };
        if rc == 0 {
            let len = buffer
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(buffer.len());
            if let Ok(name) = std::str::from_utf8(&buffer[..len])
                && let Some(host) = normalize_up_invocation_host(name)
            {
                return Some(host);
            }
        }
    }

    env::var("HOSTNAME")
        .ok()
        .and_then(|name| normalize_up_invocation_host(&name))
}

/// Returns `true` when an existing lock file's recorded process is provably no
/// longer running on this host, meaning a previous local `up` crashed or was
/// interrupted before its [`Drop`] could remove the lock. Returns `false` when
/// the holder is alive, belongs to a different host, or liveness cannot be
/// determined (missing/unparseable pid or host), so an undecidable lock is never
/// reclaimed out from under a live process.
fn up_invocation_lock_is_stale(existing: &str) -> bool {
    let Some(current_host) = current_up_invocation_host() else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(existing) else {
        return false;
    };
    let Some(lock_host) = value
        .get("host")
        .and_then(serde_json::Value::as_str)
        .and_then(normalize_up_invocation_host)
    else {
        return false;
    };
    if lock_host != current_host {
        return false;
    }
    value
        .get("pid")
        .and_then(serde_json::Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
        .is_some_and(|pid| !pid_is_running(pid))
}

fn try_acquire_up_invocation_reclaim_lock(
    lock_path: &Path,
) -> Result<Option<UpInvocationReclaimLock>> {
    let reclaim_path = up_invocation_reclaim_lock_path(lock_path);
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&reclaim_path)
    {
        Ok(_) => Ok(Some(UpInvocationReclaimLock { path: reclaim_path })),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("failed to create {}", reclaim_path.display()))
        }
    }
}

fn reclaim_stale_up_invocation_lock(lock_path: &Path, expected: &str) -> Result<bool> {
    let Some(_guard) = try_acquire_up_invocation_reclaim_lock(lock_path)? else {
        return Ok(false);
    };
    let current = match fs::read_to_string(lock_path) {
        Ok(current) => current,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => return Ok(false),
    };
    if current != expected || !up_invocation_lock_is_stale(&current) {
        return Ok(false);
    }
    fs::remove_file(lock_path).with_context(|| {
        format!(
            "failed to remove stale up invocation lock {}",
            lock_path.display()
        )
    })?;
    Ok(true)
}

fn acquire_up_invocation_lock(compose_file: &Path) -> Result<UpInvocationLock> {
    let canonical = fs::canonicalize(compose_file).unwrap_or_else(|_| compose_file.to_path_buf());
    let lock_dir = metadata_root_for(compose_file).join("locks");
    if let Err(err) = fs::create_dir_all(&lock_dir) {
        hpc_compose::diagnostics::warn(format!(
            "concurrent up protection unavailable because {} could not be created: {err}",
            lock_dir.display()
        ));
        return Ok(UpInvocationLock { path: None });
    }
    let path = up_invocation_lock_path(compose_file);
    let command = env::args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let content = serde_json::json!({
        "pid": std::process::id(),
        "host": current_up_invocation_host(),
        "command": command,
        "compose_path": canonical,
        "created_at_unix": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    });
    // Try once; if a lock already exists for a dead process, reclaim it and try
    // again exactly once. A second `AlreadyExists` means a live competitor won
    // the race after we reclaimed, so we bail rather than loop.
    for attempt in 0..2 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(
                    file,
                    "{}",
                    crate::output::to_pretty_json(&content)
                        .context("failed to serialize lock file")?
                )
                .with_context(|| format!("failed to write {}", path.display()))?;
                return Ok(UpInvocationLock { path: Some(path) });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing =
                    fs::read_to_string(&path).unwrap_or_else(|_| "<unreadable>".to_string());
                if attempt == 0 && up_invocation_lock_is_stale(&existing) {
                    // Stale lock from a crashed/interrupted local `up`: remove
                    // it only if we win the reclaim sidecar and the file still
                    // contains the same owner record we inspected above.
                    if reclaim_stale_up_invocation_lock(&path, &existing)? {
                        continue;
                    }
                }
                bail!(
                    "another hpc-compose up appears to be running for {}; lock file: {}; existing lock: {}; if this process is gone, remove the lock file and retry",
                    compose_file.display(),
                    path.display(),
                    existing.trim()
                );
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                hpc_compose::diagnostics::warn(format!(
                    "concurrent up protection unavailable because {} could not be created: {err}",
                    path.display()
                ));
                return Ok(UpInvocationLock { path: None });
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to create {}", path.display()));
            }
        }
    }
    unreachable!("up invocation lock acquisition retries at most once");
}

fn requested_walltime(plan: &RuntimePlan) -> Option<RequestedWalltime> {
    let raw = plan.slurm.time.as_deref()?;
    let seconds = parse_slurm_time_limit(raw).ok()?;
    Some(RequestedWalltime {
        original: raw.to_string(),
        seconds,
    })
}

fn sbatch_cli_args(plan: &RuntimePlan) -> Vec<String> {
    plan.slurm
        .dependency_cli_value()
        .map(|dependency| vec![format!("--dependency={dependency}")])
        .unwrap_or_default()
}

fn ensure_batch_submission_supported(plan: &RuntimePlan, watch: bool, local: bool) -> Result<()> {
    if local && plan.slurm.array.is_some() {
        bail!("--local does not support x-slurm.array");
    }
    if local && plan.slurm.has_scheduler_dependency() {
        bail!("--local does not support Slurm job dependencies");
    }
    if watch && plan.slurm.array.is_some() {
        bail!(
            "x-slurm.array requires --detach because live watch/log fan-out is not array-aware yet"
        );
    }
    Ok(())
}

fn active_allocation_job_id() -> Option<String> {
    let allocation = env::var("HPC_COMPOSE_ALLOCATION").ok()?;
    if allocation != "1" {
        return None;
    }
    env::var("SLURM_JOB_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn sh_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn allocation_bootstrap_script(
    context: &ResolvedContext,
    runtime_plan: &RuntimePlan,
    submit_dir: &Path,
) -> String {
    let compose_file = context.compose_file.value.display().to_string();
    let cache_dir = runtime_plan.cache_dir.display().to_string();
    let project_dir = context
        .compose_file
        .value
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .display()
        .to_string();
    let scontrol = context.binaries.scontrol.value.as_str();
    format!(
        r#"set -euo pipefail
submit_dir={submit_dir}
cd "$submit_dir"
job_id="${{SLURM_JOB_ID:?SLURM_JOB_ID is required inside salloc}}"
allocation_dir="$submit_dir/.hpc-compose/$job_id/allocation"
mkdir -p "$allocation_dir"
nodelist_file="$allocation_dir/nodelist"
raw_nodelist="${{SLURM_JOB_NODELIST:-${{SLURM_NODELIST:-}}}}"
if [ -n "$raw_nodelist" ] && {scontrol} show hostnames "$raw_nodelist" > "$nodelist_file" 2>/dev/null; then
  :
elif [ -n "$raw_nodelist" ]; then
  printf '%s\n' "$raw_nodelist" > "$nodelist_file"
else
  : > "$nodelist_file"
fi
primary_node="$(head -n 1 "$nodelist_file" 2>/dev/null || true)"
node_count="$(wc -l < "$nodelist_file" 2>/dev/null | tr -d '[:space:]' || true)"
if [ -z "$node_count" ] || [ "$node_count" = "0" ]; then
  node_count="${{SLURM_JOB_NUM_NODES:-${{SLURM_NNODES:-1}}}}"
fi
if [ -z "$primary_node" ]; then
  primary_node="${{HOSTNAME:-}}"
fi
export HPC_COMPOSE_ALLOCATION=1
export HPC_COMPOSE_COMPOSE_FILE={compose_file}
export HPC_COMPOSE_CACHE_DIR={cache_dir}
export HPC_COMPOSE_PROJECT_DIR={project_dir}
export HPC_COMPOSE_RUNTIME_BACKEND={runtime_backend}
export HPC_COMPOSE_PRIMARY_NODE="$primary_node"
export HPC_COMPOSE_NODE_COUNT="$node_count"
export HPC_COMPOSE_NODELIST="$raw_nodelist"
export HPC_COMPOSE_NODELIST_FILE="$nodelist_file"
printf 'hpc-compose allocation %s ready on %s node(s)\n' "$job_id" "$node_count"
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
exec "${{SHELL:-/bin/bash}}" -l
"#,
        submit_dir = sh_quote(&submit_dir.display().to_string()),
        scontrol = sh_quote(scontrol),
        compose_file = sh_quote(&compose_file),
        cache_dir = sh_quote(&cache_dir),
        project_dir = sh_quote(&project_dir),
        runtime_backend = sh_quote(runtime_plan.runtime.backend.as_str()),
    )
}

fn strip_sbatch_directives(script: &str) -> String {
    script
        .lines()
        .filter(|line| !line.trim_start().starts_with("#SBATCH"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn diff_lines(previous: &str, current: &str) -> Option<String> {
    if previous == current {
        return None;
    }
    let previous_lines = previous.lines().collect::<Vec<_>>();
    let current_lines = current.lines().collect::<Vec<_>>();
    let max_len = previous_lines.len().max(current_lines.len());
    let mut out = String::from("--- previous\n+++ current\n");
    for index in 0..max_len {
        match (previous_lines.get(index), current_lines.get(index)) {
            (Some(left), Some(right)) if left == right => {
                out.push(' ');
                out.push_str(left);
                out.push('\n');
            }
            (Some(left), Some(right)) => {
                out.push('-');
                out.push_str(left);
                out.push('\n');
                out.push('+');
                out.push_str(right);
                out.push('\n');
            }
            (Some(left), None) => {
                out.push('-');
                out.push_str(left);
                out.push('\n');
            }
            (None, Some(right)) => {
                out.push('+');
                out.push_str(right);
                out.push('\n');
            }
            (None, None) => {}
        }
    }
    Some(out)
}

fn maybe_check_resume_diff(
    compose_file: &Path,
    resume_enabled: bool,
    effective_config_yaml: &str,
    allow_resume_changes: bool,
    resume_diff_only: bool,
    output_format: OutputFormat,
) -> Result<bool> {
    if resume_diff_only && output_format == OutputFormat::Json {
        bail!("--resume-diff-only does not support --format json");
    }

    if !resume_enabled {
        if resume_diff_only {
            println!("resume diff: x-slurm.resume is not configured");
            return Ok(true);
        }
        return Ok(false);
    }

    let previous = match load_submission_record(compose_file, None) {
        Ok(record) if record.kind == SubmissionKind::Main => record,
        Ok(_) => return Ok(false),
        Err(_) => {
            if resume_diff_only {
                println!("resume diff: no prior tracked main submission exists");
                return Ok(true);
            }
            return Ok(false);
        }
    };
    let Some(previous_yaml) = previous.config_snapshot_yaml.as_deref() else {
        let note = "resume diff unavailable because the previous tracked submission has no config snapshot";
        if resume_diff_only {
            println!("{note}");
            return Ok(true);
        }
        hpc_compose::diagnostics::warn(note);
        return Ok(false);
    };
    let Some(diff) = diff_lines(previous_yaml, effective_config_yaml) else {
        if resume_diff_only {
            println!("resume diff: no changes");
            return Ok(true);
        }
        return Ok(false);
    };

    if output_format == OutputFormat::Text {
        println!("{diff}");
    }
    if resume_diff_only {
        return Ok(true);
    }
    if !allow_resume_changes {
        bail!("resume config drift detected; rerun with --allow-resume-changes to run anyway");
    }
    Ok(false)
}

fn resolve_tracked_record(
    context: &ResolvedContext,
    job_id: Option<&str>,
) -> Result<Option<SubmissionRecord>> {
    match job_id {
        Some(job_id) => {
            let direct_record_path =
                jobs_dir_for(&context.compose_file.value).join(format!("{job_id}.json"));
            if direct_record_path.exists() {
                return load_submission_record(&context.compose_file.value, Some(job_id)).map(Some);
            }
            match find_submission_record_in_repo(&context.cwd, job_id) {
                Ok(record) => Ok(Some(record)),
                Err(err) if is_missing_tracked_record_error(&err) => Ok(None),
                Err(err) => Err(err),
            }
        }
        None => {
            let latest = scan_job_records(&context.compose_file.value)?
                .into_iter()
                .max_by(|left, right| {
                    left.submitted_at
                        .cmp(&right.submitted_at)
                        .then_with(|| left.job_id.cmp(&right.job_id))
                });
            match latest {
                Some(record) => Ok(Some(record)),
                None => Ok(Some(load_submission_record(
                    &context.compose_file.value,
                    None,
                )?)),
            }
        }
    }
}

fn is_missing_tracked_record_error(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("no tracked submission metadata exists")
}

fn load_discovered_cluster_profile(
    context: &ResolvedContext,
) -> Result<Option<hpc_compose::cluster::ClusterProfile>> {
    let start = context
        .compose_file
        .value
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let Some(path) = discover_cluster_profile_path(start) else {
        return Ok(None);
    };
    Ok(Some(load_cluster_profile(&path)?))
}

fn purge_cached_artifacts(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for path in paths {
        if !path.exists() {
            continue;
        }
        if path.is_dir() {
            fs::remove_dir_all(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        } else {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
        removed.push(path.clone());
    }
    Ok(removed)
}

fn cached_artifacts_for_teardown(record: Option<&SubmissionRecord>) -> Result<Vec<PathBuf>> {
    let record = record.context(
        "--purge-cache requires tracked submission metadata with cached artifact snapshots",
    )?;
    if record.cached_artifacts.is_empty() {
        bail!(
            "tracked submission metadata for job '{}' does not contain cached artifact snapshots; refusing --purge-cache",
            record.job_id
        );
    }
    Ok(record.cached_artifacts.clone())
}

fn generate_local_job_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("local-{timestamp}-{}", std::process::id())
}

fn ensure_local_submit_supported(plan: &RuntimePlan) -> Result<()> {
    ensure_batch_submission_supported(plan, false, true)?;
    ensure_local_host_supported()?;
    ensure_local_plan_supported(plan)
}

fn ensure_local_host_supported() -> Result<()> {
    if env::consts::OS == "linux" {
        Ok(())
    } else {
        bail!("--local is only supported on Linux hosts");
    }
}

fn ensure_local_plan_supported(plan: &RuntimePlan) -> Result<()> {
    if !matches!(
        plan.runtime.backend,
        hpc_compose::spec::RuntimeBackend::Pyxis | hpc_compose::spec::RuntimeBackend::Apptainer
    ) {
        bail!(
            "--local currently supports only runtime.backend=pyxis or runtime.backend=apptainer; got runtime.backend={}",
            plan.runtime.backend.as_str()
        );
    }
    for service in &plan.ordered_services {
        if service.placement.mode != ServicePlacementMode::PrimaryNode {
            bail!(
                "--local does not support distributed or partitioned placement; service '{}' uses {} placement",
                service.name,
                local_placement_mode_label(service.placement.mode)
            );
        }
        if !service.slurm.extra_srun_args.is_empty() {
            bail!(
                "--local does not support services.<name>.x-slurm.extra_srun_args; service '{}' sets: {}",
                service.name,
                service.slurm.extra_srun_args.join(" ")
            );
        }
        if service.slurm.mpi.is_some() {
            bail!(
                "--local does not support x-slurm.mpi; service '{}' requests MPI launch integration",
                service.name
            );
        }
    }
    if plan.slurm.allocation_nodes() > 1 {
        bail!(
            "--local currently supports only single-host specs; x-slurm.nodes resolved to {}",
            plan.slurm.allocation_nodes()
        );
    }
    Ok(())
}

fn warn_local_ignored_scheduler_settings(plan: &RuntimePlan) {
    if plan
        .slurm
        .submit_args
        .iter()
        .any(|arg| arg.contains("reservation"))
    {
        hpc_compose::diagnostics::warn("--local ignores reservation-related x-slurm.submit_args");
    }
    if plan.slurm.reservation.is_some() {
        hpc_compose::diagnostics::warn("--local ignores x-slurm.reservation");
    }
    if plan.slurm.licenses.is_some() {
        hpc_compose::diagnostics::warn("--local ignores x-slurm.licenses");
    }
    if plan.slurm.error.is_some() {
        hpc_compose::diagnostics::warn(
            "--local ignores x-slurm.error and writes batch stderr into the local batch log",
        );
    }
}

fn local_render_options(
    context: &ResolvedContext,
    runtime_plan: &RuntimePlan,
    dev_reload: bool,
) -> LocalRenderOptions {
    LocalRenderOptions {
        dev_reload,
        apptainer_bin: context.binaries.apptainer.value.clone(),
        singularity_bin: context.binaries.singularity.value.clone(),
        huggingface_cli_bin: context.huggingface_cli_bin.clone(),
        runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
            &context.cwd,
            runtime_plan.slurm.runtime_root.as_deref(),
        )),
    }
}

fn local_failure_policy_mode_label(mode: ServiceFailureMode) -> &'static str {
    match mode {
        ServiceFailureMode::FailJob => "fail_job",
        ServiceFailureMode::Ignore => "ignore",
        ServiceFailureMode::RestartOnFailure => "restart_on_failure",
    }
}

fn local_placement_mode_label(mode: ServicePlacementMode) -> &'static str {
    match mode {
        ServicePlacementMode::PrimaryNode => "primary_node",
        ServicePlacementMode::Partitioned => "partitioned",
        ServicePlacementMode::Distributed => "distributed",
    }
}

fn local_service_step_name(value: &str) -> String {
    let mut token = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            token.push(byte as char);
        } else {
            token.push_str(&format!("_x{byte:02x}_"));
        }
    }
    format!("hpc-compose:{token}")
}

fn write_local_runtime_state_stub(
    record: &SubmissionRecord,
    plan: &RuntimePlan,
    supervisor_pid: u32,
) -> Result<()> {
    let job_root = runtime_job_root_for_record(record);
    let log_dir = crate::tracked_paths::latest_logs_dir(&job_root);
    fs::create_dir_all(&log_dir)
        .with_context(|| format!("failed to create {}", log_dir.display()))?;
    let state_path = state_path_for_record(record);
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if state_path.exists() {
        return Ok(());
    }

    let services = plan
        .ordered_services
        .iter()
        .enumerate()
        .map(|(index, service)| {
            serde_json::json!({
                "service_name": service.name,
                "step_name": local_service_step_name(&service.name),
                "log_path": record
                    .service_logs
                    .get(&service.name)
                    .cloned()
                    .unwrap_or_else(|| log_dir.join(log_file_name_for_service(&service.name))),
                "launch_index": index,
                "launcher_pid": serde_json::Value::Null,
                "healthy": false,
                "completed_successfully": false,
                "readiness_configured": service.readiness.is_some(),
                "failure_policy_mode": local_failure_policy_mode_label(service.failure_policy.mode),
                "restart_count": 0,
                "max_restarts": service.failure_policy.max_restarts,
                "window_seconds": service.failure_policy.window_seconds,
                "max_restarts_in_window": service.failure_policy.max_restarts_in_window,
                "last_exit_code": serde_json::Value::Null,
                "placement_mode": local_placement_mode_label(service.placement.mode),
                "nodes": service.placement.nodes,
                "ntasks": service.placement.ntasks,
                "ntasks_per_node": service.placement.ntasks_per_node,
                "nodelist": "127.0.0.1",
            })
        })
        .collect::<Vec<_>>();

    let state = serde_json::json!({
        "backend": SubmissionBackend::Local,
        "job_status": "RUNNING",
        "job_exit_code": serde_json::Value::Null,
        "supervisor_pid": supervisor_pid,
        "attempt": serde_json::Value::Null,
        "is_resume": serde_json::Value::Null,
        "resume_dir": serde_json::Value::Null,
        "services": services,
    });
    let serialized =
        serde_json::to_vec_pretty(&state).context("failed to serialize local runtime state")?;
    crate::secure_io::write_atomic(&state_path, &serialized, true)
        .with_context(|| format!("failed to write {}", state_path.display()))
}

fn kill_pid(pid: u32) -> Result<()> {
    if kill_pid_if_running(pid)? {
        Ok(())
    } else {
        bail!("failed to signal pid {pid}: No such process")
    }
}

fn kill_pid_if_running(pid: u32) -> Result<bool> {
    #[cfg(unix)]
    {
        if pid == 0 || pid > i32::MAX as u32 {
            bail!("failed to signal pid {pid}");
        }

        // Use libc directly so invalid test PIDs cannot be reinterpreted by `/bin/kill`.
        let status = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if status == 0 {
            return Ok(true);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }

        let detail = err.to_string();
        if detail.is_empty() {
            bail!("failed to signal pid {pid}");
        }
        bail!("failed to signal pid {pid}: {detail}");
    }

    #[cfg(not(unix))]
    {
        let output = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .output()
            .context("failed to execute 'kill'")?;
        if output.status.success() {
            return Ok(true);
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.contains("No such process") {
            return Ok(false);
        }
        if detail.is_empty() {
            bail!("failed to signal pid {pid}");
        }
        bail!("failed to signal pid {pid}: {detail}");
    }
}

fn rollback_local_tracking(record: &SubmissionRecord, supervisor_pid: Option<u32>) {
    if let Some(pid) = supervisor_pid
        && let Err(err) = kill_pid(pid)
    {
        hpc_compose::diagnostics::warn(format!(
            "failed to stop local supervisor {pid} during rollback: {err}"
        ));
    }
    if let Err(err) = remove_submission_record(record) {
        hpc_compose::diagnostics::warn(format!(
            "failed to roll back tracked metadata for local job {}: {err}",
            record.job_id
        ));
    }
}

fn spawn_local_supervisor(submit_dir: &Path, script_path: &Path, batch_log: &Path) -> Result<u32> {
    if let Some(parent) = batch_log.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let batch_log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(batch_log)
        .with_context(|| format!("failed to open {}", batch_log.display()))?;
    let stderr_file = batch_log_file
        .try_clone()
        .with_context(|| format!("failed to clone {}", batch_log.display()))?;
    let child = Command::new("bash")
        .arg(script_path)
        .current_dir(submit_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(batch_log_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .with_context(|| {
            format!(
                "failed to launch local supervisor '{}'",
                script_path.display()
            )
        })?;
    Ok(child.id())
}

fn print_local_launch_details(record: &SubmissionRecord, plan: &RuntimePlan, script_path: &Path) {
    println!("launched local job: {}", record.job_id);
    println!("rendered script: {}", script_path.display());
    println!("cache dir: {}", plan.cache_dir.display());
    println!("batch log: {}", record.batch_log.display());
    for service in &plan.ordered_services {
        if let Some(path) = record.service_logs.get(&service.name) {
            println!("log  service '{}': {}", service.name, path.display());
        }
    }
}

fn read_local_supervisor_pid(record: &SubmissionRecord) -> Result<Option<u32>> {
    let state_path = state_path_for_record(record);
    let Ok(raw) = fs::read_to_string(&state_path) else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", state_path.display()))?;
    Ok(value
        .get("supervisor_pid")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok()))
}

struct PreparedLocalLaunch {
    file: PathBuf,
    submit_dir: PathBuf,
    script_path: PathBuf,
    runtime_plan: RuntimePlan,
    record_options: SubmissionRecordBuildOptions,
    output_format: OutputFormat,
    local_job_id: String,
}

struct LocalLaunchOutcome {
    record: SubmissionRecord,
    submit_output: output::SubmitOutput,
}

fn prepare_local_launch<F>(
    context: &ResolvedContext,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    output_format: OutputFormat,
    quiet: bool,
    dev_reload: bool,
    precheck: F,
) -> Result<PreparedLocalLaunch>
where
    F: FnOnce(&RuntimePlan) -> Result<()>,
{
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let file = context.compose_file.value.clone();
    let effective_config =
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml =
        output::effective_config_yaml(&effective_config, &context.secret_values())?;
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    precheck(&runtime_plan)?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    ensure_local_submit_supported(&runtime_plan)?;
    warn_local_ignored_scheduler_settings(&runtime_plan);
    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Main,
        service_name: None,
        command_override: None,
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: Some(effective_config_yaml),
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
    };

    if !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: false,
                        skip_prepare,
                        fs_probes: false,
                        cluster_profile: None,
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            return Err(crate::exit::EnvironmentError::new(
                "preflight failed; fix the reported errors before local launch",
            )
            .into());
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let local_job_id = generate_local_job_id();
    let script = progress.run_result("Rendering local launcher script", || {
        render_local_script_with_options(
            &runtime_plan,
            &local_job_id,
            &context.binaries.enroot.value,
            &local_render_options(context, &runtime_plan, dev_reload),
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| output::default_local_script_path(&file));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    Ok(PreparedLocalLaunch {
        file,
        submit_dir,
        script_path,
        runtime_plan,
        record_options,
        output_format,
        local_job_id,
    })
}

fn start_prepared_local_launch(prepared: &PreparedLocalLaunch) -> Result<LocalLaunchOutcome> {
    let record = build_submission_record_with_backend_and_options(
        &prepared.file,
        &prepared.submit_dir,
        &prepared.script_path,
        &prepared.runtime_plan,
        &prepared.local_job_id,
        SubmissionBackend::Local,
        &prepared.record_options,
    )?;
    write_submission_record(&record)
        .context("failed to persist tracking metadata for local launch")?;
    let supervisor_pid = match spawn_local_supervisor(
        &prepared.submit_dir,
        &prepared.script_path,
        &record.batch_log,
    ) {
        Ok(pid) => pid,
        Err(err) => {
            rollback_local_tracking(&record, None);
            return Err(err);
        }
    };
    if let Err(err) =
        write_local_runtime_state_stub(&record, &prepared.runtime_plan, supervisor_pid)
    {
        rollback_local_tracking(&record, Some(supervisor_pid));
        return Err(err);
    }

    let submit_output = output::SubmitOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        backend: SubmissionBackend::Local,
        compose_file: prepared.file.clone(),
        script_path: prepared.script_path.clone(),
        cache_dir: prepared.runtime_plan.cache_dir.clone(),
        dry_run: false,
        launched: true,
        submitted: false,
        sbatch_stdout: None,
        job_id: Some(record.job_id.clone()),
        tracking_persisted: true,
        tracked_metadata_path: Some(latest_record_path(&record)),
        endpoints: Vec::new(),
        next_commands: Vec::new(),
    };
    Ok(LocalLaunchOutcome {
        record,
        submit_output,
    })
}

fn print_local_launch_outcome(
    prepared: &PreparedLocalLaunch,
    outcome: &LocalLaunchOutcome,
) -> Result<()> {
    match prepared.output_format {
        OutputFormat::Text => {
            print_local_launch_details(
                &outcome.record,
                &prepared.runtime_plan,
                &prepared.script_path,
            );
            output::print_submit_summary_box(
                &prepared.runtime_plan,
                &outcome.record.job_id,
                &prepared.script_path,
                Some(&latest_record_path(&outcome.record)),
            );
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&outcome.submit_output)
                    .context("failed to serialize local launch output")?
            );
        }
    }
    Ok(())
}

struct PreparedSlurmSubmission {
    file: PathBuf,
    submit_dir: PathBuf,
    script_path: PathBuf,
    runtime_plan: RuntimePlan,
    record_options: SubmissionRecordBuildOptions,
    output_format: OutputFormat,
}

struct SlurmSubmitOutcome {
    stdout: String,
    tracked_submission: Option<(SubmissionRecord, bool)>,
    submit_output: output::SubmitOutput,
}

/// Pre-creates the default batch-log directory host-side before sbatch. Slurm
/// opens `--output` before the script body runs, so when the renderer emits the
/// default `<runtime_root>/logs/hpc-compose-%j.out` (i.e. the user did not pin
/// `x-slurm.output`), its job-id-free parent must already exist. User-pinned
/// outputs may point anywhere and are the user's responsibility.
pub(super) fn ensure_default_batch_log_dir(submit_dir: &Path, plan: &RuntimePlan) -> Result<()> {
    if plan.slurm.output.is_some() {
        return Ok(());
    }
    let logs_dir =
        crate::tracked_paths::resolve_runtime_root(submit_dir, plan.slurm.runtime_root.as_deref())
            .join(crate::tracked_paths::LOGS_DIR_NAME);
    fs::create_dir_all(&logs_dir)
        .with_context(|| format!("failed to create {}", logs_dir.display()))
}

/// Turns a raw `sbatch` rejection into an actionable message. Slurm's account /
/// partition / QOS errors ("Invalid account or account/partition combination",
/// "Invalid qos specification", …) are cryptic and cluster-specific, so append
/// the discovery commands that reveal what the user is actually allowed to use.
/// hpc-compose deliberately does not administer Slurm associations, so this is a
/// hint rather than a preflight gate.
pub(crate) fn enrich_sbatch_failure(stderr: &str) -> String {
    let stderr = stderr.trim();
    let lower = stderr.to_ascii_lowercase();
    let association_issue = lower.contains("invalid account")
        || lower.contains("account/partition")
        || lower.contains("invalid qos")
        || lower.contains("invalid partition");
    if association_issue {
        format!(
            "{stderr}\n  hint: the account, partition, or QOS in x-slurm is not a valid \
             combination for you on this cluster. List what you are allowed to use:\n    \
             sacctmgr -nP show assoc user=$USER format=Account,Partition,QOS\n    sshare -U\n    \
             sinfo -s\n  then set x-slurm.account / x-slurm.partition / x-slurm.qos (or your \
             site's settings) to a valid combination."
        )
    } else {
        stderr.to_string()
    }
}

fn submit_prepared_slurm_submission(
    context: &ResolvedContext,
    prepared: &PreparedSlurmSubmission,
    progress: &ProgressReporter,
) -> Result<SlurmSubmitOutcome> {
    ensure_default_batch_log_dir(&prepared.submit_dir, &prepared.runtime_plan)?;
    let output_result = progress.run_result("Submitting job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
            .args(sbatch_cli_args(&prepared.runtime_plan))
            .arg(&prepared.script_path)
            .output()
            .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
    })?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            enrich_sbatch_failure(&String::from_utf8_lossy(&output_result.stderr))
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout)
        .trim_end()
        .to_string();
    let tracked_submission = if let Some(job_id) = output::extract_job_id(&stdout) {
        let record = build_submission_record_with_options(
            &prepared.file,
            &prepared.submit_dir,
            &prepared.script_path,
            &prepared.runtime_plan,
            job_id,
            &prepared.record_options,
        )?;
        let persisted = match write_submission_record(&record) {
            Ok(()) => true,
            Err(err) => {
                hpc_compose::diagnostics::warn(format!(
                    "job submitted, but failed to write tracking metadata: {err}"
                ));
                false
            }
        };
        Some((record, persisted))
    } else {
        None
    };
    let tracked_metadata_path = tracked_submission
        .as_ref()
        .and_then(|(record, persisted)| persisted.then(|| latest_record_path(record)));
    let submit_output = output::SubmitOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        backend: SubmissionBackend::Slurm,
        compose_file: prepared.file.clone(),
        script_path: prepared.script_path.clone(),
        cache_dir: prepared.runtime_plan.cache_dir.clone(),
        dry_run: false,
        launched: false,
        submitted: true,
        sbatch_stdout: Some(stdout.clone()),
        job_id: tracked_submission
            .as_ref()
            .map(|(record, _)| record.job_id.clone()),
        tracking_persisted: tracked_submission
            .as_ref()
            .is_some_and(|(_, persisted)| *persisted),
        tracked_metadata_path,
        endpoints: Vec::new(),
        next_commands: Vec::new(),
    };
    Ok(SlurmSubmitOutcome {
        stdout,
        tracked_submission,
        submit_output,
    })
}

fn print_slurm_submit_outcome(
    prepared: &PreparedSlurmSubmission,
    outcome: &SlurmSubmitOutcome,
) -> Result<()> {
    match prepared.output_format {
        OutputFormat::Text => {
            if !outcome.stdout.is_empty() {
                println!("{}", outcome.stdout);
            }
            output::print_submit_details(
                &prepared.runtime_plan,
                &prepared.script_path,
                &outcome.stdout,
            )?;
            if let Some((record, persisted)) = outcome.tracked_submission.as_ref() {
                if *persisted {
                    let meta_path = latest_record_path(record);
                    output::print_submit_summary_box(
                        &prepared.runtime_plan,
                        &record.job_id,
                        &prepared.script_path,
                        Some(&meta_path),
                    );
                } else {
                    println!(
                        "note: tracking metadata could not be written, so later status/logs commands will not auto-discover this submission"
                    );
                }
            } else {
                println!(
                    "note: sbatch output did not include a numeric Slurm job id, so status/logs/watch are not trackable for this submission"
                );
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&outcome.submit_output)
                    .context("failed to serialize up output")?
            );
        }
    }
    Ok(())
}

fn maybe_watch_slurm_submission(
    context: &ResolvedContext,
    outcome: &SlurmSubmitOutcome,
    watch: bool,
    watch_queue: bool,
    queue_warn_after_seconds: Option<u64>,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
) -> Result<()> {
    if !watch {
        return Ok(());
    }
    let Some((record, _)) = outcome.tracked_submission.as_ref() else {
        println!("note: skipping watch because the submission is not trackable");
        return Ok(());
    };
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    if watch_queue {
        let _ = wait_for_job_start(record, &scheduler_options, queue_warn_after_seconds)?;
    }
    output::finish_watch(
        record,
        watch_with_fallback(
            record,
            &scheduler_options,
            None,
            100,
            watch_mode,
            hold_on_exit,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_slurm_submission<F>(
    context: &ResolvedContext,
    script_out: Option<PathBuf>,
    time_override: Option<String>,
    flags: PrepareFlags,
    watch: bool,
    allow_resume_changes: bool,
    output_format: OutputFormat,
    quiet: bool,
    precheck: F,
) -> Result<PreparedSlurmSubmission>
where
    F: FnOnce(&RuntimePlan) -> Result<()>,
{
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let file = context.compose_file.value.clone();
    let effective_config =
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml =
        output::effective_config_yaml(&effective_config, &context.secret_values())?;
    let mut runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if let Some(time) = time_override {
        runtime_plan.slurm.time = Some(time);
    }
    precheck(&runtime_plan)?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Main,
        service_name: None,
        command_override: None,
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: Some(effective_config_yaml.clone()),
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
    };

    if maybe_check_resume_diff(
        &file,
        runtime_plan.slurm.resume_dir().is_some(),
        &effective_config_yaml,
        allow_resume_changes,
        false,
        output_format,
    )? {
        bail!("resume diff requested unexpectedly during conditional submission");
    }
    ensure_batch_submission_supported(&runtime_plan, watch, false)?;

    let cluster_profile = load_discovered_cluster_profile(context)?;
    if !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
                        fs_probes: false,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            return Err(crate::exit::EnvironmentError::new(
                "preflight failed; fix the reported errors before conditional submission",
            )
            .into());
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering submission script", || {
        render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                cluster_profile,
                runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                    &context.cwd,
                    runtime_plan.slurm.runtime_root.as_deref(),
                )),
                annotate: false,
            },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| output::default_script_path(&file));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    Ok(PreparedSlurmSubmission {
        file,
        submit_dir,
        script_path,
        runtime_plan,
        record_options,
        output_format,
    })
}

fn validate_when_plan_conditions(plan: &RuntimePlan, conditions: &WhenConditions) -> Result<()> {
    if let Some(condition) = &conditions.free_nodes {
        let Some(plan_partition) = plan.slurm.partition.as_deref() else {
            bail!("--free-nodes requires x-slurm.partition to be set in the compose file");
        };
        if plan_partition != condition.partition {
            bail!(
                "--partition {} must match x-slurm.partition {} for --free-nodes",
                condition.partition,
                plan_partition
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WhenSubmitOutput<'a> {
    pub(crate) schema_version: u32,
    triggered: bool,
    // `WhenConditionSummary` (in `hpc_compose::when`) does not derive `JsonSchema`
    // and is outside this task's editable scope, so describe the array as
    // permissive JSON values in the published schema. Serde output is unchanged.
    #[schemars(with = "Vec<serde_json::Value>")]
    conditions: &'a [WhenConditionSummary],
    submission: &'a output::SubmitOutput,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn when(
    context: ResolvedContext,
    conditions: WhenConditions,
    poll_interval: std::time::Duration,
    timeout: Option<std::time::Duration>,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    options: WhenOptions,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    format: Option<OutputFormat>,
) -> Result<()> {
    let WhenOptions {
        allow_resume_changes,
        detach,
        quiet,
    } = options;
    let output_format = output::resolve_output_format(format);
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    let prepared = prepare_slurm_submission(
        &context,
        script_out,
        None,
        flags,
        !detach,
        allow_resume_changes,
        output_format,
        quiet,
        |plan| validate_when_plan_conditions(plan, &conditions),
    )?;

    if output_format == OutputFormat::Text && !quiet {
        println!("waiting for conditions:");
        for description in condition_descriptions(&conditions) {
            println!("  - {description}");
        }
    }

    let monitor_options = MonitorOptions {
        conditions,
        poll_interval,
        timeout,
        sinfo_bin: context.binaries.sinfo.value.clone(),
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let mut runtime = RealMonitorRuntime::new();
    let trigger = monitor_until_ready(&monitor_options, &mut runtime)?;

    if output_format == OutputFormat::Text && !quiet {
        println!("conditions satisfied:");
        for condition in &trigger.conditions {
            println!("  - {}", condition.detail);
        }
    }

    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
    match output_format {
        OutputFormat::Text => {
            print_slurm_submit_outcome(&prepared, &outcome)?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&WhenSubmitOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                    triggered: true,
                    conditions: &trigger.conditions,
                    submission: &outcome.submit_output,
                })
                .context("failed to serialize when output")?
            );
        }
    }
    maybe_watch_slurm_submission(
        &context,
        &outcome,
        !detach,
        false,
        None,
        watch_mode,
        hold_on_exit,
    )
}

fn condition_descriptions(conditions: &WhenConditions) -> Vec<String> {
    let mut descriptions = Vec::new();
    if let Some(condition) = &conditions.free_nodes {
        descriptions.push(format!(
            "partition {} has at least {} idle node(s)",
            condition.partition, condition.minimum_idle_nodes
        ));
    }
    if let Some(condition) = &conditions.after_job {
        descriptions.push(format!(
            "job {} satisfies {}",
            condition.job_id,
            condition.condition.as_str()
        ));
    }
    if let Some(window) = &conditions.time_window {
        descriptions.push(window.description());
    }
    descriptions
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn launch(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    options: LaunchOptions,
    queue_warn_after_seconds: Option<u64>,
    format: Option<OutputFormat>,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    metrics_overrides: MetricsOverrides,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let LaunchOptions {
        watch,
        watch_queue,
        local,
        allow_resume_changes,
        resume_diff_only,
        dry_run,
        print_endpoints,
        quiet,
    } = options;
    let file = context.compose_file.value.clone();
    let effective_config =
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml =
        output::effective_config_yaml(&effective_config, &context.secret_values())?;
    let mut runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    // Apply per-run metrics overrides (`up --metrics-interval` / `--no-metrics`)
    // before preflight/prepare/render so they take effect for this invocation.
    metrics_overrides.apply(&mut runtime_plan);
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let output_format = output::resolve_output_format(format);
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let backend = if local {
        SubmissionBackend::Local
    } else {
        SubmissionBackend::Slurm
    };
    let local_job_id = local.then(generate_local_job_id);
    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Main,
        service_name: None,
        command_override: None,
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: Some(effective_config_yaml.clone()),
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
    };

    if maybe_check_resume_diff(
        &file,
        runtime_plan.slurm.resume_dir().is_some(),
        &effective_config_yaml,
        allow_resume_changes,
        resume_diff_only,
        output_format,
    )? {
        return Ok(());
    }

    if !dry_run {
        ensure_batch_submission_supported(&runtime_plan, watch, local)?;
    } else if local {
        ensure_batch_submission_supported(&runtime_plan, false, true)?;
    }

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    }

    let cluster_profile = if local {
        None
    } else {
        load_discovered_cluster_profile(&context)?
    };

    if !dry_run && !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: !local,
                        skip_prepare,
                        fs_probes: false,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            return Err(crate::exit::EnvironmentError::new(
                "preflight failed; fix the reported errors before submitting",
            )
            .into());
        }
    }

    if !dry_run && !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering submission script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script_with_options(
                &runtime_plan,
                job_id,
                &context.binaries.enroot.value,
                &local_render_options(&context, &runtime_plan, false),
            )
        } else {
            render_script_with_options(
                &runtime_plan,
                &RenderOptions {
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    cluster_profile,
                    runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                        &context.cwd,
                        runtime_plan.slurm.runtime_root.as_deref(),
                    )),
                    annotate: false,
                },
            )
        }
    })?;
    let script_path = script_out.unwrap_or_else(|| {
        if local {
            output::default_local_script_path(&file)
        } else {
            output::default_script_path(&file)
        }
    });
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        match output_format {
            OutputFormat::Text => {
                println!("  script: {}", script_path.display());
                println!("  cache:  {}", runtime_plan.cache_dir.display());
                if local {
                    println!("dry run: skipping local launch");
                } else {
                    println!("dry run: skipping sbatch submission");
                }
            }
            OutputFormat::Json => {
                let (endpoints, next_commands) = if print_endpoints {
                    (
                        output::build_submit_endpoints(&runtime_plan),
                        output::submit_next_commands(
                            None,
                            output::artifact_export_configured(&runtime_plan),
                        ),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };
                println!(
                    "{}",
                    crate::output::to_pretty_json(&output::SubmitOutput {
                        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                        backend,
                        compose_file: file,
                        script_path,
                        cache_dir: runtime_plan.cache_dir,
                        dry_run: true,
                        launched: false,
                        submitted: false,
                        sbatch_stdout: None,
                        job_id: None,
                        tracking_persisted: false,
                        tracked_metadata_path: None,
                        endpoints,
                        next_commands,
                    })
                    .context("failed to serialize up output")?
                );
            }
        }
        return Ok(());
    }

    if local {
        let record = build_submission_record_with_backend_and_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            local_job_id
                .as_deref()
                .context("missing synthetic local job id")?,
            SubmissionBackend::Local,
            &record_options,
        )?;
        write_submission_record(&record)
            .context("failed to persist tracking metadata for local launch")?;
        let supervisor_pid =
            match spawn_local_supervisor(&submit_dir, &script_path, &record.batch_log) {
                Ok(pid) => pid,
                Err(err) => {
                    rollback_local_tracking(&record, None);
                    return Err(err);
                }
            };
        if let Err(err) = write_local_runtime_state_stub(&record, &runtime_plan, supervisor_pid) {
            rollback_local_tracking(&record, Some(supervisor_pid));
            return Err(err);
        }

        match output_format {
            OutputFormat::Text => {
                print_local_launch_details(&record, &runtime_plan, &script_path);
                output::print_submit_summary_box(
                    &runtime_plan,
                    &record.job_id,
                    &script_path,
                    Some(&latest_record_path(&record)),
                );
            }
            OutputFormat::Json => {
                let (endpoints, next_commands) = if print_endpoints {
                    (
                        output::build_submit_endpoints(&runtime_plan),
                        output::submit_next_commands(
                            Some(&record.job_id),
                            output::artifact_export_configured(&runtime_plan),
                        ),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };
                println!(
                    "{}",
                    crate::output::to_pretty_json(&output::SubmitOutput {
                        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                        backend: SubmissionBackend::Local,
                        compose_file: file.clone(),
                        script_path: script_path.clone(),
                        cache_dir: runtime_plan.cache_dir.clone(),
                        dry_run: false,
                        launched: true,
                        submitted: false,
                        sbatch_stdout: None,
                        job_id: Some(record.job_id.clone()),
                        tracking_persisted: true,
                        tracked_metadata_path: Some(latest_record_path(&record)),
                        endpoints,
                        next_commands,
                    })
                    .context("failed to serialize up output")?
                );
            }
        }

        if watch {
            output::finish_watch(
                &record,
                watch_with_fallback(
                    &record,
                    &SchedulerOptions {
                        squeue_bin: context.binaries.squeue.value.clone(),
                        sacct_bin: context.binaries.sacct.value.clone(),
                    },
                    None,
                    100,
                    watch_mode,
                    hold_on_exit,
                    watch_ui::WatchPrefs::resolve(&context.watch),
                )?,
            )?;
        }
        return Ok(());
    }

    let prepared = PreparedSlurmSubmission {
        file,
        submit_dir,
        script_path,
        runtime_plan,
        record_options,
        output_format,
    };
    let mut outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
    if print_endpoints {
        outcome.submit_output.endpoints = output::build_submit_endpoints(&prepared.runtime_plan);
        outcome.submit_output.next_commands = output::submit_next_commands(
            outcome.submit_output.job_id.as_deref(),
            output::artifact_export_configured(&prepared.runtime_plan),
        );
    }
    print_slurm_submit_outcome(&prepared, &outcome)?;
    maybe_watch_slurm_submission(
        &context,
        &outcome,
        watch,
        watch_queue,
        queue_warn_after_seconds,
        watch_mode,
        hold_on_exit,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn up(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    options: UpOptions,
    queue_warn_after_seconds: Option<u64>,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    format: Option<OutputFormat>,
    metrics_overrides: MetricsOverrides,
) -> Result<()> {
    let UpOptions {
        local,
        allow_resume_changes,
        resume_diff_only,
        dry_run,
        detach,
        watch_queue,
        print_endpoints,
        quiet,
    } = options;
    let _up_lock = if dry_run {
        None
    } else {
        Some(acquire_up_invocation_lock(&context.compose_file.value)?)
    };
    launch(
        context,
        script_out,
        flags,
        LaunchOptions {
            watch: !detach,
            watch_queue,
            local,
            allow_resume_changes,
            resume_diff_only,
            dry_run,
            print_endpoints,
            quiet,
        },
        queue_warn_after_seconds,
        format.or(Some(OutputFormat::Text)),
        watch_mode,
        hold_on_exit,
        metrics_overrides,
    )
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SmokePhase {
    name: &'static str,
    status: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SmokeTestMode {
    Smoke,
    Preemption,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct SmokeServiceResult {
    service_name: String,
    appeared: bool,
    launched: bool,
    readiness_configured: bool,
    ready: bool,
    completed_successfully: bool,
    last_exit_code: Option<i32>,
    status: Option<String>,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct PreemptionTestSummary {
    signal: String,
    signal_target: &'static str,
    grace_seconds: u64,
    observed_attempt: Option<u32>,
    observed_is_resume: Option<bool>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SmokeTestOutput {
    pub(crate) schema_version: u32,
    mode: SmokeTestMode,
    ok: bool,
    backend: SubmissionBackend,
    compose_file: PathBuf,
    job_id: String,
    script_path: PathBuf,
    timeout_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    preemption: Option<PreemptionTestSummary>,
    phases: Vec<SmokePhase>,
    services: Vec<SmokeServiceResult>,
    failure_reason: Option<String>,
}

#[derive(Debug)]
struct SmokeEvaluation {
    ok: bool,
    services: Vec<SmokeServiceResult>,
    failure_reason: Option<String>,
}

fn evaluate_smoke_snapshot(snapshot: &hpc_compose::job::StatusSnapshot) -> SmokeEvaluation {
    let mut services = Vec::new();
    let mut failures = Vec::new();
    for service in &snapshot.services {
        let appeared = true;
        let launched = service.started_at.is_some()
            || service.launcher_pid.is_some()
            || service.last_exit_code.is_some();
        let readiness_configured = service.readiness_configured.unwrap_or(false);
        let ready = !readiness_configured || service.healthy.unwrap_or(false);
        let completed_successfully = service.completed_successfully.unwrap_or(false);
        let mut service_failures = Vec::new();
        if !launched {
            service_failures.push("service did not launch".to_string());
        }
        if !ready {
            service_failures.push("configured readiness did not pass".to_string());
        }
        if !completed_successfully {
            service_failures.push("service did not complete successfully".to_string());
        }
        if let Some(assertions) = &service.assertions
            && assertions.configured
            && !assertions.failures.is_empty()
        {
            service_failures.extend(
                assertions
                    .failures
                    .iter()
                    .map(|failure| format!("assertion failed: {failure}")),
            );
        }
        if !service_failures.is_empty() {
            failures.push(format!(
                "{}: {}",
                service.service_name,
                service_failures.join("; ")
            ));
        }
        services.push(SmokeServiceResult {
            service_name: service.service_name.clone(),
            appeared,
            launched,
            readiness_configured,
            ready,
            completed_successfully,
            last_exit_code: service.last_exit_code,
            status: service.status.clone(),
            failures: service_failures,
        });
    }
    if snapshot.services.is_empty() {
        failures.push("runtime state did not include any services".to_string());
    }
    if snapshot.scheduler.failed {
        failures.push(format!("scheduler state is {}", snapshot.scheduler.state));
    }
    let ok = failures.is_empty();
    SmokeEvaluation {
        ok,
        services,
        failure_reason: (!ok).then(|| failures.join("; ")),
    }
}

fn validate_preemption_contract(plan: &RuntimePlan) -> Result<()> {
    let mut missing = Vec::new();
    if plan.slurm.resume_dir().is_none() {
        missing.push("x-slurm.resume");
    }
    if plan.slurm.requeue != Some(true) {
        missing.push("x-slurm.requeue: true");
    }
    if plan.slurm.signal.is_none() {
        missing.push("x-slurm.signal");
    }
    if !plan.ordered_services.iter().any(|service| {
        service
            .assertions
            .as_ref()
            .is_some_and(|assertions| !assertions.is_empty())
    }) {
        missing.push("at least one services.<name>.assert");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        bail!(
            "test --preemption requires a verifiable preemption contract: {}",
            missing.join(", ")
        )
    }
}

fn preemption_signal_target(shell: SignalShellTarget) -> &'static str {
    match shell {
        SignalShellTarget::Step => "step",
        SignalShellTarget::Batch => "batch",
    }
}

fn preemption_ready_failures(snapshot: &hpc_compose::job::StatusSnapshot) -> Vec<String> {
    let mut failures = Vec::new();
    if !snapshot.scheduler.state.eq_ignore_ascii_case("RUNNING") {
        failures.push(format!("scheduler state is {}", snapshot.scheduler.state));
    }
    if snapshot.services.is_empty() {
        failures.push("runtime state did not include any services".to_string());
    }
    for service in &snapshot.services {
        let launched = service.started_at.is_some()
            || service.launcher_pid.is_some()
            || service.last_exit_code.is_some();
        if !launched {
            failures.push(format!("{}: service did not launch", service.service_name));
        }
        let readiness_configured = service.readiness_configured.unwrap_or(false);
        if readiness_configured && !service.healthy.unwrap_or(false) {
            failures.push(format!(
                "{}: configured readiness did not pass",
                service.service_name
            ));
        }
    }
    failures
}

fn evaluate_preemption_snapshot(snapshot: &hpc_compose::job::StatusSnapshot) -> SmokeEvaluation {
    let base = evaluate_smoke_snapshot(snapshot);
    let mut failures = Vec::new();
    if snapshot.attempt.unwrap_or(0) < 1 {
        failures.push("attempt counter did not advance to a resumed attempt".to_string());
    }
    if snapshot.is_resume != Some(true) {
        failures.push("latest runtime state did not report is_resume=true".to_string());
    }
    if snapshot.resume_dir.is_none() {
        failures.push("latest runtime state did not report a resume directory".to_string());
    }
    if let Some(reason) = base.failure_reason.clone() {
        failures.push(reason);
    }
    let ok = failures.is_empty();
    SmokeEvaluation {
        ok,
        services: base.services,
        failure_reason: (!ok).then(|| failures.join("; ")),
    }
}

fn command_failure_detail(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn send_preemption_signal(
    context: &ResolvedContext,
    record: &SubmissionRecord,
    signal: &hpc_compose::spec::SignalConfig,
) -> Result<()> {
    let mut command = Command::new(&context.binaries.scancel.value);
    command.arg(format!("--signal={}", signal.name.as_str()));
    if signal.shell == SignalShellTarget::Batch {
        command.arg("--batch");
    }
    let output = command.arg(&record.job_id).output().with_context(|| {
        format!(
            "failed to execute '{}' for synthetic preemption signal",
            context.binaries.scancel.value
        )
    })?;
    if output.status.success() {
        return Ok(());
    }
    let detail = command_failure_detail(&output);
    if detail.is_empty() {
        bail!(
            "failed to send synthetic preemption signal to job {}",
            record.job_id
        );
    }
    bail!(
        "failed to send synthetic preemption signal to job {}: {detail}",
        record.job_id
    )
}

fn requeue_preemption_job(context: &ResolvedContext, record: &SubmissionRecord) -> Result<()> {
    let output = Command::new(&context.binaries.scontrol.value)
        .arg("requeue")
        .arg(&record.job_id)
        .output()
        .with_context(|| {
            format!(
                "failed to execute '{}' for synthetic preemption requeue",
                context.binaries.scontrol.value
            )
        })?;
    if output.status.success() {
        return Ok(());
    }
    let detail = command_failure_detail(&output);
    if detail.is_empty() {
        bail!(
            "failed to requeue job {} for preemption test",
            record.job_id
        );
    }
    bail!(
        "failed to requeue job {} for preemption test: {detail}",
        record.job_id
    )
}

fn wait_for_smoke_terminal(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    timeout_seconds: u64,
) -> Result<(hpc_compose::job::StatusSnapshot, bool)> {
    wait_for_smoke_terminal_until(
        record,
        options,
        Instant::now() + Duration::from_secs(timeout_seconds),
    )
}

fn wait_for_smoke_terminal_until(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    deadline: Instant,
) -> Result<(hpc_compose::job::StatusSnapshot, bool)> {
    loop {
        let snapshot = build_status_snapshot(&record.compose_file, Some(&record.job_id), options)?;
        if snapshot.scheduler.terminal {
            return Ok((snapshot, false));
        }
        if Instant::now() >= deadline {
            return Ok((snapshot, true));
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn wait_for_preemption_ready_until(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    deadline: Instant,
) -> Result<(hpc_compose::job::StatusSnapshot, bool, bool)> {
    loop {
        let snapshot = build_status_snapshot(&record.compose_file, Some(&record.job_id), options)?;
        if preemption_ready_failures(&snapshot).is_empty() {
            return Ok((snapshot, true, false));
        }
        if snapshot.scheduler.terminal {
            return Ok((snapshot, false, false));
        }
        if Instant::now() >= deadline {
            return Ok((snapshot, false, true));
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn wait_for_preemption_attempt_until(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    deadline: Instant,
) -> Result<(hpc_compose::job::StatusSnapshot, bool, bool)> {
    loop {
        let snapshot = build_status_snapshot(&record.compose_file, Some(&record.job_id), options)?;
        if snapshot.attempt.unwrap_or(0) >= 1 && snapshot.is_resume == Some(true) {
            return Ok((snapshot, true, false));
        }
        if snapshot.scheduler.terminal {
            return Ok((snapshot, false, false));
        }
        if Instant::now() >= deadline {
            return Ok((snapshot, false, true));
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn cancel_smoke_timeout(context: &ResolvedContext, record: &SubmissionRecord) {
    if record.backend == SubmissionBackend::Local {
        if let Ok(Some(pid)) = read_local_supervisor_pid(record)
            && let Err(err) = kill_pid(pid)
        {
            hpc_compose::diagnostics::warn(format!(
                "smoke test timed out but failed to stop local supervisor pid {pid}: {err}"
            ));
        }
        return;
    }
    match Command::new(&context.binaries.scancel.value)
        .arg(&record.job_id)
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(status) => hpc_compose::diagnostics::warn(format!(
            "smoke test timed out but scancel exited with {status} for job {}",
            record.job_id
        )),
        Err(err) => hpc_compose::diagnostics::warn(format!(
            "smoke test timed out but failed to run scancel for job {}: {err}",
            record.job_id
        )),
    }
}

fn smoke_test_via_dev_cluster(
    context: &ResolvedContext,
    time: &str,
    timeout: &str,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<()> {
    let checkout_root = find_dev_cluster_checkout(&context.cwd)?;
    let project_dir = hpc_compose::context::repo_root_or_cwd(&context.cwd);
    let compose_file =
        dev_cluster_container_path(&project_dir, &context.compose_file.value, "compose file")?;
    let script_out = script_out
        .map(|path| {
            let absolute = crate::path_util::absolute_path(&path, &context.cwd);
            dev_cluster_container_path(&project_dir, &absolute, "--script-out")
        })
        .transpose()?;
    let devcluster_script = checkout_root.join("scripts/devcluster.sh");

    let up_status = Command::new(&devcluster_script)
        .arg("up")
        .arg("--project")
        .arg(&project_dir)
        .status()
        .with_context(|| {
            format!(
                "failed to start local Slurm dev cluster via {}",
                devcluster_script.display()
            )
        })?;
    dev_cluster_child_status_result(
        up_status,
        "local Slurm dev-cluster startup terminated by signal",
    )?;

    let mut args = vec![OsString::from("exec"), OsString::from("hpc-compose")];
    if quiet {
        args.push(OsString::from("--quiet"));
    }
    if let Some(settings_path) = &context.settings_path {
        let settings_path =
            dev_cluster_container_path(&project_dir, settings_path, "--settings-file")?;
        args.extend([
            OsString::from("--settings-file"),
            OsString::from(settings_path),
        ]);
    }
    if let Some(profile) = &context.selected_profile {
        args.extend([OsString::from("--profile"), OsString::from(profile)]);
    }
    args.extend([
        OsString::from("test"),
        OsString::from("--submit"),
        OsString::from("--time"),
        OsString::from(time),
        OsString::from("--timeout"),
        OsString::from(timeout),
        OsString::from("-f"),
        OsString::from(compose_file),
    ]);
    if let Some(path) = script_out {
        args.extend([OsString::from("--script-out"), OsString::from(path)]);
    }
    if flags.keep_failed_prep {
        args.push(OsString::from("--keep-failed-prep"));
    }
    if flags.skip_prepare {
        args.push(OsString::from("--skip-prepare"));
    }
    if flags.force_rebuild {
        args.push(OsString::from("--force-rebuild"));
    }
    if flags.no_preflight {
        args.push(OsString::from("--no-preflight"));
    }
    if output_format == OutputFormat::Json {
        args.extend([OsString::from("--format"), OsString::from("json")]);
    }

    let exec_status = Command::new(&devcluster_script)
        .args(args)
        .status()
        .with_context(|| {
            format!(
                "failed to run test --submit inside local Slurm dev cluster via {}",
                devcluster_script.display()
            )
        })?;
    dev_cluster_child_status_result(
        exec_status,
        "local Slurm dev-cluster smoke test terminated by signal",
    )
}

fn find_dev_cluster_checkout(start: &Path) -> Result<PathBuf> {
    if let Some(root) = find_dev_cluster_checkout_from(start) {
        return Ok(root);
    }
    if let Ok(exe) = env::current_exe()
        && let Some(root) = find_dev_cluster_checkout_from(&exe)
    {
        return Ok(root);
    }
    bail!(
        "test --submit --dev-cluster requires an hpc-compose source checkout containing scripts/devcluster.sh and dev-cluster/compose.yaml"
    );
}

fn find_dev_cluster_checkout_from(start: &Path) -> Option<PathBuf> {
    for dir in start.ancestors() {
        if dir.join("scripts/devcluster.sh").is_file()
            && dir.join("dev-cluster/compose.yaml").is_file()
        {
            return Some(dir.to_path_buf());
        }
    }
    None
}

fn dev_cluster_container_path(project_dir: &Path, host_path: &Path, label: &str) -> Result<String> {
    let relative = dev_cluster_project_relative_path(project_dir, host_path).with_context(|| {
        format!(
            "test --submit --dev-cluster requires {label} '{}' to live under the mounted project directory '{}'",
            host_path.display(),
            project_dir.display()
        )
    })?;
    Ok(Path::new("/workspace").join(relative).display().to_string())
}

fn dev_cluster_project_relative_path(project_dir: &Path, host_path: &Path) -> Result<PathBuf> {
    if let Ok(relative) = host_path.strip_prefix(project_dir) {
        return Ok(relative.to_path_buf());
    }

    let canonical_project = fs::canonicalize(project_dir)
        .with_context(|| format!("failed to resolve {}", project_dir.display()))?;
    let canonical_host = canonical_or_parent_joined(host_path)
        .with_context(|| format!("failed to resolve {}", host_path.display()))?;
    canonical_host
        .strip_prefix(&canonical_project)
        .map(Path::to_path_buf)
        .with_context(|| "prefix not found")
}

fn canonical_or_parent_joined(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path)
            .with_context(|| format!("failed to resolve {}", path.display()));
    }
    let parent = path
        .parent()
        .with_context(|| format!("path '{}' has no parent", path.display()))?;
    let file_name = path
        .file_name()
        .with_context(|| format!("path '{}' has no file name", path.display()))?;
    Ok(fs::canonicalize(parent)
        .with_context(|| format!("failed to resolve {}", parent.display()))?
        .join(file_name))
}

fn dev_cluster_child_status_result(
    status: std::process::ExitStatus,
    signal_message: &str,
) -> Result<()> {
    match status.code() {
        Some(0) => Ok(()),
        Some(code) => Err(crate::exit::ExitCodeError(code).into()),
        None => bail!("{signal_message}"),
    }
}

fn print_smoke_output(output_format: OutputFormat, report: &SmokeTestOutput) -> Result<()> {
    match output_format {
        OutputFormat::Text => {
            let label = match report.mode {
                SmokeTestMode::Smoke => "smoke test",
                SmokeTestMode::Preemption => "preemption test",
            };
            if report.ok {
                println!("{label} passed: {}", report.job_id);
            } else {
                println!("{label} failed: {}", report.job_id);
                if let Some(reason) = &report.failure_reason {
                    println!("reason: {reason}");
                }
            }
            println!("script: {}", report.script_path.display());
            if let Some(preemption) = &report.preemption {
                println!(
                    "preemption: signal={} target={} grace={}s observed_attempt={} is_resume={}",
                    preemption.signal,
                    preemption.signal_target,
                    preemption.grace_seconds,
                    preemption
                        .observed_attempt
                        .map_or_else(|| "unknown".to_string(), |attempt| attempt.to_string()),
                    preemption
                        .observed_is_resume
                        .map_or_else(|| "unknown".to_string(), |is_resume| is_resume.to_string())
                );
            }
            for service in &report.services {
                let state = if service.failures.is_empty() {
                    "ok".to_string()
                } else {
                    service.failures.join("; ")
                };
                println!("service {}: {state}", service.service_name);
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(report)
                    .context("failed to serialize smoke test output")?
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn smoke_test(
    context: ResolvedContext,
    local: bool,
    submit: bool,
    dev_cluster: bool,
    preemption: bool,
    time: String,
    timeout: String,
    preemption_grace: Option<String>,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    if dev_cluster && !submit {
        bail!("test --dev-cluster requires --submit");
    }
    let mode_count = [local, submit, preemption]
        .into_iter()
        .filter(|enabled| *enabled)
        .count();
    if mode_count != 1 {
        bail!(
            "test requires exactly one execution mode; choose --local, --submit, or --preemption"
        );
    }
    if preemption_grace.is_some() && !preemption {
        bail!("--preemption-grace is only valid with test --preemption");
    }
    if dev_cluster && preemption {
        bail!("test --dev-cluster is only valid with test --submit smoke tests");
    }
    if submit || preemption {
        parse_slurm_time_limit(&time).context("test --time is invalid")?;
    }
    let timeout_seconds =
        parse_log_since_duration(&timeout).context("test --timeout is invalid")?;
    let preemption_grace_seconds = match preemption_grace {
        Some(value) => {
            Some(parse_log_since_duration(&value).context("test --preemption-grace is invalid")?)
        }
        None if preemption => Some(10),
        None => None,
    };
    let output_format = output::resolve_output_format(format);
    if dev_cluster {
        return smoke_test_via_dev_cluster(
            &context,
            &time,
            &timeout,
            script_out,
            flags,
            output_format,
            quiet,
        );
    }
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    let (backend, record, script_path, preemption_signal) = if local {
        let prepared = prepare_local_launch(
            &context,
            script_out,
            flags,
            output_format,
            quiet,
            false,
            |_| Ok(()),
        )?;
        let outcome = start_prepared_local_launch(&prepared)?;
        (
            SubmissionBackend::Local,
            outcome.record,
            prepared.script_path.clone(),
            None,
        )
    } else {
        let preemption_enabled = preemption;
        let prepared = prepare_slurm_submission(
            &context,
            script_out,
            Some(time),
            flags,
            false,
            false,
            output_format,
            quiet,
            |plan| {
                if preemption_enabled {
                    validate_preemption_contract(plan)?;
                }
                Ok(())
            },
        )?;
        let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
        let outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
        let record = outcome
            .tracked_submission
            .as_ref()
            .map(|(record, _)| record.clone())
            .context(
                "smoke test submission was not trackable; sbatch output did not include a job id",
            )?;
        (
            SubmissionBackend::Slurm,
            record,
            prepared.script_path.clone(),
            prepared.runtime_plan.slurm.signal.clone(),
        )
    };

    let mode = if preemption {
        SmokeTestMode::Preemption
    } else {
        SmokeTestMode::Smoke
    };
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut phases = vec![SmokePhase {
        name: "launch",
        status: "ok",
    }];
    let mut preemption_summary = None;
    let (snapshot, timed_out) = if preemption {
        let (ready_snapshot, ready, ready_timed_out) =
            wait_for_preemption_ready_until(&record, &scheduler_options, deadline)?;
        phases.push(SmokePhase {
            name: "running",
            status: if ready_timed_out {
                "timeout"
            } else if ready {
                "ok"
            } else {
                "failed"
            },
        });
        if !ready {
            (ready_snapshot, ready_timed_out)
        } else {
            let signal = preemption_signal
                .as_ref()
                .context("test --preemption requires x-slurm.signal")?;
            send_preemption_signal(&context, &record, signal)?;
            phases.push(SmokePhase {
                name: "signal",
                status: "ok",
            });
            let grace_seconds = preemption_grace_seconds.unwrap_or(10);
            let remaining_before_grace = deadline.saturating_duration_since(Instant::now());
            preemption_summary = Some(PreemptionTestSummary {
                signal: signal.name.as_str().to_string(),
                signal_target: preemption_signal_target(signal.shell),
                grace_seconds,
                observed_attempt: None,
                observed_is_resume: None,
            });
            if remaining_before_grace < Duration::from_secs(grace_seconds) {
                thread::sleep(remaining_before_grace);
                (
                    build_status_snapshot(
                        &record.compose_file,
                        Some(&record.job_id),
                        &scheduler_options,
                    )?,
                    true,
                )
            } else {
                thread::sleep(Duration::from_secs(grace_seconds));
                requeue_preemption_job(&context, &record)?;
                phases.push(SmokePhase {
                    name: "requeue",
                    status: "ok",
                });
                let (attempt_snapshot, attempt_seen, attempt_timed_out) =
                    wait_for_preemption_attempt_until(&record, &scheduler_options, deadline)?;
                phases.push(SmokePhase {
                    name: "attempt_2",
                    status: if attempt_timed_out {
                        "timeout"
                    } else if attempt_seen {
                        "ok"
                    } else {
                        "failed"
                    },
                });
                if let Some(summary) = &mut preemption_summary {
                    summary.observed_attempt = attempt_snapshot.attempt;
                    summary.observed_is_resume = attempt_snapshot.is_resume;
                }
                if !attempt_seen {
                    (attempt_snapshot, attempt_timed_out)
                } else {
                    wait_for_smoke_terminal_until(&record, &scheduler_options, deadline)?
                }
            }
        }
    } else {
        wait_for_smoke_terminal(&record, &scheduler_options, timeout_seconds)?
    };
    if timed_out {
        cancel_smoke_timeout(&context, &record);
    }
    let mut evaluation = if preemption {
        evaluate_preemption_snapshot(&snapshot)
    } else {
        evaluate_smoke_snapshot(&snapshot)
    };
    if timed_out {
        let label = match mode {
            SmokeTestMode::Smoke => "smoke test",
            SmokeTestMode::Preemption => "preemption test",
        };
        let timeout_reason = format!("{label} timed out after {timeout_seconds}s");
        evaluation.ok = false;
        evaluation.failure_reason = Some(match evaluation.failure_reason {
            Some(reason) => format!("{timeout_reason}; {reason}"),
            None => timeout_reason,
        });
    }
    phases.push(SmokePhase {
        name: "terminal",
        status: if timed_out { "timeout" } else { "ok" },
    });
    phases.push(SmokePhase {
        name: "evaluate",
        status: if evaluation.ok { "ok" } else { "failed" },
    });
    let report = SmokeTestOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        mode,
        ok: evaluation.ok,
        backend,
        compose_file: record.compose_file.clone(),
        job_id: record.job_id.clone(),
        script_path,
        timeout_seconds,
        preemption: preemption_summary,
        phases,
        services: evaluation.services,
        failure_reason: evaluation.failure_reason.clone(),
    };
    print_smoke_output(output_format, &report)?;
    if !report.ok {
        bail!(
            "{}",
            report
                .failure_reason
                .unwrap_or_else(|| "smoke test failed".to_string())
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
