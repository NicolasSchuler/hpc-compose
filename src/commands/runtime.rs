use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{HoldOnExit, OutputFormat, StatsOutputFormat, WatchMode};
use hpc_compose::cluster::{discover_cluster_profile_path, load_cluster_profile};
use hpc_compose::context::{BinaryOverrides, ResolveRequest, ResolvedContext, resolve};
#[cfg(test)]
use hpc_compose::job::build_submission_record_with_backend;
use hpc_compose::job::{
    ArtifactExportOptions, CleanupMode, EfficiencyScoreOptions, MetricsProbeOptions,
    RequestedWalltime, SWEEP_MANIFEST_SCHEMA_VERSION, SchedulerOptions, StatsOptions,
    SubmissionBackend, SubmissionKind, SubmissionRecord, SubmissionRecordBuildOptions,
    SweepExpansionTrial, SweepManifest, SweepManifestTrial, SweepTrialMetadata,
    build_array_status_snapshot, build_cleanup_report, build_efficiency_score_report,
    build_job_diff_report, build_metrics_probe_report, build_ps_snapshot, build_replay_report,
    build_rightsize_report, build_stats_snapshot, build_status_snapshot,
    build_status_snapshot_with_array, build_submission_record_with_backend_and_options,
    build_submission_record_with_options, expand_sweep_with_limit, export_artifacts,
    find_submission_record_in_repo, generate_sweep_id, interpolation_vars_for_sweep_trial,
    jobs_dir_for, latest_canary_record_path_for, latest_notebook_record_path_for,
    latest_record_path_for, latest_run_record_path_for, load_submission_record,
    load_sweep_manifest, metadata_root_for, parse_log_since_duration, print_logs,
    remove_submission_record, run_cleanup_report, runtime_job_root_for_record, scan_job_inventory,
    scan_job_records, scan_sweep_manifests, serialize_metrics_probe_report, state_path_for_record,
    sweep_manifest_path_for, validate_metrics_probe_options, wait_for_job_start, watch_submission,
    write_submission_record, write_sweep_manifest,
};
use hpc_compose::planner::{
    ExecutionSpec, ImageSource, ServicePlacementMode, apply_resource_profile_defaults,
};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{
    PrepareOptions, RuntimePlan, base_image_path_for_backend, prepare_runtime_plan,
};
use hpc_compose::render::{
    LocalRenderOptions, RenderOptions, log_file_name_for_service, render_local_script,
    render_local_script_with_options, render_script_with_options,
};
use hpc_compose::rendezvous::{self, RendezvousRegisterRequest};
use hpc_compose::spec::{
    ComposeSpec, MetricsCollector, MetricsConfig, RuntimeConfig, ServiceFailureMode,
    parse_slurm_time_limit,
};
use hpc_compose::when::{
    MonitorOptions, RealMonitorRuntime, WhenConditionSummary, WhenConditions, monitor_until_ready,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::output;
use crate::progress::{PrepareProgress, ProgressReporter};
use crate::term;
use crate::watch_ui;

pub(crate) mod notebook;
mod resources;
pub(crate) use notebook::NotebookKind;
use notebook::{
    NotebookArgs, build_connection, build_notebook_service_spec, build_server_command,
    generate_token, preset_for, readiness_spec, resolve_image,
};
pub(crate) use resources::ResourceCliOptions;
use resources::{
    build_ephemeral_runtime_plan, build_synthetic_service_plan, parse_env_entries,
    push_slurm_salloc_options, push_slurm_srun_options, slurm_from_resource_options,
};

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
                let _ = writeln!(
                    io::stderr(),
                    "warning: live watch UI unavailable ({err}); falling back to line mode"
                );
                let _ = io::stderr().flush();
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

struct UpInvocationLock {
    path: Option<PathBuf>,
}

impl Drop for UpInvocationLock {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_file(path);
        }
    }
}

fn acquire_up_invocation_lock(compose_file: &Path) -> Result<UpInvocationLock> {
    let canonical = fs::canonicalize(compose_file).unwrap_or_else(|_| compose_file.to_path_buf());
    let mut digest = Sha256::new();
    digest.update(canonical.to_string_lossy().as_bytes());
    let hash = hex::encode(digest.finalize());
    let lock_dir = metadata_root_for(compose_file).join("locks");
    if let Err(err) = fs::create_dir_all(&lock_dir) {
        let _ = writeln!(
            io::stderr(),
            "warning: concurrent up protection unavailable because {} could not be created: {err}",
            lock_dir.display()
        );
        let _ = io::stderr().flush();
        return Ok(UpInvocationLock { path: None });
    }
    let path = lock_dir.join(format!("{hash}.up.lock"));
    let command = env::args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let content = serde_json::json!({
        "pid": std::process::id(),
        "command": command,
        "compose_path": canonical,
        "created_at_unix": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    });
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            writeln!(
                file,
                "{}",
                serde_json::to_string_pretty(&content).context("failed to serialize lock file")?
            )
            .with_context(|| format!("failed to write {}", path.display()))?;
            Ok(UpInvocationLock { path: Some(path) })
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read_to_string(&path).unwrap_or_else(|_| "<unreadable>".to_string());
            bail!(
                "another hpc-compose up appears to be running for {}; lock file: {}; existing lock: {}; if this process is gone, remove the lock file and retry",
                compose_file.display(),
                path.display(),
                existing.trim()
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            let _ = writeln!(
                io::stderr(),
                "warning: concurrent up protection unavailable because {} could not be created: {err}",
                path.display()
            );
            let _ = io::stderr().flush();
            Ok(UpInvocationLock { path: None })
        }
        Err(err) => Err(err).with_context(|| format!("failed to create {}", path.display())),
    }
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
        let _ = writeln!(io::stderr(), "warning: {note}");
        let _ = io::stderr().flush();
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
    if plan.runtime.backend != hpc_compose::spec::RuntimeBackend::Pyxis {
        bail!(
            "--local currently supports only runtime.backend=pyxis; got runtime.backend={}",
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
        let _ = writeln!(
            io::stderr(),
            "warning: --local ignores reservation-related x-slurm.submit_args"
        );
    }
    if plan.slurm.error.is_some() {
        let _ = writeln!(
            io::stderr(),
            "warning: --local ignores x-slurm.error and writes batch stderr into the local batch log"
        );
    }
    let _ = io::stderr().flush();
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
    let log_dir = job_root.join("logs");
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
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&state).context("failed to serialize local runtime state")?,
    )
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
        let _ = writeln!(
            io::stderr(),
            "warning: failed to stop local supervisor {} during rollback: {err}",
            pid
        );
    }
    if let Err(err) = remove_submission_record(record) {
        let _ = writeln!(
            io::stderr(),
            "warning: failed to roll back tracked metadata for local job {}: {err}",
            record.job_id
        );
    }
    let _ = io::stderr().flush();
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
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
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
            bail!("preflight failed; fix the reported errors before local launch");
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
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
            &LocalRenderOptions { dev_reload },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| output::default_local_script_path(&file));
    fs::write(&script_path, script).with_context(|| {
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
                serde_json::to_string_pretty(&outcome.submit_output)
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

fn submit_prepared_slurm_submission(
    context: &ResolvedContext,
    prepared: &PreparedSlurmSubmission,
    progress: &ProgressReporter,
) -> Result<SlurmSubmitOutcome> {
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
            String::from_utf8_lossy(&output_result.stderr).trim()
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
                let _ = writeln!(
                    io::stderr(),
                    "warning: job submitted, but failed to write tracking metadata: {err}"
                );
                let _ = io::stderr().flush();
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
                serde_json::to_string_pretty(&outcome.submit_output)
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
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let mut runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
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
            bail!("preflight failed; fix the reported errors before conditional submission");
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
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
                cluster_profile,
            },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| output::default_script_path(&file));
    fs::write(&script_path, script).with_context(|| {
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

#[derive(Debug, Serialize)]
struct WhenSubmitOutput<'a> {
    triggered: bool,
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
    allow_resume_changes: bool,
    detach: bool,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
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
                serde_json::to_string_pretty(&WhenSubmitOutput {
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
    watch: bool,
    watch_queue: bool,
    queue_warn_after_seconds: Option<u64>,
    local: bool,
    allow_resume_changes: bool,
    resume_diff_only: bool,
    dry_run: bool,
    format: Option<OutputFormat>,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let file = context.compose_file.value.clone();
    let effective_config =
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let output_format = output::resolve_output_format(format, false);
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
                        require_submit_tools: !local,
                        skip_prepare,
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
            bail!("preflight failed; fix the reported errors before submitting");
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering submission script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script(&runtime_plan, job_id, &context.binaries.enroot.value)
        } else {
            render_script_with_options(
                &runtime_plan,
                &RenderOptions {
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    cluster_profile,
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
    fs::write(&script_path, script).with_context(|| {
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
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output::SubmitOutput {
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
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output::SubmitOutput {
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
    let outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
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
    local: bool,
    allow_resume_changes: bool,
    resume_diff_only: bool,
    dry_run: bool,
    detach: bool,
    watch_queue: bool,
    queue_warn_after_seconds: Option<u64>,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    launch(
        context,
        script_out,
        flags,
        !detach,
        watch_queue,
        queue_warn_after_seconds,
        local,
        allow_resume_changes,
        resume_diff_only,
        dry_run,
        format.or(Some(OutputFormat::Text)),
        watch_mode,
        hold_on_exit,
        quiet,
    )
}

#[derive(Debug, Serialize)]
struct SmokePhase {
    name: &'static str,
    status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Serialize)]
struct SmokeTestOutput {
    ok: bool,
    backend: SubmissionBackend,
    compose_file: PathBuf,
    job_id: String,
    script_path: PathBuf,
    timeout_seconds: u64,
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

fn wait_for_smoke_terminal(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    timeout_seconds: u64,
) -> Result<(hpc_compose::job::StatusSnapshot, bool)> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
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

fn cancel_smoke_timeout(context: &ResolvedContext, record: &SubmissionRecord) {
    if record.backend == SubmissionBackend::Local {
        if let Ok(Some(pid)) = read_local_supervisor_pid(record) {
            let _ = kill_pid(pid);
        }
        return;
    }
    let _ = Command::new(&context.binaries.scancel.value)
        .arg(&record.job_id)
        .status();
}

fn print_smoke_output(output_format: OutputFormat, report: &SmokeTestOutput) -> Result<()> {
    match output_format {
        OutputFormat::Text => {
            if report.ok {
                println!("smoke test passed: {}", report.job_id);
            } else {
                println!("smoke test failed: {}", report.job_id);
                if let Some(reason) = &report.failure_reason {
                    println!("reason: {reason}");
                }
            }
            println!("script: {}", report.script_path.display());
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
                serde_json::to_string_pretty(report)
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
    time: String,
    wait_timeout: String,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    if local == submit {
        bail!("test requires exactly one execution mode; choose --local or --submit");
    }
    if submit {
        parse_slurm_time_limit(&time).context("test --time is invalid")?;
    }
    let timeout_seconds =
        parse_log_since_duration(&wait_timeout).context("test --wait-timeout is invalid")?;
    let output_format = output::resolve_output_format(format, false);
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    let (backend, record, script_path) = if local {
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
        )
    } else {
        let prepared = prepare_slurm_submission(
            &context,
            script_out,
            Some(time),
            flags,
            false,
            false,
            output_format,
            quiet,
            |_| Ok(()),
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
        )
    };

    let (snapshot, timed_out) =
        wait_for_smoke_terminal(&record, &scheduler_options, timeout_seconds)?;
    if timed_out {
        cancel_smoke_timeout(&context, &record);
    }
    let mut evaluation = evaluate_smoke_snapshot(&snapshot);
    if timed_out {
        let timeout_reason = format!("smoke test timed out after {timeout_seconds}s");
        evaluation.ok = false;
        evaluation.failure_reason = Some(match evaluation.failure_reason {
            Some(reason) => format!("{timeout_reason}; {reason}"),
            None => timeout_reason,
        });
    }
    let report = SmokeTestOutput {
        ok: evaluation.ok,
        backend,
        compose_file: record.compose_file.clone(),
        job_id: record.job_id.clone(),
        script_path,
        timeout_seconds,
        phases: vec![
            SmokePhase {
                name: "launch",
                status: "ok",
            },
            SmokePhase {
                name: "terminal",
                status: if timed_out { "timeout" } else { "ok" },
            },
            SmokePhase {
                name: "evaluate",
                status: if evaluation.ok { "ok" } else { "failed" },
            },
        ],
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DevPathState {
    modified_nanos: Option<u128>,
    len: u64,
    is_dir: bool,
}

#[derive(Debug, Clone)]
struct DevWatchTarget {
    root: PathBuf,
    services: BTreeSet<String>,
    snapshot: BTreeMap<PathBuf, DevPathState>,
}

type DevWatchSnapshot = BTreeMap<PathBuf, DevPathState>;

fn normalize_dev_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn absolute_dev_path(cwd: &Path, path: &Path) -> PathBuf {
    normalize_dev_path(if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    })
}

fn canonical_dev_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn mount_host_path(mount: &str) -> Option<PathBuf> {
    let (host, rest) = mount.split_once(':')?;
    if host.is_empty() || rest.is_empty() {
        return None;
    }
    Some(PathBuf::from(host))
}

fn infer_dev_watch_targets(
    plan: &RuntimePlan,
    cwd: &Path,
    explicit_paths: &[PathBuf],
) -> Result<Vec<DevWatchTarget>> {
    let mut roots: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let cache_dir = canonical_dev_path(&plan.cache_dir);
    for service in &plan.ordered_services {
        for mount in &service.volumes {
            let Some(host) = mount_host_path(mount) else {
                continue;
            };
            let host = absolute_dev_path(cwd, &host);
            if !host.is_dir() {
                continue;
            }
            let host = canonical_dev_path(&host);
            if host.starts_with(&cache_dir) {
                continue;
            }
            roots.entry(host).or_default().insert(service.name.clone());
        }
    }
    let all_services = plan
        .ordered_services
        .iter()
        .map(|service| service.name.clone())
        .collect::<BTreeSet<_>>();
    for raw_path in explicit_paths {
        let path = absolute_dev_path(cwd, raw_path);
        if !path.is_dir() {
            bail!(
                "dev --watch-paths must point to an existing directory: {}",
                path.display()
            );
        }
        let path = canonical_dev_path(&path);
        roots.entry(path).or_default().extend(all_services.clone());
    }
    if roots.is_empty() {
        bail!(
            "dev could not infer any watchable source directories from service volumes; add --watch-paths PATH"
        );
    }
    roots
        .into_iter()
        .map(|(root, services)| {
            Ok(DevWatchTarget {
                snapshot: collect_dev_snapshot(&root)?,
                root,
                services,
            })
        })
        .collect()
}

fn path_modified_nanos(path: &Path) -> Option<u128> {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn collect_dev_snapshot(root: &Path) -> Result<DevWatchSnapshot> {
    let mut snapshot = BTreeMap::new();
    collect_dev_snapshot_inner(root, &mut snapshot)?;
    Ok(snapshot)
}

fn collect_dev_snapshot_inner(root: &Path, snapshot: &mut DevWatchSnapshot) -> Result<()> {
    let metadata = match fs::metadata(root) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(()),
    };
    snapshot.insert(
        root.to_path_buf(),
        DevPathState {
            modified_nanos: path_modified_nanos(root),
            len: metadata.len(),
            is_dir: metadata.is_dir(),
        },
    );
    if !metadata.is_dir() {
        return Ok(());
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if file_type.is_symlink() {
            continue;
        }
        collect_dev_snapshot_inner(&path, snapshot)?;
    }
    Ok(())
}

fn write_dev_restart_request(control_dir: &Path, services: &BTreeSet<String>) -> Result<PathBuf> {
    let request_dir = control_dir.join("restart");
    fs::create_dir_all(&request_dir)
        .with_context(|| format!("failed to create {}", request_dir.display()))?;
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = request_dir.join(format!("restart-{}-{millis}.request", std::process::id()));
    let body = services.iter().cloned().collect::<Vec<_>>().join("\n");
    fs::write(&path, format!("{body}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

/// Detects changes across all dev watch targets once, returning the services to
/// restart and updating each target's snapshot in place.
fn detect_dev_changes(targets: &mut [DevWatchTarget]) -> BTreeSet<String> {
    let mut affected = BTreeSet::new();
    for target in targets {
        if let Ok(current) = collect_dev_snapshot(&target.root)
            && current != target.snapshot
        {
            affected.extend(target.services.iter().cloned());
            target.snapshot = current;
        }
    }
    affected
}

/// Spawns a background thread that watches the dev source directories and writes
/// restart requests on change, mirroring the text-mode dev loop. It runs until
/// [`DEV_SHUTDOWN_REQUESTED`] is set so the foreground watch UI stays in control.
fn spawn_dev_file_watch(
    mut targets: Vec<DevWatchTarget>,
    control_dir: PathBuf,
    debounce_ms: u64,
) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        while !DEV_SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            let mut affected = detect_dev_changes(&mut targets);
            if !affected.is_empty() {
                thread::sleep(Duration::from_millis(debounce_ms));
                affected.extend(detect_dev_changes(&mut targets));
                let _ = write_dev_restart_request(&control_dir, &affected);
            }
            thread::sleep(Duration::from_millis(250));
        }
    })
}

#[cfg(unix)]
extern "C" fn handle_dev_signal(_: libc::c_int) {
    DEV_SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_dev_signal_handlers() {
    DEV_SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGINT, handle_dev_signal as *const () as usize);
        libc::signal(libc::SIGTERM, handle_dev_signal as *const () as usize);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn dev(
    context: ResolvedContext,
    watch_paths: Vec<PathBuf>,
    debounce_ms: u64,
    keep_running: bool,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
    tui: bool,
) -> Result<()> {
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    let prepared = prepare_local_launch(
        &context,
        script_out,
        flags,
        OutputFormat::Text,
        quiet,
        true,
        |plan| infer_dev_watch_targets(plan, &context.cwd, &watch_paths).map(|_| ()),
    )?;
    let mut targets = infer_dev_watch_targets(&prepared.runtime_plan, &context.cwd, &watch_paths)?;
    let outcome = start_prepared_local_launch(&prepared)?;
    if !quiet {
        print_local_launch_outcome(&prepared, &outcome)?;
        println!("watching source directories:");
        for target in &targets {
            println!(
                "  {} -> {}",
                target.root.display(),
                target
                    .services
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    let control_dir = runtime_job_root_for_record(&outcome.record).join("dev-control");
    install_dev_signal_handlers();
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    if tui {
        // Drive file-watch reloads from a background thread while the live watch
        // UI runs in the foreground. The in-job supervisor consumes the restart
        // requests both threads write (auto-reloads here, the `r` key in the UI).
        let prefs = watch_ui::WatchPrefs::resolve(&context.watch);
        let watcher = spawn_dev_file_watch(targets, control_dir, debounce_ms);
        let ui_result = watch_ui::run_watch_ui(
            &outcome.record,
            &scheduler_options,
            None,
            200,
            HoldOnExit::Failure,
            prefs,
        );
        DEV_SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        let _ = watcher.join();
        ui_result?;
        if !keep_running && let Some(pid) = read_local_supervisor_pid(&outcome.record)? {
            kill_pid(pid).with_context(|| {
                format!("failed to stop local dev job {}", outcome.record.job_id)
            })?;
            if !quiet {
                println!("stopped local dev job: {}", outcome.record.job_id);
            }
        }
        return Ok(());
    }

    loop {
        if DEV_SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            if !keep_running && let Some(pid) = read_local_supervisor_pid(&outcome.record)? {
                kill_pid(pid).with_context(|| {
                    format!("failed to stop local dev job {}", outcome.record.job_id)
                })?;
                if !quiet {
                    println!("stopped local dev job: {}", outcome.record.job_id);
                }
            }
            return Ok(());
        }
        let snapshot = build_status_snapshot(
            &outcome.record.compose_file,
            Some(&outcome.record.job_id),
            &scheduler_options,
        )?;
        if snapshot.scheduler.terminal {
            if snapshot.scheduler.failed {
                bail!(
                    "local dev job {} reached terminal state {}",
                    outcome.record.job_id,
                    snapshot.scheduler.state
                );
            }
            if !quiet {
                println!(
                    "local dev job {} completed; leaving dev mode",
                    outcome.record.job_id
                );
            }
            return Ok(());
        }

        let mut affected = BTreeSet::new();
        for target in &mut targets {
            let current = collect_dev_snapshot(&target.root)?;
            if current != target.snapshot {
                affected.extend(target.services.iter().cloned());
                target.snapshot = current;
            }
        }
        if !affected.is_empty() {
            thread::sleep(Duration::from_millis(debounce_ms));
            for target in &mut targets {
                let current = collect_dev_snapshot(&target.root)?;
                if current != target.snapshot {
                    affected.extend(target.services.iter().cloned());
                }
                target.snapshot = current;
            }
            write_dev_restart_request(&control_dir, &affected)?;
            if !quiet {
                println!(
                    "dev reload requested: {}",
                    affected.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn shell_quote_for_tmux_command(value: &Path) -> String {
    let raw = value.to_string_lossy();
    format!("'{}'", raw.replace('\'', "'\\''"))
}

fn ensure_tmux_available(tmux_bin: &str) -> Result<()> {
    let output = Command::new(tmux_bin)
        .arg("-V")
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if !output.status.success() {
        bail!(
            "tmux binary '{}' is not usable: {}",
            tmux_bin,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn tmux_session_exists(tmux_bin: &str, session: &str) -> bool {
    Command::new(tmux_bin)
        .args(["has-session", "-t", session])
        .status()
        .is_ok_and(|status| status.success())
}

fn run_tmux(tmux_bin: &str, args: &[&str]) -> Result<()> {
    let output = Command::new(tmux_bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "tmux command failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn run_tmux_capture(tmux_bin: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(tmux_bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }
    bail!(
        "tmux command failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn tmux_tail_command(path: &Path, lines: usize) -> String {
    format!("tail -n {lines} -F {}", shell_quote_for_tmux_command(path))
}

fn open_tmux_dashboard(
    record: &SubmissionRecord,
    tmux_bin: &str,
    session: Option<String>,
    no_attach: bool,
    lines: usize,
) -> Result<String> {
    if record.backend != SubmissionBackend::Local {
        bail!(
            "tmux only supports tracked local jobs; job {} uses {:?}",
            record.job_id,
            record.backend
        );
    }
    ensure_tmux_available(tmux_bin)?;
    if record.service_logs.is_empty() {
        bail!(
            "tracked job {} does not contain any service logs",
            record.job_id
        );
    }
    let session_name = session.unwrap_or_else(|| format!("hpc-compose-{}", record.job_id));
    if !tmux_session_exists(tmux_bin, &session_name) {
        let mut services = record.service_logs.iter();
        let (first_service, first_log) = services.next().expect("checked non-empty");
        let first_cmd = tmux_tail_command(first_log, lines);
        run_tmux(
            tmux_bin,
            &[
                "new-session",
                "-d",
                "-s",
                &session_name,
                "-n",
                "logs",
                &first_cmd,
            ],
        )?;
        run_tmux(
            tmux_bin,
            &[
                "select-pane",
                "-t",
                &format!("{session_name}:0.0"),
                "-T",
                first_service,
            ],
        )?;
        for (service, log_path) in services {
            let command = tmux_tail_command(log_path, lines);
            let pane_id = run_tmux_capture(
                tmux_bin,
                &[
                    "split-window",
                    "-t",
                    &format!("{session_name}:0"),
                    "-d",
                    "-P",
                    "-F",
                    "#{pane_id}",
                    &command,
                ],
            )?;
            let target = if pane_id.is_empty() {
                format!("{session_name}:0")
            } else {
                pane_id
            };
            run_tmux(tmux_bin, &["select-pane", "-t", &target, "-T", service])?;
        }
        run_tmux(
            tmux_bin,
            &["select-layout", "-t", &format!("{session_name}:0"), "tiled"],
        )?;
    }
    if !no_attach {
        run_tmux(tmux_bin, &["attach-session", "-t", &session_name])?;
    }
    Ok(session_name)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn tmux(
    context: ResolvedContext,
    job_id: Option<String>,
    session: Option<String>,
    tmux_bin: String,
    no_attach: bool,
    lines: usize,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let record = if let Some(job_id) = job_id {
        resolve_tracked_record(&context, Some(&job_id))?
            .with_context(|| format!("tracked job '{job_id}' was not found"))?
    } else {
        let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
        let prepared = prepare_local_launch(
            &context,
            script_out,
            flags,
            OutputFormat::Text,
            quiet,
            false,
            |_| Ok(()),
        )?;
        let outcome = start_prepared_local_launch(&prepared)?;
        if !quiet {
            print_local_launch_outcome(&prepared, &outcome)?;
        }
        outcome.record
    };
    let session_name = open_tmux_dashboard(&record, &tmux_bin, session, no_attach, lines)?;
    if no_attach && !quiet {
        println!("tmux session: {session_name}");
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct GerminateOutput<'a> {
    compose_file: &'a Path,
    script_path: &'a Path,
    cache_dir: &'a Path,
    dry_run: bool,
    job_id: Option<&'a str>,
    tracked_metadata_path: Option<PathBuf>,
    yaml_patch: Option<String>,
    report: Option<&'a hpc_compose::job::RightsizeReport>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn germinate(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    canary_time: String,
    metrics_interval: u64,
    pending_timeout: String,
    min_cpus: u32,
    min_mem: String,
    min_gpus: u32,
    flags: PrepareFlags,
    dry_run: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    if metrics_interval == 0 {
        bail!("germinate --metrics-interval must be at least 1");
    }
    if min_cpus == 0 {
        bail!("germinate --min-cpus must be at least 1");
    }
    if min_gpus == 0 {
        bail!("germinate --min-gpus must be at least 1");
    }
    if min_mem.trim().is_empty() {
        bail!("germinate --min-mem must not be empty");
    }
    parse_slurm_time_limit(&canary_time).context("germinate --canary-time is invalid")?;
    let pending_timeout_seconds = parse_log_since_duration(&pending_timeout)
        .context("germinate --pending-timeout is invalid")?;

    let file = context.compose_file.value.clone();
    let output_format = output::resolve_output_format(format, false);
    let effective_config =
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let original_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if original_plan.slurm.array.is_some() {
        bail!("germinate does not support x-slurm.array; submit one representative task instead");
    }
    ensure_batch_submission_supported(&original_plan, false, false)?;

    let canary_plan = minimized_canary_plan(
        &original_plan,
        &canary_time,
        metrics_interval,
        min_cpus,
        &min_mem,
        min_gpus,
    );
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let cluster_profile = load_discovered_cluster_profile(&context)?;

    if !no_preflight {
        let report = progress.run_checked_result(
            "Running canary preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &canary_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
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
            bail!("preflight failed; fix the reported errors before submitting a canary");
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&canary_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result("Preparing canary runtime artifacts", || {
            prepare_runtime_plan(
                &canary_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering canary submission script", || {
        render_script_with_options(
            &canary_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                cluster_profile,
            },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| default_canary_script_path(&file));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered canary script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        match output_format {
            OutputFormat::Text => {
                println!("  script: {}", script_path.display());
                println!("  cache:  {}", canary_plan.cache_dir.display());
                println!("dry run: skipping sbatch submission");
            }
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&GerminateOutput {
                        compose_file: &file,
                        script_path: &script_path,
                        cache_dir: &canary_plan.cache_dir,
                        dry_run: true,
                        job_id: None,
                        tracked_metadata_path: None,
                        yaml_patch: None,
                        report: None,
                    })
                    .context("failed to serialize germinate output")?
                );
            }
        }
        return Ok(());
    }

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Canary,
        service_name: None,
        command_override: None,
        requested_walltime: requested_walltime(&canary_plan),
        slurm_array: None,
        sweep: None,
        config_snapshot_yaml: Some(effective_config_yaml),
        cached_artifacts: tracked_cached_artifacts(&canary_plan),
    };
    let prepared = PreparedSlurmSubmission {
        file: file.clone(),
        submit_dir,
        script_path: script_path.clone(),
        runtime_plan: canary_plan.clone(),
        record_options,
        output_format,
    };
    let outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
    let Some((record, persisted)) = outcome.tracked_submission.as_ref() else {
        bail!("sbatch output did not include a numeric Slurm job id; cannot analyze canary usage");
    };
    if !persisted {
        bail!("canary submitted but tracking metadata could not be written; cannot analyze usage");
    }

    wait_for_canary_terminal(
        &file,
        &record.job_id,
        pending_timeout_seconds,
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value.clone(),
            sacct_bin: context.binaries.sacct.value.clone(),
        },
    )?;

    let mut report = build_rightsize_report(
        &original_plan,
        record,
        &StatsOptions {
            sstat_bin: context.binaries.sstat.value.clone(),
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            accounting: true,
        },
    )?;
    suppress_canary_walltime_recommendations(&mut report);
    let yaml_patch = recommendation_yaml_patch(&report);

    match output_format {
        OutputFormat::Text => {
            println!("canary job: {}", record.job_id);
            println!("rendered script: {}", script_path.display());
            println!("tracked metadata: {}", latest_record_path(record).display());
            output::print_rightsize_report(&report)?;
            println!();
            println!("{}", hpc_compose::term::styled_bold("suggested YAML patch"));
            if yaml_patch.trim().is_empty() {
                println!(
                    "No concrete YAML resource changes suggested from the available evidence."
                );
            } else {
                println!("{yaml_patch}");
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&GerminateOutput {
                    compose_file: &file,
                    script_path: &script_path,
                    cache_dir: &canary_plan.cache_dir,
                    dry_run: false,
                    job_id: Some(&record.job_id),
                    tracked_metadata_path: Some(latest_record_path(record)),
                    yaml_patch: Some(yaml_patch),
                    report: Some(&report),
                })
                .context("failed to serialize germinate output")?
            );
        }
    }
    Ok(())
}

fn default_canary_script_path(compose_file: &Path) -> PathBuf {
    let parent = compose_file.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose-canary.sbatch")
}

fn minimized_canary_plan(
    original: &RuntimePlan,
    canary_time: &str,
    metrics_interval: u64,
    min_cpus: u32,
    min_mem: &str,
    min_gpus: u32,
) -> RuntimePlan {
    let mut plan = original.clone();
    plan.name = format!("{}-canary", original.name);
    plan.slurm.time = Some(canary_time.to_string());
    plan.slurm.cpus_per_task = Some(min_cpus);
    plan.slurm.mem = Some(min_mem.to_string());
    if allocation_or_service_requests_gpus(original) {
        if plan.slurm.gpus.is_some() {
            plan.slurm.gpus = Some(min_gpus);
        }
        if plan.slurm.gpus_per_node.is_some() {
            plan.slurm.gpus_per_node = Some(min_gpus);
        }
        if plan.slurm.gpus_per_task.is_some() {
            plan.slurm.gpus_per_task = Some(min_gpus);
        }
        if let Some(gres) = &mut plan.slurm.gres {
            *gres = minimized_gpu_gres(gres, min_gpus);
        }
    }
    plan.slurm.metrics = Some(MetricsConfig {
        enabled: Some(true),
        interval_seconds: Some(metrics_interval),
        collectors: vec![MetricsCollector::Gpu, MetricsCollector::Slurm],
    });
    for service in &mut plan.ordered_services {
        if service.slurm.cpus_per_task.is_some() {
            service.slurm.cpus_per_task = Some(min_cpus);
        }
        if service.slurm.gpus.is_some() {
            service.slurm.gpus = Some(min_gpus);
        }
        if service.slurm.gpus_per_node.is_some() {
            service.slurm.gpus_per_node = Some(min_gpus);
        }
        if service.slurm.gpus_per_task.is_some() {
            service.slurm.gpus_per_task = Some(min_gpus);
        }
        if let Some(gres) = &mut service.slurm.gres {
            *gres = minimized_gpu_gres(gres, min_gpus);
        }
    }
    plan
}

fn minimized_gpu_gres(gres: &str, min_gpus: u32) -> String {
    gres.split(',')
        .map(|part| minimized_gpu_gres_part(part.trim(), min_gpus))
        .collect::<Vec<_>>()
        .join(",")
}

fn minimized_gpu_gres_part(part: &str, min_gpus: u32) -> String {
    let mut fields = part.split(':').collect::<Vec<_>>();
    let Some(resource) = fields.first().copied() else {
        return part.to_string();
    };
    if resource != "gpu" && !resource.ends_with("/gpu") {
        return part.to_string();
    }
    if fields
        .last()
        .is_some_and(|last| last.parse::<u32>().is_ok())
    {
        fields.pop();
        fields.push("");
        let mut minimized = fields.join(":");
        minimized.push_str(&min_gpus.to_string());
        return minimized;
    }
    part.to_string()
}

fn allocation_or_service_requests_gpus(plan: &RuntimePlan) -> bool {
    plan.slurm.gpus.unwrap_or(0) > 0
        || plan.slurm.gpus_per_node.unwrap_or(0) > 0
        || plan.slurm.gpus_per_task.unwrap_or(0) > 0
        || plan
            .slurm
            .gres
            .as_deref()
            .is_some_and(|gres| gres.contains("gpu"))
        || plan.ordered_services.iter().any(|service| {
            service.slurm.gpus.unwrap_or(0) > 0
                || service.slurm.gpus_per_node.unwrap_or(0) > 0
                || service.slurm.gpus_per_task.unwrap_or(0) > 0
                || service
                    .slurm
                    .gres
                    .as_deref()
                    .is_some_and(|gres| gres.contains("gpu"))
        })
}

fn wait_for_canary_terminal(
    spec_path: &Path,
    job_id: &str,
    timeout_seconds: u64,
    scheduler: &SchedulerOptions,
) -> Result<()> {
    let started = SystemTime::now();
    loop {
        let snapshot = build_status_snapshot(spec_path, Some(job_id), scheduler)
            .with_context(|| format!("failed to inspect canary job {job_id}"))?;
        if snapshot.scheduler.terminal {
            return Ok(());
        }
        let elapsed = started
            .elapsed()
            .map(|duration| duration.as_secs())
            .unwrap_or(timeout_seconds);
        if elapsed >= timeout_seconds {
            bail!(
                "canary job {job_id} did not reach a terminal scheduler state within {timeout_seconds}s; inspect the queue with `hpc-compose status --job-id {job_id}`"
            );
        }
        thread::sleep(Duration::from_secs(5));
    }
}

fn suppress_canary_walltime_recommendations(report: &mut hpc_compose::job::RightsizeReport) {
    let before = report.recommendations.len();
    report
        .recommendations
        .retain(|recommendation| recommendation.resource != "time");
    if report.recommendations.len() != before {
        report.notes.push(
            "walltime is observed from the canary but not down-sized from a one-minute probe"
                .to_string(),
        );
    }
}

fn recommendation_yaml_patch(report: &hpc_compose::job::RightsizeReport) -> String {
    let mut top_level = BTreeMap::<String, String>::new();
    let mut services = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut unknown = Vec::new();
    for recommendation in &report.recommendations {
        if let Some(key) = recommendation.target_path.strip_prefix("x-slurm.") {
            top_level.insert(key.to_string(), recommendation.suggested.clone());
        } else if let Some(rest) = recommendation.target_path.strip_prefix("services.") {
            if let Some((service, key)) = rest.split_once(".x-slurm.") {
                services
                    .entry(service.to_string())
                    .or_default()
                    .insert(key.to_string(), recommendation.suggested.clone());
            } else {
                unknown.push(recommendation);
            }
        } else {
            unknown.push(recommendation);
        }
    }
    let mut out = String::new();
    if !top_level.is_empty() {
        out.push_str("x-slurm:\n");
        for (key, value) in top_level {
            out.push_str(&format!("  {key}: {value}\n"));
        }
    }
    if !services.is_empty() {
        out.push_str("services:\n");
        for (service, values) in services {
            out.push_str(&format!("  {service}:\n"));
            out.push_str("    x-slurm:\n");
            for (key, value) in values {
                out.push_str(&format!("      {key}: {value}\n"));
            }
        }
    }
    for recommendation in unknown {
        out.push_str(&format!(
            "# {}: {}\n",
            recommendation.target_path, recommendation.suggested
        ));
    }
    out
}

#[derive(Debug, Serialize)]
struct RendezvousRegisterOutput {
    cache_dir: PathBuf,
    record_path: PathBuf,
    record: hpc_compose::rendezvous::RendezvousRecord,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn rendezvous_register(
    cache_dir: PathBuf,
    name: String,
    job_id: String,
    service: Option<String>,
    host: String,
    port: u16,
    protocol: String,
    path: Option<String>,
    ttl_seconds: u64,
    format: Option<OutputFormat>,
) -> Result<()> {
    let now = rendezvous::unix_timestamp_now();
    let record = rendezvous::build_record(
        &cache_dir,
        RendezvousRegisterRequest {
            name,
            job_id,
            service,
            host,
            port,
            protocol,
            path,
            ttl_seconds,
            metadata: BTreeMap::new(),
        },
        now,
    )?;
    let record_path = rendezvous::register(&cache_dir, &record)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("registered rendezvous: {}", record.name);
            println!("url: {}", record.url);
            println!("job id: {}", record.job_id);
            println!("record: {}", record_path.display());
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&RendezvousRegisterOutput {
                cache_dir,
                record_path,
                record,
            })
            .context("failed to serialize rendezvous register output")?
        ),
    }
    Ok(())
}

pub(crate) fn rendezvous_resolve(
    cache_dir: PathBuf,
    name: String,
    format: Option<OutputFormat>,
) -> Result<()> {
    let Some(record) = rendezvous::resolve(&cache_dir, &name, rendezvous::unix_timestamp_now())?
    else {
        bail!(
            "no live rendezvous record named '{}' found under {}",
            name,
            rendezvous::root_dir(&cache_dir).display()
        );
    };
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("name: {}", record.name);
            println!("url: {}", record.url);
            println!("host: {}", record.host);
            println!("port: {}", record.port);
            println!("job id: {}", record.job_id);
            if let Some(service) = &record.service {
                println!("service: {service}");
            }
            println!(
                "expires in: {}s",
                record.ttl_seconds.saturating_sub(
                    rendezvous::unix_timestamp_now().saturating_sub(record.registered_at)
                )
            );
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&record)
                .context("failed to serialize rendezvous resolve output")?
        ),
    }
    Ok(())
}

pub(crate) fn rendezvous_list(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
    let records = rendezvous::list(&cache_dir, rendezvous::unix_timestamp_now())?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            if records.is_empty() {
                println!(
                    "no live rendezvous records found under {}",
                    rendezvous::root_dir(&cache_dir).display()
                );
            } else {
                for record in records {
                    println!("{} {} job={}", record.name, record.url, record.job_id);
                }
            }
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&records)
                .context("failed to serialize rendezvous list output")?
        ),
    }
    Ok(())
}

pub(crate) fn rendezvous_prune(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
    let report = rendezvous::prune(&cache_dir, rendezvous::unix_timestamp_now())?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("removed {} rendezvous record(s)", report.removed.len());
            for path in &report.removed {
                println!("  {}", path.display());
            }
        }
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .context("failed to serialize rendezvous prune output")?
        ),
    }
    Ok(())
}

pub(crate) fn alloc(
    context: ResolvedContext,
    command: Vec<String>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "alloc does not support x-slurm.array; interactive allocations run one compose allocation"
        );
    }
    if runtime_plan.slurm.has_scheduler_dependency() {
        bail!("alloc does not support Slurm job dependencies");
    }
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet);
    let cluster_profile = load_discovered_cluster_profile(&context)?;

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
                        sbatch_bin: context.binaries.salloc.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before opening an allocation");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let bootstrap = allocation_bootstrap_script(&context, &runtime_plan, &submit_dir);
    let mut args = Vec::new();
    push_slurm_salloc_options(&mut args, &runtime_plan.slurm);
    args.push("bash".to_string());
    args.push("-lc".to_string());
    args.push(bootstrap);
    args.push("hpc-compose-alloc".to_string());
    args.extend(command);

    if !quiet {
        println!(
            "opening Slurm allocation with {}",
            context.binaries.salloc.value
        );
    }
    let status = Command::new(&context.binaries.salloc.value)
        .args(&args)
        .current_dir(&submit_dir)
        .status()
        .with_context(|| format!("failed to execute '{}'", context.binaries.salloc.value))?;
    if !status.success() {
        bail!("salloc failed with status {status}");
    }
    Ok(())
}

pub(crate) fn run_service(
    context: ResolvedContext,
    service_name: String,
    command: Vec<String>,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let file = context.compose_file.value.clone();
    let progress = ProgressReporter::new(!quiet);
    let active_allocation_job_id = active_allocation_job_id();
    let mut runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let Some(mut service) = runtime_plan
        .ordered_services
        .iter()
        .find(|candidate| candidate.name == service_name)
        .cloned()
    else {
        bail!(
            "service '{}' does not exist in {}",
            service_name,
            file.display()
        );
    };
    service.depends_on.clear();
    service.execution = ExecutionSpec::Exec(command.clone());
    runtime_plan.name = format!("{}-{}-run", runtime_plan.name, service_name);
    runtime_plan.slurm.resume = None;
    runtime_plan.ordered_services = vec![service];

    ensure_batch_submission_supported(&runtime_plan, true, false)?;

    let cluster_profile = load_discovered_cluster_profile(&context)?;

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
                        sbatch_bin: active_allocation_job_id
                            .as_ref()
                            .map(|_| context.binaries.srun.value.clone())
                            .unwrap_or_else(|| context.binaries.sbatch.value.clone()),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before running");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering run script", || {
        let script = render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                cluster_profile,
            },
        )?;
        Ok::<_, anyhow::Error>(if active_allocation_job_id.is_some() {
            strip_sbatch_directives(&script)
        } else {
            script
        })
    })?;
    let script_path = script_out.unwrap_or_else(|| default_run_script_path(&file, &service_name));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if let Some(job_id) = active_allocation_job_id.as_deref() {
        println!("using active Slurm allocation {job_id}");
        let record = build_submission_record_with_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            job_id,
            &SubmissionRecordBuildOptions {
                kind: SubmissionKind::Run,
                service_name: Some(service_name.clone()),
                command_override: Some(command.clone()),
                requested_walltime: requested_walltime(&runtime_plan),
                slurm_array: None,
                sweep: None,
                config_snapshot_yaml: None,
                cached_artifacts: tracked_cached_artifacts(&runtime_plan),
            },
        )?;
        write_submission_record(&record)
            .context("failed to persist tracking metadata for in-allocation run")?;
        output::print_submit_summary_box(
            &runtime_plan,
            &record.job_id,
            &script_path,
            Some(&latest_record_path(&record)),
        );
        let status = Command::new("bash")
            .arg(&script_path)
            .current_dir(&submit_dir)
            .status()
            .with_context(|| format!("failed to execute '{}'", script_path.display()))?;
        if !status.success() {
            bail!("in-allocation run failed with status {status}");
        }
        return Ok(());
    }

    let output_result = progress.run_result("Submitting run job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
            .args(sbatch_cli_args(&runtime_plan))
            .arg(&script_path)
            .output()
            .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
    })?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            String::from_utf8_lossy(&output_result.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout);
    print!("{stdout}");
    output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;

    let Some(job_id) = output::extract_job_id(stdout.trim()) else {
        println!(
            "note: sbatch output did not include a numeric Slurm job id, so this run is not trackable"
        );
        return Ok(());
    };

    let record = build_submission_record_with_options(
        &file,
        &submit_dir,
        &script_path,
        &runtime_plan,
        job_id,
        &SubmissionRecordBuildOptions {
            kind: SubmissionKind::Run,
            service_name: Some(service_name.clone()),
            command_override: Some(command),
            requested_walltime: requested_walltime(&runtime_plan),
            slurm_array: runtime_plan.slurm.array.clone(),
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        },
    )?;
    write_submission_record(&record)?;
    output::print_submit_summary_box(
        &runtime_plan,
        &record.job_id,
        &script_path,
        Some(&latest_record_path(&record)),
    );
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            Some(&service_name),
            100,
            WatchMode::Auto,
            HoldOnExit::Failure,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ephemeral(
    context: ResolvedContext,
    image: String,
    command: Vec<String>,
    resource_options: ResourceCliOptions,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    local: bool,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    if image.trim().is_empty() {
        bail!("run --image requires a non-empty image");
    }
    if command.is_empty() {
        bail!("run --image requires a command after --");
    }
    let file = context.cwd.join("hpc-compose-run.yaml");
    let progress = ProgressReporter::new(!quiet);
    let runtime_plan =
        build_ephemeral_runtime_plan(&context, image, command.clone(), &resource_options)?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    } else {
        ensure_batch_submission_supported(&runtime_plan, true, false)?;
    }

    let cluster_profile = if local {
        None
    } else {
        load_discovered_cluster_profile(&context)?
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
                        require_submit_tools: !local,
                        skip_prepare,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before running");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let local_job_id = local.then(generate_local_job_id);
    let script = progress.run_result("Rendering run script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script(&runtime_plan, job_id, &context.binaries.enroot.value)
        } else {
            render_script_with_options(
                &runtime_plan,
                &RenderOptions {
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    cluster_profile,
                },
            )
        }
    })?;
    let script_path =
        script_out.unwrap_or_else(|| default_ephemeral_run_script_path(&context.cwd, local));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Run,
        service_name: Some("run".to_string()),
        command_override: Some(command),
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: None,
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
    };

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
        print_local_launch_details(&record, &runtime_plan, &script_path);
        output::print_submit_summary_box(
            &runtime_plan,
            &record.job_id,
            &script_path,
            Some(&latest_record_path(&record)),
        );
        return output::finish_watch(
            &record,
            watch_with_fallback(
                &record,
                &SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
                Some("run"),
                100,
                WatchMode::Auto,
                HoldOnExit::Failure,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )?,
        );
    }

    let output_result = progress.run_result("Submitting run job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
            .args(sbatch_cli_args(&runtime_plan))
            .arg(&script_path)
            .output()
            .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
    })?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            String::from_utf8_lossy(&output_result.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout);
    print!("{stdout}");
    output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;

    let Some(job_id) = output::extract_job_id(stdout.trim()) else {
        println!(
            "note: sbatch output did not include a numeric Slurm job id, so this run is not trackable"
        );
        return Ok(());
    };

    let record = build_submission_record_with_options(
        &file,
        &submit_dir,
        &script_path,
        &runtime_plan,
        job_id,
        &record_options,
    )?;
    write_submission_record(&record)?;
    output::print_submit_summary_box(
        &runtime_plan,
        &record.job_id,
        &script_path,
        Some(&latest_record_path(&record)),
    );
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            Some("run"),
            100,
            WatchMode::Auto,
            HoldOnExit::Failure,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

pub(crate) fn shell(
    context: ResolvedContext,
    image: String,
    resource_options: ResourceCliOptions,
) -> Result<()> {
    if image.trim().is_empty() {
        bail!("shell --image requires a non-empty image");
    }
    let env_map = parse_env_entries(&resource_options.env)?;
    let mut slurm = slurm_from_resource_options("hpc-compose-shell", &resource_options)?;
    apply_resource_profile_defaults(&mut slurm, &context.resource_profiles)?;
    slurm.validate()?;
    ensure_batch_submission_supported(
        &RuntimePlan {
            name: "hpc-compose-shell".to_string(),
            cache_dir: context.cache_dir.value.clone(),
            runtime: RuntimeConfig::default(),
            slurm: slurm.clone(),
            ordered_services: Vec::new(),
        },
        false,
        false,
    )?;

    let mut args = Vec::new();
    push_slurm_srun_options(&mut args, &slurm);
    args.push("--pty".to_string());
    args.push(format!("--container-image={image}"));
    if !env_map.is_empty() {
        args.push(format!(
            "--container-env={}",
            env_map.keys().cloned().collect::<Vec<_>>().join(",")
        ));
    }
    args.push("bash".to_string());
    args.push("-l".to_string());

    let mut command = Command::new(&context.binaries.srun.value);
    command.args(&args);
    for (key, value) in env_map {
        command.env(key, value);
    }
    let status = command
        .status()
        .with_context(|| format!("failed to execute '{}'", context.binaries.srun.value))?;
    if !status.success() {
        bail!("srun failed with status {status}");
    }
    Ok(())
}

fn default_notebook_script_path(cwd: &Path, local: bool) -> PathBuf {
    if local {
        cwd.join("hpc-compose-notebook.local.sh")
    } else {
        cwd.join("hpc-compose-notebook.sbatch")
    }
}

/// Best-effort fully-qualified hostname of the current host, used as the SSH
/// jump host in the Jupyter tunnel hint. Returns `None` when it cannot be
/// determined so the hint degrades to a placeholder.
fn current_hostname() -> Option<String> {
    if let Some(name) = env::var_os("HOSTNAME")
        && !name.is_empty()
        && name.to_string_lossy() != "127.0.0.1"
    {
        return Some(name.to_string_lossy().into_owned());
    }
    Command::new("hostname")
        .arg("-f")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// Polls the notebook service log until *pattern* matches or *timeout* elapses.
///
/// Returns the full log text at match time so the caller can scrape the
/// connection URL. Reuses the same regex-on-log approach as
/// `readiness_util::wait_for_log`.
fn wait_for_notebook_log(log_path: &Path, pattern: &str, timeout: Duration) -> Result<String> {
    let regex = regex::Regex::new(pattern)
        .with_context(|| format!("notebook readiness pattern '{pattern}' is not a valid regex"))?;
    let started = Instant::now();
    loop {
        match fs::read_to_string(log_path) {
            Ok(content) if regex.is_match(&content) => return Ok(content),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => bail!("failed to read notebook log {}: {err}", log_path.display()),
        }
        if started.elapsed() >= timeout {
            bail!(
                "notebook did not become ready within {}s; inspect the log at {} and the tracked job with `hpc-compose status`",
                timeout.as_secs(),
                log_path.display()
            );
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn print_notebook_connection(connection: &notebook::NotebookConnection) {
    println!();
    println!("{}", term::styled_section_header("Notebook ready"));
    println!(
        "{}",
        term::styled_success(&format!("Open: {}", connection.url))
    );
    if let Some(hint) = connection.tunnel_hint.as_deref() {
        println!();
        println!("{}", hint);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn notebook(
    context: ResolvedContext,
    nb_args: NotebookArgs,
    resource_options: ResourceCliOptions,
    script_out: Option<PathBuf>,
    ready_timeout: Duration,
    follow: bool,
    dry_run: bool,
    flags: PrepareFlags,
    local: bool,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let preset = preset_for(nb_args.kind);
    let image = resolve_image(&nb_args, &preset)?;
    let token = nb_args.token.clone().unwrap_or_else(generate_token);
    let command = build_server_command(&nb_args, &token);
    let readiness = readiness_spec(&preset);
    let service = build_notebook_service_spec(&nb_args, &image, command.clone(), readiness);
    let job_name = format!("hpc-compose-notebook-{}", preset.kind.as_str());
    let runtime_plan =
        build_synthetic_service_plan(&context, &job_name, "notebook", service, &resource_options)?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let file = context.cwd.join(format!("{job_name}.yaml"));

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    } else {
        ensure_batch_submission_supported(&runtime_plan, false, local)?;
    }
    let cluster_profile = if local {
        None
    } else {
        load_discovered_cluster_profile(&context)?
    };

    let progress = ProgressReporter::new(!quiet);
    // --dry-run is a static preview: skip preflight and image preparation so
    // it works without a runtime backend or network access.
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
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before launching the notebook");
        }
    }

    if !dry_run && !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = progress.run_result("Preparing runtime artifacts", || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let local_job_id = local.then(generate_local_job_id);
    let script = progress.run_result("Rendering notebook script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script(&runtime_plan, job_id, &context.binaries.enroot.value)
        } else {
            render_script_with_options(
                &runtime_plan,
                &RenderOptions {
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    cluster_profile,
                },
            )
        }
    })?;
    let script_path =
        script_out.unwrap_or_else(|| default_notebook_script_path(&context.cwd, local));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        println!(
            "{}",
            term::styled_success(&format!(
                "rendered notebook launcher: {}",
                script_path.display()
            ))
        );
        return Ok(());
    }

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Notebook,
        service_name: Some("notebook".to_string()),
        command_override: Some(command),
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: None,
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
    };

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    // Submit -----------------------------------------------------------------
    let record = if local {
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
            .context("failed to persist tracking metadata for local notebook")?;
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
        print_local_launch_details(&record, &runtime_plan, &script_path);
        record
    } else {
        let output_result = progress.run_result("Submitting notebook job to Slurm", || {
            Command::new(&context.binaries.sbatch.value)
                .args(sbatch_cli_args(&runtime_plan))
                .arg(&script_path)
                .output()
                .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
        })?;
        if !output_result.status.success() {
            bail!(
                "sbatch failed: {}",
                String::from_utf8_lossy(&output_result.stderr).trim()
            );
        }
        let stdout = String::from_utf8_lossy(&output_result.stdout);
        print!("{stdout}");
        output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
        let Some(job_id) = output::extract_job_id(stdout.trim()) else {
            bail!(
                "sbatch output did not include a numeric Slurm job id; cannot track the notebook"
            );
        };
        let record = build_submission_record_with_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            job_id,
            &record_options,
        )?;
        write_submission_record(&record)?;
        // Wait until the allocation is RUNNING before polling the log.
        wait_for_job_start(&record, &scheduler_options, None)?;
        record
    };

    output::print_submit_summary_box(
        &runtime_plan,
        &record.job_id,
        &script_path,
        Some(&latest_record_path(&record)),
    );

    // Readiness gate --------------------------------------------------------
    let log_path = record
        .service_logs
        .get("notebook")
        .with_context(|| "tracked notebook service log path was not recorded")?;
    println!(
        "{}",
        term::styled_dim(&format!(
            "waiting for notebook to become ready (timeout {}s)...",
            ready_timeout.as_secs()
        ))
    );
    let log_text = wait_for_notebook_log(log_path, preset.readiness_log_pattern, ready_timeout)?;

    let (compute_node, login_node) = if local {
        (None, None)
    } else {
        let snapshot = build_status_snapshot(&file, Some(&record.job_id), &scheduler_options)?;
        let compute = snapshot
            .services
            .iter()
            .find(|row| row.service_name == "notebook")
            .and_then(|row| row.nodelist.clone())
            .and_then(|nodes| nodes.split(',').next().map(str::to_string));
        (compute, current_hostname())
    };
    let connection = build_connection(
        &nb_args,
        &preset,
        &token,
        &log_text,
        compute_node.as_deref(),
        login_node.as_deref(),
        local,
    )?;
    print_notebook_connection(&connection);
    println!(
        "{}",
        term::styled_dim(&format!(
            "manage with: `hpc-compose status -f {}` / `hpc-compose cancel -f {}`",
            file.display(),
            file.display()
        ))
    );

    if follow {
        return output::finish_watch(
            &record,
            watch_with_fallback(
                &record,
                &scheduler_options,
                Some("notebook"),
                100,
                WatchMode::Auto,
                HoldOnExit::Failure,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )?,
        );
    }
    Ok(())
}

pub(crate) fn status(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    json: bool,
    array: bool,
) -> Result<()> {
    let lookup_job_id = if array {
        job_id
            .as_deref()
            .and_then(|value| value.split_once('_').map(|(parent, _)| parent.to_string()))
            .or_else(|| job_id.clone())
    } else {
        job_id.clone()
    };
    let record = match lookup_job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, lookup_job_id.as_deref())?,
        None => None,
    };
    if lookup_job_id.is_some() && record.is_none() {
        bail!("{}", tracked_job_hint(lookup_job_id.as_deref()));
    }
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value,
        sacct_bin: context.binaries.sacct.value,
    };
    let mut snapshot = build_status_snapshot_with_array(
        compose_file,
        lookup_job_id.as_deref(),
        &scheduler_options,
        array,
    )?;
    if array && job_id.as_deref() != lookup_job_id.as_deref() {
        snapshot.array = Some(build_array_status_snapshot(
            &snapshot.record,
            job_id.as_deref(),
            &scheduler_options,
        )?);
    }
    match output::resolve_output_format(format, json) {
        OutputFormat::Text => {
            output::print_status_snapshot(&snapshot).context("failed to write status output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&snapshot)
                    .context("failed to serialize status output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn stats(
    context: ResolvedContext,
    job_id: Option<String>,
    json: bool,
    format: Option<StatsOutputFormat>,
    accounting: bool,
) -> Result<()> {
    let record = match job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, job_id.as_deref())?,
        None => None,
    };
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let snapshot = build_stats_snapshot(
        compose_file,
        job_id.as_deref(),
        &StatsOptions {
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            sstat_bin: context.binaries.sstat.value,
            accounting,
        },
    )?;
    match output::resolve_stats_output_format(format, json) {
        StatsOutputFormat::Text => {
            output::print_stats_snapshot(&snapshot).context("failed to write stats output")?;
        }
        StatsOutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&snapshot)
                    .context("failed to serialize stats output")?
            );
        }
        StatsOutputFormat::Csv => output::write_stats_snapshot_csv(&mut io::stdout(), &snapshot)
            .context("failed to write csv stats output")?,
        StatsOutputFormat::Jsonl => {
            output::write_stats_snapshot_jsonl(&mut io::stdout(), &snapshot)
                .context("failed to write jsonl stats output")?;
        }
    }
    Ok(())
}

pub(crate) fn metrics_probe(
    duration_seconds: u64,
    format: OutputFormat,
    compare_nvidia_smi: bool,
) -> Result<()> {
    if format != OutputFormat::Json {
        bail!("metrics-probe currently supports only --format json");
    }
    let options = MetricsProbeOptions {
        duration_seconds,
        compare_nvidia_smi,
    };
    validate_metrics_probe_options(options)?;
    let report = build_metrics_probe_report(options)?;
    println!("{}", serialize_metrics_probe_report(&report)?);
    Ok(())
}

pub(crate) fn score(
    context: ResolvedContext,
    job_id: Option<String>,
    json: bool,
    format: Option<OutputFormat>,
    pue: f64,
    gpu_tdp_w: f64,
    cpu_watts_per_core: f64,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &record.compose_file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )
        .with_context(|| {
            format!(
                "failed to load runtime plan for tracked job {} from {}",
                record.job_id,
                record.compose_file.display()
            )
        })?;
    let report = build_efficiency_score_report(
        &runtime_plan,
        &record,
        &EfficiencyScoreOptions {
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            sstat_bin: context.binaries.sstat.value,
            pue,
            gpu_tdp_w,
            cpu_watts_per_core,
        },
    )?;
    match output::resolve_output_format(format, json) {
        OutputFormat::Text => {
            output::print_efficiency_score_report(&report)
                .context("failed to write score output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize score output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn artifacts(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    json: bool,
    bundles: Vec<String>,
    tarball: bool,
) -> Result<()> {
    let report = export_artifacts(
        &context.compose_file.value,
        job_id.as_deref(),
        &ArtifactExportOptions {
            selected_bundles: bundles,
            tarball,
        },
    )?;
    match output::resolve_output_format(format, json) {
        OutputFormat::Text => {
            output::print_artifact_export_report(&report)
                .context("failed to write artifacts output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize artifacts output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn diff(
    context: ResolvedContext,
    job_id_1: String,
    job_id_2: String,
    format: Option<OutputFormat>,
) -> Result<()> {
    let left = resolve_tracked_record(&context, Some(&job_id_1))?
        .with_context(|| format!("tracked job '{job_id_1}' was not found"))?;
    let right = resolve_tracked_record(&context, Some(&job_id_2))?
        .with_context(|| format!("tracked job '{job_id_2}' was not found"))?;
    let report = build_job_diff_report(
        &left,
        &right,
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value,
            sacct_bin: context.binaries.sacct.value,
        },
    );
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output::print_job_diff_report(&report).context("failed to write diff output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).context("failed to serialize diff output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn logs(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    follow: bool,
    lines: usize,
    grep: Option<String>,
    since: Option<String>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let since_seconds = since.as_deref().map(parse_log_since_duration).transpose()?;
    print_logs(
        &record,
        &hpc_compose::job::LogPrintOptions {
            service,
            lines,
            follow,
            grep,
            since_seconds,
        },
    )
}

pub(crate) fn ps(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = match job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, job_id.as_deref())?,
        None => None,
    };
    if job_id.is_some() && record.is_none() {
        bail!("{}", tracked_job_hint(job_id.as_deref()));
    }
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let snapshot = build_ps_snapshot(
        compose_file,
        job_id.as_deref(),
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value,
            sacct_bin: context.binaries.sacct.value,
        },
    )?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output::print_ps_snapshot(&snapshot).context("failed to write ps output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&snapshot).context("failed to serialize ps output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn watch(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    lines: usize,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            service.as_deref(),
            lines,
            watch_mode,
            hold_on_exit,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

pub(crate) fn replay(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    speed: f64,
    lines: usize,
    watch_mode: WatchMode,
    format: Option<OutputFormat>,
) -> Result<()> {
    if !speed.is_finite() || speed <= 0.0 {
        bail!("replay --speed must be a positive finite number");
    }
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let report = build_replay_report(&record, service.as_deref())?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize replay output")?
            );
            Ok(())
        }
        OutputFormat::Text => match watch_mode {
            WatchMode::Line => print_replay_summary(&report),
            WatchMode::Tui => watch_ui::run_replay_ui(
                &report,
                service.as_deref(),
                lines,
                speed,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )
            .context("replay UI requested with --watch-mode tui but could not be started"),
            WatchMode::Auto => {
                if watch_ui::can_use_watch_ui() {
                    match watch_ui::run_replay_ui(
                        &report,
                        service.as_deref(),
                        lines,
                        speed,
                        watch_ui::WatchPrefs::resolve(&context.watch),
                    ) {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            let _ = writeln!(
                                io::stderr(),
                                "warning: replay UI unavailable ({err}); printing static replay summary"
                            );
                            let _ = io::stderr().flush();
                            print_replay_summary(&report)
                        }
                    }
                } else {
                    print_replay_summary(&report)
                }
            }
        },
    }
}

fn print_replay_summary(report: &hpc_compose::job::ReplayReport) -> Result<()> {
    let mut stdout = io::stdout();
    writeln!(
        stdout,
        "hpc-compose replay | job {} | {}",
        report.job_id, report.fidelity
    )
    .context("failed to write replay output")?;
    if let (Some(start), Some(end)) = (report.timeline_start_unix, report.timeline_end_unix) {
        writeln!(stdout, "timeline: {start}..{end}")?;
    }
    writeln!(stdout, "events: {}", report.events.len())?;
    if !report.notes.is_empty() {
        writeln!(stdout, "notes:")?;
        for note in &report.notes {
            writeln!(stdout, "  - {note}")?;
        }
    }
    writeln!(stdout, "artifacts:")?;
    for path in &report.artifacts.runtime_roots {
        writeln!(stdout, "  runtime: {}", path.display())?;
    }
    for path in &report.artifacts.service_exit_dirs {
        writeln!(stdout, "  service-exits: {}", path.display())?;
    }
    for path in &report.artifacts.metrics_dirs {
        writeln!(stdout, "  metrics: {}", path.display())?;
    }
    writeln!(stdout, "timeline:")?;
    for frame in &report.frames {
        let event = &frame.event;
        let service = event
            .service
            .as_deref()
            .map(|service| format!(" service={service}"))
            .unwrap_or_default();
        let exit = event
            .exit_code
            .map(|code| format!(" exit={code}"))
            .unwrap_or_default();
        let detail = event
            .detail
            .as_deref()
            .map(|detail| format!(" ({detail})"))
            .unwrap_or_default();
        let metrics = frame
            .metrics_line
            .as_deref()
            .map(|line| format!(" | metrics: {line}"))
            .unwrap_or_default();
        writeln!(
            stdout,
            "  {} {}{}{}{}{}",
            event.at_unix,
            replay_event_kind_label(event.kind),
            service,
            exit,
            detail,
            metrics
        )?;
    }
    stdout.flush().context("failed to flush replay output")
}

fn replay_event_kind_label(kind: hpc_compose::job::ReplayEventKind) -> &'static str {
    match kind {
        hpc_compose::job::ReplayEventKind::AttemptStart => "attempt_start",
        hpc_compose::job::ReplayEventKind::ServiceStart => "service_start",
        hpc_compose::job::ReplayEventKind::MetricsSample => "metrics_sample",
        hpc_compose::job::ReplayEventKind::ServiceExit => "service_exit",
        hpc_compose::job::ReplayEventKind::FinalSnapshot => "final_snapshot",
    }
}

fn tracked_job_hint(job_id: Option<&str>) -> String {
    match job_id {
        Some(job_id) => format!(
            "tracked job '{job_id}' was not found from this repository; run `hpc-compose jobs list` to inspect known tracked jobs"
        ),
        None => "no tracked job was found for the active compose file; run `hpc-compose jobs list` to inspect known tracked jobs".to_string(),
    }
}

#[derive(Debug, Serialize)]
struct DebugLogTail {
    service_name: Option<String>,
    path: PathBuf,
    present: bool,
    lines: Vec<String>,
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct DebugSummary {
    scheduler_state: Option<String>,
    failed_service: Option<String>,
    exit_code: Option<i64>,
    log_path: Option<PathBuf>,
    next_command: String,
}

#[derive(Debug, Serialize)]
struct DebugReport {
    tracked: bool,
    compose_file: PathBuf,
    job_id: Option<String>,
    summary: DebugSummary,
    status: Option<hpc_compose::job::StatusSnapshot>,
    ps: Option<hpc_compose::job::PsSnapshot>,
    batch_log: Option<DebugLogTail>,
    service_logs: Vec<DebugLogTail>,
    notes: Vec<String>,
    recommendation: String,
    preflight: Option<serde_json::Value>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn debug(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    lines: usize,
    run_preflight_again: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    let mut notes = Vec::new();
    let mut preflight_json = None;
    let mut preflight_failed = false;

    let record = match resolve_tracked_record(&context, job_id.as_deref()) {
        Ok(record) => record,
        Err(err) => {
            notes.push(format!("{err:#}"));
            None
        }
    };
    let preflight_context = if run_preflight_again {
        match record.as_ref() {
            Some(record) if record.compose_file != context.compose_file.value => Some(
                resolve_context_for_tracked_compose(&context, record.compose_file.clone())?,
            ),
            Some(_) => Some(context.clone()),
            None => None,
        }
    } else {
        None
    };

    if run_preflight_again {
        let preflight_context = match preflight_context.as_ref() {
            Some(preflight_context) => preflight_context,
            None => &context,
        };
        let runtime_plan =
            output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
                &preflight_context.compose_file.value,
                &preflight_context.interpolation_vars,
                Some(&preflight_context.cache_dir.value),
                &preflight_context.resource_profiles,
            )?;
        let cluster_profile = load_discovered_cluster_profile(preflight_context)?;
        let report = run_preflight(
            &runtime_plan,
            &PreflightOptions {
                enroot_bin: preflight_context.binaries.enroot.value.clone(),
                apptainer_bin: preflight_context.binaries.apptainer.value.clone(),
                singularity_bin: preflight_context.binaries.singularity.value.clone(),
                sbatch_bin: preflight_context.binaries.sbatch.value.clone(),
                srun_bin: preflight_context.binaries.srun.value.clone(),
                scontrol_bin: preflight_context.binaries.scontrol.value.clone(),
                require_submit_tools: true,
                skip_prepare: false,
                cluster_profile,
            },
        );
        preflight_failed = report.has_errors();
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, true);
        }
        preflight_json = Some(
            serde_json::to_value(report.grouped())
                .context("failed to serialize preflight report")?,
        );
    }

    let Some(record) = record else {
        let report = DebugReport {
            tracked: false,
            compose_file: context.compose_file.value.clone(),
            job_id,
            summary: DebugSummary {
                scheduler_state: None,
                failed_service: None,
                exit_code: None,
                log_path: None,
                next_command: format!("hpc-compose up -f {}", context.compose_file.value.display()),
            },
            status: None,
            ps: None,
            batch_log: None,
            service_logs: Vec::new(),
            notes,
            recommendation: format!(
                "No tracked run was found. Run `hpc-compose plan -f {}` before `hpc-compose up -f {}`.",
                context.compose_file.value.display(),
                context.compose_file.value.display()
            ),
            preflight: preflight_json,
        };
        emit_debug_report(&report, output_format)?;
        if preflight_failed {
            bail!("preflight failed");
        }
        return Ok(());
    };
    let debug_compose_file = record.compose_file.clone();

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let status_snapshot = build_status_snapshot(
        &debug_compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )?;
    let ps_snapshot = build_ps_snapshot(
        &debug_compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )?;
    let batch_log = DebugLogTail {
        service_name: None,
        path: status_snapshot.batch_log.path.clone(),
        present: status_snapshot.batch_log.present,
        lines: tail_file_lines(&status_snapshot.batch_log.path, lines)?,
        note: (!status_snapshot.batch_log.present).then_some(
            "batch log is missing; the job may not have started or the scheduler wrote elsewhere"
                .to_string(),
        ),
    };
    let selected_services = if let Some(service_name) = service.as_deref() {
        let matching = status_snapshot
            .services
            .iter()
            .filter(|entry| entry.service_name == service_name)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            bail!(
                "service '{}' does not exist in tracked job {}",
                service_name,
                record.job_id
            );
        }
        matching
    } else {
        status_snapshot.services.iter().collect::<Vec<_>>()
    };
    let mut service_logs = Vec::with_capacity(selected_services.len());
    for service in selected_services {
        service_logs.push(DebugLogTail {
            service_name: Some(service.service_name.clone()),
            path: service.path.clone(),
            present: service.present,
            lines: tail_file_lines(&service.path, lines)?,
            note: (!service.present).then_some(
                "service log is missing; check the batch log for launch-time failures".to_string(),
            ),
        });
    }
    let recommendation = debug_recommendation(&debug_compose_file, &status_snapshot);
    let summary = build_debug_summary(
        &debug_compose_file,
        &record.job_id,
        &status_snapshot,
        &recommendation,
    );
    let report = DebugReport {
        tracked: true,
        compose_file: debug_compose_file,
        job_id: Some(record.job_id.clone()),
        summary,
        status: Some(status_snapshot),
        ps: Some(ps_snapshot),
        batch_log: Some(batch_log),
        service_logs,
        notes,
        recommendation,
        preflight: preflight_json,
    };
    emit_debug_report(&report, output_format)?;
    if preflight_failed {
        bail!("preflight failed");
    }
    Ok(())
}

fn resolve_context_for_tracked_compose(
    context: &ResolvedContext,
    compose_file: PathBuf,
) -> Result<ResolvedContext> {
    resolve(&ResolveRequest {
        cwd: context.cwd.clone(),
        profile: context.selected_profile.clone(),
        settings_file: context.settings_path.clone(),
        compose_file_override: Some(compose_file),
        binary_overrides: resolved_binary_overrides(context),
    })
}

fn resolved_binary_overrides(context: &ResolvedContext) -> BinaryOverrides {
    BinaryOverrides {
        enroot: Some(context.binaries.enroot.value.clone()),
        apptainer: Some(context.binaries.apptainer.value.clone()),
        singularity: Some(context.binaries.singularity.value.clone()),
        salloc: Some(context.binaries.salloc.value.clone()),
        sbatch: Some(context.binaries.sbatch.value.clone()),
        srun: Some(context.binaries.srun.value.clone()),
        scontrol: Some(context.binaries.scontrol.value.clone()),
        sinfo: Some(context.binaries.sinfo.value.clone()),
        squeue: Some(context.binaries.squeue.value.clone()),
        sacct: Some(context.binaries.sacct.value.clone()),
        sstat: Some(context.binaries.sstat.value.clone()),
        scancel: Some(context.binaries.scancel.value.clone()),
        sshare: Some(context.binaries.sshare.value.clone()),
        sprio: Some(context.binaries.sprio.value.clone()),
    }
}

fn build_debug_summary(
    compose_file: &Path,
    job_id: &str,
    status: &hpc_compose::job::StatusSnapshot,
    recommendation: &str,
) -> DebugSummary {
    let failed = status.services.iter().find(|service| {
        service.status.as_deref() == Some("failed")
            || service.last_exit_code.is_some_and(|code| code != 0)
    });
    DebugSummary {
        scheduler_state: Some(status.scheduler.state.clone()),
        failed_service: failed.map(|service| service.service_name.clone()),
        exit_code: failed.and_then(|service| service.last_exit_code.map(i64::from)),
        log_path: failed.map(|service| service.path.clone()).or_else(|| {
            status
                .batch_log
                .present
                .then(|| status.batch_log.path.clone())
        }),
        next_command: if status.scheduler.failed {
            format!(
                "hpc-compose debug -f {} --job-id {} --preflight",
                compose_file.display(),
                job_id
            )
        } else {
            recommendation.to_string()
        },
    }
}

fn emit_debug_report(report: &DebugReport, output_format: OutputFormat) -> Result<()> {
    match output_format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(report).context("failed to serialize debug output")?
            );
        }
        OutputFormat::Text => {
            println!(
                "{}",
                hpc_compose::term::styled_label(
                    "compose file",
                    &report.compose_file.display().to_string()
                )
            );
            if !report.tracked {
                println!("tracked job: none");
                print_debug_summary(&report.summary);
                for note in &report.notes {
                    println!("note: {note}");
                }
                println!("recommendation: {}", report.recommendation);
                return Ok(());
            }
            print_debug_summary(&report.summary);
            if let Some(status) = report.status.as_ref() {
                output::print_status_snapshot(status)
                    .context("failed to write debug status output")?;
            }
            if let Some(ps) = report.ps.as_ref() {
                println!(
                    "{}",
                    hpc_compose::term::styled_section_header("Per-service state:")
                );
                output::print_ps_snapshot(ps).context("failed to write debug ps output")?;
            }
            if let Some(batch_log) = report.batch_log.as_ref() {
                print_debug_log_tail("Batch log", batch_log);
            }
            if !report.service_logs.is_empty() {
                println!(
                    "{}",
                    hpc_compose::term::styled_section_header("Service log tails:")
                );
                for log in &report.service_logs {
                    let label = log.service_name.as_deref().unwrap_or("service");
                    print_debug_log_tail(label, log);
                }
            }
            for note in &report.notes {
                println!("note: {note}");
            }
            println!("recommendation: {}", report.recommendation);
        }
    }
    Ok(())
}

fn print_debug_summary(summary: &DebugSummary) {
    println!(
        "{}",
        hpc_compose::term::styled_section_header("Debug summary:")
    );
    if let Some(state) = summary.scheduler_state.as_deref() {
        println!("  scheduler state: {state}");
    }
    if let Some(service) = summary.failed_service.as_deref() {
        println!("  failed service: {service}");
    }
    if let Some(exit_code) = summary.exit_code {
        println!("  exit code: {exit_code}");
    }
    if let Some(path) = summary.log_path.as_ref() {
        println!("  relevant log: {}", path.display());
    }
    println!("  next command: {}", summary.next_command);
}

fn print_debug_log_tail(label: &str, log: &DebugLogTail) {
    println!(
        "{} {} (present: {})",
        hpc_compose::term::styled_section_header(label),
        log.path.display(),
        if log.present { "yes" } else { "no" }
    );
    if let Some(note) = &log.note {
        println!("  note: {note}");
    }
    if log.lines.is_empty() {
        println!("  <no log lines>");
    } else {
        for line in &log.lines {
            println!("  {line}");
        }
    }
}

fn tail_file_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut collected = raw.lines().map(ToString::to_string).collect::<Vec<_>>();
    if collected.len() > lines {
        collected.drain(0..(collected.len() - lines));
    }
    Ok(collected)
}

fn debug_recommendation(
    compose_file: &Path,
    snapshot: &hpc_compose::job::StatusSnapshot,
) -> String {
    if snapshot.scheduler.failed {
        format!(
            "Run `hpc-compose debug -f {} --preflight` if preflight has not been rerun, then inspect the batch log above.",
            compose_file.display()
        )
    } else if snapshot.scheduler.terminal {
        format!(
            "The tracked job is terminal. Use `hpc-compose artifacts -f {}` if artifacts are configured.",
            compose_file.display()
        )
    } else {
        format!(
            "The tracked job is still active. Use `hpc-compose watch -f {}` or `hpc-compose logs -f {} --follow`.",
            compose_file.display(),
            compose_file.display()
        )
    }
}

pub(crate) fn cancel(
    context: ResolvedContext,
    job_id: Option<String>,
    purge_cache: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?;
    let resolved_job_id = record
        .as_ref()
        .map(|record| record.job_id.clone())
        .or(job_id)
        .context("missing job id for cancel")?;
    let cache_paths = if purge_cache {
        cached_artifacts_for_teardown(record.as_ref())?
    } else {
        Vec::new()
    };

    if record
        .as_ref()
        .is_some_and(|record| record.backend == SubmissionBackend::Local)
    {
        let record = record.as_ref().expect("checked above");
        let cancelled = if let Some(pid) = read_local_supervisor_pid(record)? {
            kill_pid_if_running(pid)
                .with_context(|| format!("failed to cancel local job {resolved_job_id}"))?
        } else {
            false
        };
        remove_submission_record(record)?;
        let purged_cache_paths = if purge_cache {
            purge_cached_artifacts(&cache_paths)?
        } else {
            Vec::new()
        };
        return match output::resolve_output_format(format, false) {
            OutputFormat::Text => {
                if cancelled {
                    println!("cancelled job: {resolved_job_id}");
                } else {
                    println!("local job is not running: {resolved_job_id}");
                }
                println!(
                    "removed tracked metadata: {}",
                    latest_record_path(record).display()
                );
                for path in &purged_cache_paths {
                    println!("purged cache artifact: {}", path.display());
                }
                Ok(())
            }
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output::CancelOutput {
                        job_id: resolved_job_id,
                        cancelled,
                        command_stdout: None,
                        tracking_removed: Some(true),
                        purged_cache_paths,
                    })
                    .context("failed to serialize cancel output")?
                );
                Ok(())
            }
        };
    }

    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output::cancel_job(&resolved_job_id, &context.binaries.scancel.value)?;
            let tracking_removed = if let Some(record) = record.as_ref() {
                remove_submission_record(record)?;
                println!(
                    "removed tracked metadata: {}",
                    latest_record_path(record).display()
                );
                true
            } else {
                false
            };
            let purged_cache_paths = if purge_cache {
                purge_cached_artifacts(&cache_paths)?
            } else {
                Vec::new()
            };
            for path in &purged_cache_paths {
                println!("purged cache artifact: {}", path.display());
            }
            if !tracking_removed {
                println!("note: no tracked metadata was found for job {resolved_job_id}");
            }
            Ok(())
        }
        OutputFormat::Json => {
            let output = Command::new(&context.binaries.scancel.value)
                .arg(&resolved_job_id)
                .output()
                .context(format!(
                    "failed to execute '{}'",
                    context.binaries.scancel.value
                ))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let detail = if !stderr.is_empty() { stderr } else { stdout };
                if detail.is_empty() {
                    bail!("scancel failed for job {resolved_job_id}");
                }
                bail!("scancel failed for job {resolved_job_id}: {detail}");
            }
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let tracking_removed = if let Some(record) = record.as_ref() {
                remove_submission_record(record)?;
                Some(true)
            } else {
                Some(false)
            };
            let purged_cache_paths = if purge_cache {
                purge_cached_artifacts(&cache_paths)?
            } else {
                Vec::new()
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&output::CancelOutput {
                    job_id: resolved_job_id,
                    cancelled: true,
                    command_stdout: (!stdout.is_empty()).then_some(stdout),
                    tracking_removed,
                    purged_cache_paths,
                })
                .context("failed to serialize cancel output")?
            );
            Ok(())
        }
    }
}

pub(crate) fn jobs_list(disk_usage: bool, format: Option<OutputFormat>) -> Result<()> {
    let cwd = env::current_dir().context("failed to determine current working directory")?;
    let report = scan_job_inventory(&cwd, disk_usage)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output::print_job_inventory_scan(&report, disk_usage)
                .context("failed to write jobs list output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize jobs list output")?
            );
        }
    }
    Ok(())
}

const DEFAULT_SWEEP_MAX_TRIALS: usize = 100;

#[derive(Debug, Serialize)]
struct SweepSubmitOutput<'a> {
    dry_run: bool,
    manifest_path: Option<PathBuf>,
    manifest: &'a SweepManifest,
}

#[derive(Debug, Serialize)]
struct SweepStatusOutput {
    sweep_id: String,
    compose_file: PathBuf,
    submitted_at: u64,
    summary: BTreeMap<String, usize>,
    trials: Vec<SweepStatusTrialOutput>,
}

#[derive(Debug, Serialize)]
struct SweepStatusTrialOutput {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    job_id: Option<String>,
    status: String,
    scheduler_state: Option<String>,
    record_path: Option<PathBuf>,
    submit_error: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct SweepListOutput {
    compose_file: PathBuf,
    sweeps: Vec<SweepManifest>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sweep_submit(
    context: ResolvedContext,
    dry_run: bool,
    max_trials: Option<usize>,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let sweep = ComposeSpec::load_sweep(&file)?.with_context(|| {
        format!(
            "{} does not contain a top-level sweep block",
            file.display()
        )
    })?;
    let sweep_id = generate_sweep_id();
    let max_trials = max_trials.unwrap_or(DEFAULT_SWEEP_MAX_TRIALS);
    let expansion = expand_sweep_with_limit(&sweep, &sweep_id, Some(max_trials))?;

    let output_format = output::resolve_output_format(format, false);
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let manifest_path = sweep_manifest_path_for(&file, &sweep_id);
    let sweep_root = manifest_path
        .parent()
        .context("sweep manifest path has no parent")?
        .to_path_buf();
    let submitted_at = unix_timestamp_now_for_command();
    let mut manifest = SweepManifest {
        schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
        sweep_id: sweep_id.clone(),
        compose_file: file.clone(),
        submitted_at,
        matrix: expansion.matrix.clone(),
        seed: expansion.seed.clone(),
        total_combinations: expansion.total_combinations,
        objective: sweep.objective.clone(),
        best_trial: None,
        stopped_at: None,
        stop_reason: None,
        trials: expansion
            .trials
            .iter()
            .map(|trial| SweepManifestTrial {
                trial_id: trial.trial_id.clone(),
                index: trial.index,
                variables: trial.variables.clone(),
                script_path: sweep_root.join(format!("{}.sbatch", trial.trial_id)),
                job_id: None,
                record_path: None,
                submitted_at: None,
                submit_error: None,
                objective: None,
                objective_error: None,
                observed_at: None,
            })
            .collect(),
    };

    if dry_run {
        let cluster_profile = load_discovered_cluster_profile(&context)?;
        for trial in &expansion.trials {
            validate_sweep_trial_plan(&context, trial, &sweep_id, cluster_profile.clone())?;
        }
        print_sweep_submit_output(output_format, true, None, &manifest)?;
        return Ok(());
    }

    write_sweep_manifest(&manifest).context("failed to persist initial sweep manifest")?;
    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);

    for (index, trial) in expansion.trials.iter().enumerate() {
        let result = submit_sweep_trial(
            &context,
            trial,
            &sweep_id,
            &submit_dir,
            &manifest.trials[index].script_path,
            skip_prepare,
            force_rebuild,
            no_preflight,
            cluster_profile.clone(),
            &progress,
            output_format,
            quiet,
        );
        match result {
            Ok(record) => {
                let record_path = latest_record_path(&record);
                manifest.trials[index].job_id = Some(record.job_id.clone());
                manifest.trials[index].record_path = Some(record_path);
                manifest.trials[index].submitted_at = Some(record.submitted_at);
                write_sweep_manifest(&manifest)
                    .context("failed to persist sweep manifest after trial submission")?;
                if output_format == OutputFormat::Text {
                    println!(
                        "submitted {} job {} ({})",
                        trial.trial_id,
                        record.job_id,
                        format_sweep_variables(&trial.variables)
                    );
                }
            }
            Err(err) => {
                manifest.trials[index].submit_error = Some(err.to_string());
                write_sweep_manifest(&manifest)
                    .context("failed to persist sweep manifest after trial failure")?;
                return Err(err.context(format!("sweep trial {} failed", trial.trial_id)));
            }
        }
    }

    print_sweep_submit_output(output_format, false, Some(manifest_path), &manifest)
}

fn validate_sweep_trial_plan(
    context: &ResolvedContext,
    trial: &SweepExpansionTrial,
    sweep_id: &str,
    cluster_profile: Option<hpc_compose::cluster::ClusterProfile>,
) -> Result<()> {
    let vars = sweep_interpolation_vars(context, sweep_id, trial);
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "sweep submit does not support x-slurm.array; each sweep trial is already a separate allocation"
        );
    }
    render_script_with_options(
        &runtime_plan,
        &RenderOptions {
            apptainer_bin: context.binaries.apptainer.value.clone(),
            singularity_bin: context.binaries.singularity.value.clone(),
            cluster_profile,
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn submit_sweep_trial(
    context: &ResolvedContext,
    trial: &SweepExpansionTrial,
    sweep_id: &str,
    submit_dir: &Path,
    script_path: &Path,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    cluster_profile: Option<hpc_compose::cluster::ClusterProfile>,
    progress: &ProgressReporter,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<SubmissionRecord> {
    let vars = sweep_interpolation_vars(context, sweep_id, trial);
    let file = context.compose_file.value.clone();
    let effective_config =
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "sweep submit does not support x-slurm.array; each sweep trial is already a separate allocation"
        );
    }

    if !no_preflight {
        let report = progress.run_checked_result(
            format!("Running preflight checks for {}", trial.trial_id),
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
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed for sweep trial {}", trial.trial_id);
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result(format!("Preparing {}", trial.trial_id), || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep: false,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result(format!("Rendering {}", trial.trial_id), || {
        render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                cluster_profile,
            },
        )
    })?;
    fs::write(script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    let prepared = PreparedSlurmSubmission {
        file,
        submit_dir: submit_dir.to_path_buf(),
        script_path: script_path.to_path_buf(),
        runtime_plan: runtime_plan.clone(),
        record_options: SubmissionRecordBuildOptions {
            kind: SubmissionKind::SweepTrial,
            service_name: None,
            command_override: None,
            requested_walltime: requested_walltime(&runtime_plan),
            slurm_array: None,
            sweep: Some(SweepTrialMetadata {
                sweep_id: sweep_id.to_string(),
                trial_id: trial.trial_id.clone(),
                trial_index: trial.index,
                variables: trial.variables.clone(),
            }),
            config_snapshot_yaml: Some(effective_config_yaml),
            cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        },
        output_format,
    };
    let outcome = submit_prepared_slurm_submission(context, &prepared, progress)?;
    let Some((record, persisted)) = outcome.tracked_submission else {
        bail!(
            "sbatch output for sweep trial {} did not include a numeric Slurm job id",
            trial.trial_id
        );
    };
    if !persisted {
        bail!(
            "tracking metadata could not be written for sweep trial {} job {}",
            trial.trial_id,
            record.job_id
        );
    }
    Ok(record)
}

fn sweep_interpolation_vars(
    context: &ResolvedContext,
    sweep_id: &str,
    trial: &SweepExpansionTrial,
) -> BTreeMap<String, String> {
    let mut vars = context.interpolation_vars.clone();
    vars.extend(interpolation_vars_for_sweep_trial(sweep_id, trial));
    vars
}

fn print_sweep_submit_output(
    output_format: OutputFormat,
    dry_run: bool,
    manifest_path: Option<PathBuf>,
    manifest: &SweepManifest,
) -> Result<()> {
    match output_format {
        OutputFormat::Text => {
            println!("sweep: {}", manifest.sweep_id);
            println!("trials: {}", manifest.trials.len());
            if let Some(seed) = &manifest.seed {
                println!("seed: {seed}");
            }
            if dry_run {
                println!("dry run: no scripts written and no jobs submitted");
            } else if let Some(path) = &manifest_path {
                println!("manifest: {}", path.display());
            }
            for trial in &manifest.trials {
                let status = trial
                    .job_id
                    .as_deref()
                    .or(trial.submit_error.as_deref())
                    .unwrap_or("pending submit");
                println!(
                    "  {} {} {}",
                    trial.trial_id,
                    status,
                    format_sweep_variables(&trial.variables)
                );
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&SweepSubmitOutput {
                    dry_run,
                    manifest_path,
                    manifest,
                })
                .context("failed to serialize sweep submit output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn sweep_status(
    context: ResolvedContext,
    sweep_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
    let options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value,
        sacct_bin: context.binaries.sacct.value,
    };
    let mut summary = BTreeMap::new();
    let trials = manifest
        .trials
        .iter()
        .map(|trial| {
            let output = status_for_sweep_trial(&manifest, trial, &options);
            *summary.entry(output.status.clone()).or_insert(0) += 1;
            output
        })
        .collect::<Vec<_>>();
    let report = SweepStatusOutput {
        sweep_id: manifest.sweep_id,
        compose_file: manifest.compose_file,
        submitted_at: manifest.submitted_at,
        summary,
        trials,
    };
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => print_sweep_status_output(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize sweep status output")?
            );
            Ok(())
        }
    }
}

fn status_for_sweep_trial(
    manifest: &SweepManifest,
    trial: &SweepManifestTrial,
    options: &SchedulerOptions,
) -> SweepStatusTrialOutput {
    if let Some(error) = &trial.submit_error {
        return SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: trial.job_id.clone(),
            status: "submit_failed".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: Some(error.clone()),
            detail: None,
        };
    }
    let Some(job_id) = trial.job_id.as_deref() else {
        return SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: None,
            status: "unknown".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: Some("trial has no recorded job id".to_string()),
        };
    };
    match build_status_snapshot(&manifest.compose_file, Some(job_id), options) {
        Ok(snapshot) => SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: Some(job_id.to_string()),
            status: categorize_sweep_status(&snapshot),
            scheduler_state: Some(snapshot.scheduler.state),
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: None,
        },
        Err(err) => SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: Some(job_id.to_string()),
            status: "missing_tracking".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: Some(err.to_string()),
        },
    }
}

fn categorize_sweep_status(snapshot: &hpc_compose::job::StatusSnapshot) -> String {
    if snapshot.scheduler.state == "PENDING" {
        return "pending".to_string();
    }
    if snapshot.scheduler.state == "RUNNING" {
        return "running".to_string();
    }
    let service_failed = snapshot.services.iter().any(|service| {
        service.status.as_deref() == Some("failed")
            || service
                .assertions
                .as_ref()
                .is_some_and(|assertions| !assertions.failures.is_empty())
    });
    if snapshot.scheduler.terminal {
        if snapshot.scheduler.failed || service_failed {
            "failed".to_string()
        } else {
            "completed".to_string()
        }
    } else {
        "unknown".to_string()
    }
}

fn print_sweep_status_output(report: &SweepStatusOutput) -> Result<()> {
    println!("sweep: {}", report.sweep_id);
    println!("trials: {}", report.trials.len());
    print!("summary:");
    for (status, count) in &report.summary {
        print!(" {status}={count}");
    }
    println!();
    for trial in &report.trials {
        let job = trial.job_id.as_deref().unwrap_or("-");
        let scheduler = trial.scheduler_state.as_deref().unwrap_or("-");
        println!(
            "  {} {:<16} job={} scheduler={} {}",
            trial.trial_id,
            trial.status,
            job,
            scheduler,
            format_sweep_variables(&trial.variables)
        );
    }
    Ok(())
}

pub(crate) fn sweep_list(context: ResolvedContext, format: Option<OutputFormat>) -> Result<()> {
    let sweeps = scan_sweep_manifests(&context.compose_file.value)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("compose: {}", context.compose_file.value.display());
            println!("sweeps: {}", sweeps.len());
            for sweep in &sweeps {
                println!(
                    "  {} trials={} submitted_at={}",
                    sweep.sweep_id,
                    sweep.trials.len(),
                    sweep.submitted_at
                );
            }
            Ok(())
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&SweepListOutput {
                    compose_file: context.compose_file.value,
                    sweeps,
                })
                .context("failed to serialize sweep list output")?
            );
            Ok(())
        }
    }
}

fn format_sweep_variables(vars: &BTreeMap<String, String>) -> String {
    vars.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parses one trial's objective value from its tracked log or artifacts.
///
/// Returns `Ok(Some(value))` on success, `Ok(None)` when the trial is not yet
/// terminal or has no parseable objective, and `Err` only on unexpected IO.
fn parse_trial_objective(
    trial: &SweepManifestTrial,
    manifest: &SweepManifest,
    options: &SchedulerOptions,
) -> Result<Option<f64>> {
    let Some(job_id) = trial.job_id.as_deref() else {
        return Ok(None);
    };
    let objective = match manifest.objective.as_ref() {
        Some(objective) => objective,
        None => return Ok(None),
    };
    let snapshot = build_status_snapshot(&manifest.compose_file, Some(job_id), options)?;
    if !snapshot.scheduler.terminal {
        return Ok(None);
    }
    if let Some(pattern) = &objective.log_pattern {
        let re = regex::Regex::new(pattern).with_context(|| {
            format!("sweep.objective.log_pattern '{pattern}' is not a valid regex")
        })?;
        let group = objective.group as usize;
        for service in &snapshot.services {
            let Some(log_path) = &service.log_path else {
                continue;
            };
            let Ok(text) = fs::read_to_string(log_path) else {
                continue;
            };
            if let Some(captures) = re.captures(&text)
                && let Some(matched) = captures.get(group)
                && let Ok(value) = matched.as_str().parse::<f64>()
            {
                return Ok(Some(value));
            }
        }
        return Ok(None);
    }
    // json_path source: read from the trial job's artifact tree.
    if let (Some(json_rel), Some(field)) = (&objective.json_path, &objective.json_field) {
        let record_path = trial.record_path.as_deref().with_context(|| {
            format!(
                "trial {} has no record path for json objective",
                trial.trial_id
            )
        })?;
        let record_dir = record_path.parent().unwrap_or_else(|| Path::new("."));
        let job_root = crate::tracked_paths::runtime_job_root(record_dir, job_id);
        let artifacts_dir = crate::tracked_paths::latest_artifacts_dir(&job_root);
        let json_path = artifacts_dir.join(json_rel);
        if let Ok(text) = fs::read_to_string(&json_path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
            && let Some(num) = value.get(field).and_then(|v| v.as_f64())
        {
            return Ok(Some(num));
        }
        return Ok(None);
    }
    Ok(None)
}

/// Selects the best trial id from observed objectives given the direction.
fn best_trial_id(
    trials: &[SweepManifestTrial],
    direction: hpc_compose::spec::ObjectiveDirection,
) -> Option<String> {
    let scored = trials.iter().filter_map(|t| {
        t.objective
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|value| (value, &t.trial_id))
    });
    match direction {
        hpc_compose::spec::ObjectiveDirection::Minimize => scored
            .min_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, id)| id.clone()),
        hpc_compose::spec::ObjectiveDirection::Maximize => scored
            .max_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, id)| id.clone()),
    }
}

/// One comparison operator for `--stop-when` evaluation.
type StopOperator = (&'static str, fn(f64, f64) -> bool);

fn evaluate_stop_condition(expr: &str, best: Option<f64>) -> Result<bool> {
    let expr = expr.trim();
    let operators: &[StopOperator] = &[
        ("<=", |a, b| a <= b),
        (">=", |a, b| a >= b),
        ("<", |a, b| a < b),
        (">", |a, b| a > b),
    ];
    let Some(rest) = expr.strip_prefix("objective") else {
        bail!("--stop-when must look like `objective < 0.05` or `objective >= 0.9` (got '{expr}')")
    };
    let rest = rest.trim();
    for (op, cmp) in operators {
        if let Some(threshold_str) = rest.strip_prefix(op) {
            let threshold: f64 = threshold_str.trim().parse().with_context(|| {
                format!(
                    "--stop-when threshold '{}' is not a number",
                    threshold_str.trim()
                )
            })?;
            return Ok(best.is_some_and(|value| cmp(value, threshold)));
        }
    }
    bail!("--stop-when must look like `objective < 0.05` or `objective >= 0.9` (got '{expr}')");
}

#[derive(Debug, Serialize)]
struct SweepObserveOutput {
    sweep_id: String,
    objective_configured: bool,
    best_trial: Option<String>,
    best_objective: Option<String>,
    trials: Vec<SweepObserveTrial>,
}

#[derive(Debug, Clone, Serialize)]
struct SweepObserveTrial {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    job_id: Option<String>,
    status: String,
    objective: Option<String>,
    objective_error: Option<String>,
}

pub(crate) fn sweep_observe(
    context: ResolvedContext,
    sweep_id: Option<String>,
    watch: bool,
    stop_when: Option<String>,
    poll_interval: Duration,
    timeout: Option<Duration>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let output_format = output::resolve_output_format(format, false);
    let deadline = timeout
        .filter(|t| *t > Duration::ZERO)
        .map(|t| Instant::now() + t);
    loop {
        let mut manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
        let objective_configured = manifest.objective.is_some();
        let now = unix_timestamp_now_for_command();

        let direction = manifest
            .objective
            .as_ref()
            .map(|o| o.direction)
            .unwrap_or(hpc_compose::spec::ObjectiveDirection::Minimize);

        // Pass 1 (immutable): compute each trial's status and parsed objective.
        let mut results: Vec<(String, Option<f64>, Option<String>)> = Vec::new();
        for trial in &manifest.trials {
            let status = status_for_sweep_trial(&manifest, trial, &scheduler_options);
            let status_label = status.status.clone();
            let (parsed, error) = match parse_trial_objective(trial, &manifest, &scheduler_options)
            {
                Ok(Some(value)) => (Some(value), None),
                Ok(None) => (None, None),
                Err(err) => (None, Some(format!("{err:#}"))),
            };
            results.push((status_label, parsed, error));
        }

        let mut trial_outputs = Vec::new();
        let best_objective = if objective_configured {
            // Pass 2 (mutable): write objective state back into the manifest.
            for (trial, (status_label, parsed, error)) in manifest.trials.iter_mut().zip(results) {
                trial.objective = parsed.map(|v| v.to_string());
                trial.objective_error = error.clone();
                trial.observed_at = Some(now);
                trial_outputs.push(SweepObserveTrial {
                    trial_id: trial.trial_id.clone(),
                    index: trial.index,
                    variables: trial.variables.clone(),
                    job_id: trial.job_id.clone(),
                    status: status_label,
                    objective: trial.objective.clone(),
                    objective_error: error,
                });
            }
            manifest.best_trial = best_trial_id(&manifest.trials, direction);
            let best_objective = manifest.best_trial.as_ref().and_then(|id| {
                manifest
                    .trials
                    .iter()
                    .find(|t| &t.trial_id == id)
                    .and_then(|t| t.objective.clone())
            });
            write_sweep_manifest(&manifest)?;
            best_objective
        } else {
            for (trial, (status_label, _parsed, _error)) in manifest.trials.iter().zip(results) {
                trial_outputs.push(SweepObserveTrial {
                    trial_id: trial.trial_id.clone(),
                    index: trial.index,
                    variables: trial.variables.clone(),
                    job_id: trial.job_id.clone(),
                    status: status_label,
                    objective: None,
                    objective_error: None,
                });
            }
            None
        };

        let report = SweepObserveOutput {
            sweep_id: manifest.sweep_id.clone(),
            objective_configured,
            best_trial: manifest.best_trial.clone(),
            best_objective: best_objective.clone(),
            trials: trial_outputs,
        };
        match output_format {
            OutputFormat::Text => print_sweep_observe_output(&report, direction),
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .context("failed to serialize sweep observe output")?
                );
            }
        }

        if !watch {
            return Ok(());
        }
        // --watch --stop-when: stop the sweep when the condition is met.
        if let Some(expr) = stop_when.as_deref() {
            let best_value = best_objective
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok());
            if evaluate_stop_condition(expr, best_value)? {
                if output_format == OutputFormat::Text {
                    println!(
                        "{}",
                        term::styled_success(&format!(
                            "stop condition '{expr}' met; stopping sweep {}",
                            manifest.sweep_id
                        ))
                    );
                }
                let reason = format!("stop-when condition '{expr}' satisfied");
                let report = sweep_stop_inner(
                    &context,
                    sweep_id.as_deref(),
                    true,
                    Some(reason),
                    output_format == OutputFormat::Text,
                )?;
                if output_format == OutputFormat::Text {
                    print_sweep_stop_output(&report);
                }
                return Ok(());
            }
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            bail!("sweep observe --watch timed out before --stop-when was satisfied");
        }
        thread::sleep(poll_interval);
    }
}

fn print_sweep_observe_output(
    report: &SweepObserveOutput,
    direction: hpc_compose::spec::ObjectiveDirection,
) {
    println!("sweep: {}", report.sweep_id);
    if !report.objective_configured {
        println!(
            "{}",
            term::styled_warning("no sweep.objective configured; nothing to observe")
        );
        return;
    }
    println!(
        "direction: {}",
        match direction {
            hpc_compose::spec::ObjectiveDirection::Minimize => "minimize",
            hpc_compose::spec::ObjectiveDirection::Maximize => "maximize",
        }
    );
    if let Some(best) = &report.best_trial {
        let label = report.best_objective.as_deref().unwrap_or("?");
        println!("best: {} (objective={})", best, label);
    }
    // Rank: best first.
    let mut ranked = report.trials.clone();
    ranked.sort_by(|a, b| {
        let av = a.objective.as_deref().and_then(|s| s.parse::<f64>().ok());
        let bv = b.objective.as_deref().and_then(|s| s.parse::<f64>().ok());
        match (av, bv) {
            (Some(a), Some(b)) => match direction {
                hpc_compose::spec::ObjectiveDirection::Minimize => a.partial_cmp(&b),
                hpc_compose::spec::ObjectiveDirection::Maximize => b.partial_cmp(&a),
            }
            .unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    for trial in &ranked {
        let objective = trial.objective.as_deref().unwrap_or("-");
        println!(
            "  {} status={} objective={} {}",
            trial.trial_id,
            trial.status,
            objective,
            format_sweep_variables(&trial.variables)
        );
        if let Some(error) = &trial.objective_error {
            println!("    objective_error: {error}");
        }
    }
}

pub(crate) fn sweep_stop(
    context: ResolvedContext,
    sweep_id: Option<String>,
    yes: bool,
    reason: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    let report = sweep_stop_inner(
        &context,
        sweep_id.as_deref(),
        yes,
        reason,
        output_format == OutputFormat::Text,
    )?;
    match output_format {
        OutputFormat::Text => print_sweep_stop_output(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize sweep stop output")?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SweepStopOutput {
    sweep_id: String,
    cancelled_count: usize,
    skipped_count: usize,
    cancelled_trials: Vec<String>,
    skipped_trials: Vec<String>,
    stopped_at: u64,
    stop_reason: String,
}

fn sweep_stop_inner(
    context: &ResolvedContext,
    sweep_id: Option<&str>,
    yes: bool,
    reason: Option<String>,
    print_confirmation_hint: bool,
) -> Result<SweepStopOutput> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let mut manifest = load_sweep_manifest(&context.compose_file.value, sweep_id)?;
    if !yes {
        if print_confirmation_hint {
            println!(
                "About to cancel all non-terminal trials of sweep {} ({}). Re-run with --yes to proceed.",
                manifest.sweep_id,
                manifest.trials.len()
            );
        }
        bail!("--yes not set; refusing to cancel sweep trials");
    }
    let mut cancelled = Vec::new();
    let mut skipped = Vec::new();
    for trial in &manifest.trials {
        let Some(job_id) = trial.job_id.as_deref() else {
            skipped.push(trial.trial_id.clone());
            continue;
        };
        let status = status_for_sweep_trial(&manifest, trial, &scheduler_options);
        let terminal = matches!(
            status.status.as_str(),
            "completed" | "failed" | "submit_failed"
        );
        if terminal {
            skipped.push(trial.trial_id.clone());
            continue;
        }
        match output::cancel_job(job_id, &context.binaries.scancel.value) {
            Ok(()) => cancelled.push(trial.trial_id.clone()),
            Err(err) => {
                skipped.push(trial.trial_id.clone());
                let _ = writeln!(
                    io::stderr(),
                    "warning: failed to cancel trial {} (job {}): {err}",
                    trial.trial_id,
                    job_id
                );
            }
        }
    }
    let now = unix_timestamp_now_for_command();
    manifest.stopped_at = Some(now);
    manifest.stop_reason = reason.or_else(|| Some("manual sweep stop".to_string()));
    let stop_reason = manifest
        .stop_reason
        .clone()
        .unwrap_or_else(|| "manual sweep stop".to_string());
    write_sweep_manifest(&manifest)?;
    Ok(SweepStopOutput {
        sweep_id: manifest.sweep_id,
        cancelled_count: cancelled.len(),
        skipped_count: skipped.len(),
        cancelled_trials: cancelled,
        skipped_trials: skipped,
        stopped_at: now,
        stop_reason,
    })
}

fn print_sweep_stop_output(report: &SweepStopOutput) {
    println!(
        "stopped sweep {}: {} trial(s) cancelled, {} skipped",
        report.sweep_id, report.cancelled_count, report.skipped_count
    );
}

fn unix_timestamp_now_for_command() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn clean(
    context: ResolvedContext,
    age: Option<u64>,
    all: bool,
    dry_run: bool,
    disk_usage: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let mode = if let Some(days) = age {
        CleanupMode::Age { age_days: days }
    } else {
        debug_assert!(all);
        CleanupMode::AllExceptLatest
    };
    let report = build_cleanup_report(&context.compose_file.value, mode, disk_usage, dry_run)?;
    if !dry_run {
        run_cleanup_report(&report)?;
    }
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output::print_cleanup_report(&report, disk_usage)
                .context("failed to write clean output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize clean output")?
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use hpc_compose::context::{ResolvedBinaries, ResolvedValue, ValueSource};

    fn write_compose(root: &Path) -> PathBuf {
        let compose = root.join("compose.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: docker://redis:7\nx-slurm:\n  cache_dir: {}\n",
                root.join("cache").display()
            ),
        )
        .expect("write compose");
        compose
    }

    fn write_local_compose(root: &Path) -> PathBuf {
        let local_image = root.join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let compose = root.join("compose-local.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                root.join("cache-local").display()
            ),
        )
        .expect("write local compose");
        compose
    }

    fn write_local_compose_with_services(root: &Path) -> PathBuf {
        let local_image = root.join("local-rich.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let compose = root.join("compose-local-rich.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {}\n    command: /bin/true\n    readiness:\n      type: log\n      pattern: ready\n      timeout_seconds: 5\n  worker:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                local_image.display(),
                root.join("cache-rich").display()
            ),
        )
        .expect("write rich local compose");
        compose
    }

    fn resolved_string(value: &str) -> ResolvedValue<String> {
        ResolvedValue {
            value: value.to_string(),
            source: ValueSource::Cli,
        }
    }

    fn context_for(compose: &Path, cwd: &Path) -> ResolvedContext {
        ResolvedContext {
            cwd: cwd.to_path_buf(),
            settings_path: None,
            settings_base_dir: None,
            selected_profile: None,
            compose_file: ResolvedValue {
                value: compose.to_path_buf(),
                source: ValueSource::Cli,
            },
            cache_dir: ResolvedValue {
                value: cwd.join(".cache/hpc-compose"),
                source: ValueSource::Builtin,
            },
            resource_profiles: BTreeMap::new(),
            binaries: ResolvedBinaries {
                enroot: resolved_string("/definitely/missing-enroot"),
                apptainer: resolved_string("/definitely/missing-apptainer"),
                singularity: resolved_string("/definitely/missing-singularity"),
                salloc: resolved_string("/definitely/missing-salloc"),
                sbatch: resolved_string("/definitely/missing-sbatch"),
                srun: resolved_string("/definitely/missing-srun"),
                scontrol: resolved_string("/definitely/missing-scontrol"),
                sinfo: resolved_string("/definitely/missing-sinfo"),
                squeue: resolved_string("/definitely/missing-squeue"),
                sacct: resolved_string("/definitely/missing-sacct"),
                sstat: resolved_string("/definitely/missing-sstat"),
                scancel: resolved_string("/definitely/missing-scancel"),
                sshare: resolved_string("/definitely/missing-sshare"),
                sprio: resolved_string("/definitely/missing-sprio"),
            },
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
            watch: Default::default(),
        }
    }

    #[test]
    fn detect_dev_changes_reports_modified_targets_once() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let root = tmpdir.path().to_path_buf();
        fs::write(root.join("a.txt"), "one").expect("write");
        let snapshot = collect_dev_snapshot(&root).expect("snapshot");
        let mut targets = vec![DevWatchTarget {
            root: root.clone(),
            services: BTreeSet::from(["api".to_string()]),
            snapshot,
        }];

        // No change yet.
        assert!(detect_dev_changes(&mut targets).is_empty());

        // A differently-sized rewrite is detected and the snapshot advances.
        fs::write(root.join("a.txt"), "modified-content").expect("rewrite");
        let affected = detect_dev_changes(&mut targets);
        assert!(affected.contains("api"));

        // The advanced snapshot means a second pass is clean.
        assert!(detect_dev_changes(&mut targets).is_empty());
    }

    #[test]
    fn dev_watch_inference_uses_directory_mounts_and_explicit_roots() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let source_dir = tmpdir.path().join("src");
        let relative_source_dir = tmpdir.path().join("relative-src");
        let explicit_dir = tmpdir.path().join("extra");
        let cache_dir = tmpdir.path().join("cache");
        let file_mount = tmpdir.path().join("settings.toml");
        fs::create_dir_all(&source_dir).expect("source dir");
        fs::create_dir_all(&relative_source_dir).expect("relative source dir");
        fs::create_dir_all(&explicit_dir).expect("explicit dir");
        fs::create_dir_all(&cache_dir).expect("cache dir");
        fs::write(&file_mount, "x").expect("file mount");
        let compose = tmpdir.path().join("compose-dev.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nx-slurm:\n  cache_dir: {}\nservices:\n  api:\n    image: {}\n    command: /bin/true\n    volumes:\n      - {}:/workspace\n      - ./relative-src:/relative\n      - {}:/config.toml:ro\n      - {}:/cache\n  worker:\n    image: {}\n    command: /bin/true\n    volumes:\n      - {}:/workspace\n",
                cache_dir.display(),
                local_image.display(),
                source_dir.display(),
                file_mount.display(),
                cache_dir.display(),
                local_image.display(),
                source_dir.display(),
            ),
        )
        .expect("compose");
        let plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let targets =
            infer_dev_watch_targets(&plan, tmpdir.path(), std::slice::from_ref(&explicit_dir))
                .expect("watch targets");
        let source_dir = canonical_dev_path(&source_dir);
        let relative_source_dir = canonical_dev_path(&relative_source_dir);
        let explicit_dir = canonical_dev_path(&explicit_dir);
        let cache_dir = canonical_dev_path(&cache_dir);
        let file_mount = canonical_dev_path(&file_mount);
        let source = targets
            .iter()
            .find(|target| target.root == source_dir)
            .expect("source target");
        assert!(source.services.contains("api"));
        assert!(source.services.contains("worker"));
        let relative_source = targets
            .iter()
            .find(|target| target.root == relative_source_dir)
            .unwrap_or_else(|| panic!("relative source target missing from {targets:#?}"));
        assert!(relative_source.services.contains("api"));
        assert!(!targets.iter().any(|target| target.root == cache_dir));
        assert!(!targets.iter().any(|target| target.root == file_mount));
        let explicit = targets
            .iter()
            .find(|target| target.root == explicit_dir)
            .expect("explicit target");
        assert_eq!(
            explicit.services,
            ["api".to_string(), "worker".to_string()]
                .into_iter()
                .collect()
        );

        let before = collect_dev_snapshot(&source_dir).expect("snapshot before");
        fs::write(source_dir.join("main.py"), "print('hi')\n").expect("source change");
        let after = collect_dev_snapshot(&source_dir).expect("snapshot after");
        assert_ne!(before, after);
    }

    #[test]
    fn smoke_evaluation_rejects_missing_readiness_and_completion() {
        let snapshot = hpc_compose::job::StatusSnapshot {
            record: SubmissionRecord {
                schema_version: 2,
                backend: SubmissionBackend::Slurm,
                kind: SubmissionKind::Main,
                job_id: "123".into(),
                submitted_at: 1,
                compose_file: PathBuf::from("compose.yaml"),
                submit_dir: PathBuf::from("/tmp"),
                script_path: PathBuf::from("job.sbatch"),
                cache_dir: PathBuf::from("/tmp/cache"),
                batch_log: PathBuf::from("slurm-123.out"),
                service_logs: BTreeMap::new(),
                artifact_export_dir: None,
                resume_dir: None,
                service_name: None,
                command_override: None,
                requested_walltime: None,
                slurm_array: None,
                sweep: None,
                config_snapshot_yaml: None,
                cached_artifacts: Vec::new(),
            },
            scheduler: hpc_compose::job::SchedulerStatus {
                state: "COMPLETED".into(),
                source: hpc_compose::job::SchedulerSource::Sacct,
                terminal: true,
                failed: false,
                detail: None,
            },
            queue_diagnostics: None,
            array: None,
            log_dir: PathBuf::from("/tmp/logs"),
            batch_log: hpc_compose::job::BatchLogStatus {
                path: PathBuf::from("slurm-123.out"),
                present: true,
                updated_at: None,
                updated_age_seconds: None,
            },
            services: vec![hpc_compose::job::PsServiceRow {
                service_name: "api".into(),
                path: PathBuf::from("api.log"),
                present: true,
                updated_at: None,
                updated_age_seconds: None,
                log_path: None,
                step_name: None,
                launch_index: Some(0),
                launcher_pid: None,
                healthy: Some(false),
                completed_successfully: Some(false),
                readiness_configured: Some(true),
                status: Some("exited(1)".into()),
                failure_policy_mode: Some("ignore".into()),
                restart_count: Some(0),
                max_restarts: None,
                window_seconds: None,
                max_restarts_in_window: None,
                restart_failures_in_window: None,
                last_exit_code: Some(1),
                started_at: Some(10),
                finished_at: Some(11),
                duration_seconds: Some(1),
                assertions: None,
                placement_mode: None,
                nodes: None,
                ntasks: None,
                ntasks_per_node: None,
                nodelist: None,
            }],
            attempt: None,
            is_resume: None,
            resume_dir: None,
        };
        let evaluation = evaluate_smoke_snapshot(&snapshot);
        assert!(!evaluation.ok);
        let reason = evaluation.failure_reason.expect("failure reason");
        assert!(reason.contains("api"));
        assert!(reason.contains("readiness"));
        assert!(reason.contains("complete successfully"));
    }

    #[test]
    fn tmux_tail_command_quotes_log_paths() {
        let command = tmux_tail_command(Path::new("/tmp/demo run/api's log.txt"), 25);
        assert_eq!(command, "tail -n 25 -F '/tmp/demo run/api'\\''s log.txt'");
    }

    #[test]
    fn runtime_command_wrappers_cover_success_and_error_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_compose(tmpdir.path());
        let context = context_for(&compose, tmpdir.path());
        let local_compose = write_local_compose(tmpdir.path());

        launch(
            context_for(&local_compose, tmpdir.path()),
            Some(tmpdir.path().join("job.sbatch")),
            PrepareFlags {
                keep_failed_prep: false,
                skip_prepare: true,
                force_rebuild: false,
                no_preflight: true,
            },
            false,
            false,
            None,
            false,
            false,
            false,
            true,
            None,
            WatchMode::Auto,
            HoldOnExit::Failure,
            false,
        )
        .expect("submit dry run");
        launch(
            context_for(&local_compose, tmpdir.path()),
            Some(tmpdir.path().join("job.json.sbatch")),
            PrepareFlags {
                keep_failed_prep: false,
                skip_prepare: true,
                force_rebuild: false,
                no_preflight: true,
            },
            false,
            false,
            None,
            false,
            false,
            false,
            true,
            Some(OutputFormat::Json),
            WatchMode::Auto,
            HoldOnExit::Failure,
            false,
        )
        .expect("submit dry run json");

        let status_err = status(
            context.clone(),
            Some("12345".into()),
            Some(OutputFormat::Json),
            false,
            false,
        )
        .expect_err("status should require tracked metadata");
        assert!(status_err.to_string().contains("tracked job '12345'"));

        stats(
            context.clone(),
            Some("12345".into()),
            false,
            Some(StatsOutputFormat::Json),
            false,
        )
        .expect("stats should degrade when scheduler commands are unavailable");

        let artifacts_err = artifacts(
            context.clone(),
            None,
            Some(OutputFormat::Json),
            false,
            Vec::new(),
            false,
        )
        .expect_err("artifacts should require a tracked submission");
        assert!(
            artifacts_err
                .to_string()
                .contains("no tracked submission metadata exists")
        );

        let logs_err = logs(context.clone(), None, None, false, 10, None, None)
            .expect_err("logs should require a tracked submission");
        assert!(
            logs_err
                .to_string()
                .contains("no tracked submission metadata exists")
        );

        jobs_list(false, Some(OutputFormat::Json)).expect("jobs list");
        clean(
            context,
            Some(7),
            false,
            true,
            true,
            Some(OutputFormat::Json),
        )
        .expect("clean");

        let sbatch_path = tmpdir.path().join("fake-sbatch.sh");
        fs::write(
            &sbatch_path,
            "#!/bin/sh\nprintf 'submit boom\\n' >&2\nexit 1\n",
        )
        .expect("fake sbatch");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&sbatch_path).expect("metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&sbatch_path, perms).expect("chmod");
        }
        let mut sbatch_context = context_for(&compose, tmpdir.path());
        sbatch_context.binaries.sbatch.value = sbatch_path.to_string_lossy().to_string();
        let submit_err = launch(
            sbatch_context,
            Some(tmpdir.path().join("submit-fail.sbatch")),
            PrepareFlags {
                keep_failed_prep: false,
                skip_prepare: true,
                force_rebuild: false,
                no_preflight: true,
            },
            false,
            false,
            None,
            false,
            false,
            false,
            false,
            None,
            WatchMode::Auto,
            HoldOnExit::Failure,
            false,
        )
        .expect_err("sbatch failure");
        assert!(
            submit_err
                .to_string()
                .contains("sbatch failed: submit boom")
        );
    }

    #[test]
    fn local_helper_functions_cover_labels_ids_and_stub_state_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose_with_services(tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("job.local.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-test-123",
            SubmissionBackend::Local,
        )
        .expect("record");

        assert!(generate_local_job_id().starts_with("local-"));
        assert_eq!(
            local_failure_policy_mode_label(ServiceFailureMode::FailJob),
            "fail_job"
        );
        assert_eq!(
            local_failure_policy_mode_label(ServiceFailureMode::Ignore),
            "ignore"
        );
        assert_eq!(
            local_failure_policy_mode_label(ServiceFailureMode::RestartOnFailure),
            "restart_on_failure"
        );
        assert_eq!(
            local_placement_mode_label(ServicePlacementMode::PrimaryNode),
            "primary_node"
        );
        assert_eq!(
            local_placement_mode_label(ServicePlacementMode::Distributed),
            "distributed"
        );
        assert_eq!(
            local_placement_mode_label(ServicePlacementMode::Partitioned),
            "partitioned"
        );
        assert_eq!(local_service_step_name("api"), "hpc-compose:api");
        assert_eq!(
            local_service_step_name("api.worker-1"),
            "hpc-compose:api_x2e_worker_x2d_1"
        );

        write_local_runtime_state_stub(&record, &runtime_plan, 777).expect("state stub");
        let state_path = state_path_for_record(&record);
        let state: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&state_path).expect("read state"))
                .expect("parse state");
        assert_eq!(state["backend"], serde_json::Value::from("local"));
        assert_eq!(state["supervisor_pid"], serde_json::Value::from(777));
        assert_eq!(state["services"].as_array().map(Vec::len), Some(2));
        assert_eq!(state["services"][0]["service_name"], "api");
        assert_eq!(state["services"][0]["readiness_configured"], true);
        assert_eq!(state["services"][1]["service_name"], "worker");

        assert_eq!(
            read_local_supervisor_pid(&record).expect("supervisor pid"),
            Some(777)
        );

        fs::write(&state_path, "{\"supervisor_pid\":9}").expect("overwrite state");
        write_local_runtime_state_stub(&record, &runtime_plan, 888).expect("existing state");
        let preserved: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&state_path).expect("read preserved"))
                .expect("parse preserved");
        assert_eq!(preserved["supervisor_pid"], serde_json::Value::from(9));
    }

    #[test]
    fn process_helpers_cover_spawn_kill_and_pid_reader_edges() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose(tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("job.local.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-test-456",
            SubmissionBackend::Local,
        )
        .expect("record");

        assert_eq!(
            read_local_supervisor_pid(&record).expect("missing state pid"),
            None
        );

        let state_path = state_path_for_record(&record);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("state dir");
        }
        fs::write(&state_path, "{not-json").expect("bad state");
        let parse_err = read_local_supervisor_pid(&record).expect_err("malformed state");
        assert!(parse_err.to_string().contains("failed to parse"));

        fs::write(&script_path, "#!/bin/bash\ntrap 'exit 0' TERM\nsleep 30\n").expect("script");
        let batch_log = tmpdir.path().join("batch.log");
        let pid =
            spawn_local_supervisor(tmpdir.path(), &script_path, &batch_log).expect("spawn local");
        assert!(batch_log.exists());

        kill_pid(pid).expect("kill child");
        thread::sleep(Duration::from_millis(200));

        let kill_err = kill_pid(u32::MAX).expect_err("unknown pid");
        assert!(kill_err.to_string().contains("failed to signal pid"));
    }

    #[test]
    fn tracking_resolution_and_cache_purge_helpers_cover_edge_cases() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose(tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let context = context_for(&compose, tmpdir.path());
        let script_path = tmpdir.path().join("job.local.sh");
        let mut older = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-old",
            SubmissionBackend::Local,
        )
        .expect("older record");
        older.submitted_at = 100;
        let mut newer = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-new",
            SubmissionBackend::Local,
        )
        .expect("newer record");
        newer.submitted_at = 200;
        write_submission_record(&newer).expect("write newer");
        write_submission_record(&older).expect("write older");

        assert_eq!(
            resolve_tracked_record(&context, None)
                .expect("resolve latest")
                .expect("latest")
                .job_id,
            "local-new"
        );
        assert_eq!(
            resolve_tracked_record(&context, Some("local-old"))
                .expect("resolve explicit")
                .expect("explicit")
                .job_id,
            "local-old"
        );
        assert!(
            resolve_tracked_record(&context, Some("missing"))
                .expect("resolve missing")
                .is_none()
        );

        let file_artifact = tmpdir.path().join("cache/file.sqsh");
        let dir_artifact = tmpdir.path().join("cache/dir-artifact");
        fs::create_dir_all(&dir_artifact).expect("dir artifact");
        fs::create_dir_all(file_artifact.parent().expect("file parent")).expect("file parent");
        fs::write(&file_artifact, "artifact").expect("file artifact");
        fs::write(dir_artifact.join("payload"), "artifact").expect("dir payload");
        let missing = tmpdir.path().join("cache/missing.sqsh");
        let removed =
            purge_cached_artifacts(&[file_artifact.clone(), dir_artifact.clone(), missing.clone()])
                .expect("purge");
        assert_eq!(removed, vec![file_artifact.clone(), dir_artifact.clone()]);
        assert!(!file_artifact.exists());
        assert!(!dir_artifact.exists());

        assert!(
            cached_artifacts_for_teardown(None)
                .expect_err("missing record")
                .to_string()
                .contains("--purge-cache requires tracked submission metadata")
        );
        assert!(
            cached_artifacts_for_teardown(Some(&older))
                .expect_err("empty cached artifacts")
                .to_string()
                .contains("does not contain cached artifact snapshots")
        );
        newer.cached_artifacts = vec![missing.clone()];
        assert_eq!(
            cached_artifacts_for_teardown(Some(&newer)).expect("cached artifacts"),
            vec![missing]
        );
    }

    #[test]
    fn local_submit_support_and_warning_helpers_cover_non_linux_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose(tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");

        warn_local_ignored_scheduler_settings(&runtime_plan);

        let distributed = tmpdir.path().join("distributed.yaml");
        let local_image = tmpdir.path().join("distributed.sqsh");
        fs::write(&local_image, "sqsh").expect("distributed image");
        fs::write(
            &distributed,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      nodes: 2\nx-slurm:\n  cache_dir: {}\n  nodes: 2\n",
                local_image.display(),
                tmpdir.path().join("cache-distributed").display()
            ),
        )
        .expect("distributed compose");
        let distributed_plan = output::load_runtime_plan(&distributed).expect("distributed plan");
        let distributed_err =
            ensure_local_plan_supported(&distributed_plan).expect_err("distributed unsupported");
        assert!(
            distributed_err
                .to_string()
                .contains("does not support distributed or partitioned placement")
        );

        let extra_args = tmpdir.path().join("extra-args.yaml");
        fs::write(
            &extra_args,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      extra_srun_args:\n        - --exclusive\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache-extra").display()
            ),
        )
        .expect("extra args compose");
        let extra_args_plan = output::load_runtime_plan(&extra_args).expect("extra args plan");
        let extra_args_err =
            ensure_local_plan_supported(&extra_args_plan).expect_err("extra args unsupported");
        assert!(extra_args_err.to_string().contains("extra_srun_args"));

        let mpi = tmpdir.path().join("mpi.yaml");
        fs::write(
            &mpi,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      mpi:\n        type: pmix\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache-mpi").display()
            ),
        )
        .expect("mpi compose");
        let mpi_plan = output::load_runtime_plan(&mpi).expect("mpi plan");
        let mpi_err = ensure_local_plan_supported(&mpi_plan).expect_err("mpi unsupported");
        assert!(mpi_err.to_string().contains("x-slurm.mpi"));

        let multi_node = tmpdir.path().join("multi-node.yaml");
        fs::write(
            &multi_node,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      nodes: 1\nx-slurm:\n  cache_dir: {}\n  nodes: 2\n",
                local_image.display(),
                tmpdir.path().join("cache-nodes").display()
            ),
        )
        .expect("multi-node compose");
        let multi_node_plan = output::load_runtime_plan(&multi_node).expect("multi-node plan");
        let multi_node_err =
            ensure_local_plan_supported(&multi_node_plan).expect_err("multi-node unsupported");
        assert!(
            multi_node_err
                .to_string()
                .contains("only single-host specs")
        );

        if env::consts::OS != "linux" {
            let err = ensure_local_host_supported().expect_err("non-linux");
            assert!(err.to_string().contains("only supported on Linux hosts"));
        }
    }

    #[test]
    fn local_watch_cancel_and_rollback_helpers_cover_terminal_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose(tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("watch.local.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-watch-123",
            SubmissionBackend::Local,
        )
        .expect("record");

        write_submission_record(&record).expect("persist record");
        let state_path = state_path_for_record(&record);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("state dir");
        }
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "backend": SubmissionBackend::Local,
                "job_status": "COMPLETED",
                "job_exit_code": 0,
                "supervisor_pid": serde_json::Value::Null,
                "services": [],
            }))
            .expect("state json"),
        )
        .expect("write state");

        print_local_launch_details(&record, &runtime_plan, &script_path);

        let watch = watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: "/definitely/missing-squeue".into(),
                sacct_bin: "/definitely/missing-sacct".into(),
            },
            None,
            5,
            WatchMode::Auto,
            HoldOnExit::Failure,
            watch_ui::WatchPrefs::default(),
        )
        .expect("watch");
        assert!(matches!(
            watch,
            hpc_compose::job::WatchOutcome::Completed(_)
        ));

        cancel(
            context_for(&compose, tmpdir.path()),
            Some(record.job_id.clone()),
            false,
            Some(OutputFormat::Json),
        )
        .expect("cancel local without pid");

        write_submission_record(&record).expect("rewrite record");
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("recreate state dir");
        }
        let mut sleeper = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "backend": SubmissionBackend::Local,
                "job_status": "RUNNING",
                "job_exit_code": serde_json::Value::Null,
                "supervisor_pid": sleeper.id(),
                "services": [],
            }))
            .expect("running state json"),
        )
        .expect("write running state");
        cancel(
            context_for(&compose, tmpdir.path()),
            Some(record.job_id.clone()),
            false,
            None,
        )
        .expect("cancel running local");
        sleeper.wait().expect("wait for cancelled sleep");

        let reservation_compose = tmpdir.path().join("compose-reservation.yaml");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(
            &reservation_compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n  error: local.err\n  submit_args:\n    - --reservation=debug\n",
                local_image.display(),
                tmpdir.path().join("cache-reservation").display()
            ),
        )
        .expect("reservation compose");
        let reservation_plan =
            output::load_runtime_plan(&reservation_compose).expect("reservation runtime plan");
        warn_local_ignored_scheduler_settings(&reservation_plan);

        let mut sleeper = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        rollback_local_tracking(&record, Some(sleeper.id()));
        sleeper.wait().expect("wait for sleep");
        assert!(
            load_submission_record(&compose, Some(&record.job_id)).is_err(),
            "rollback should remove tracked record"
        );
    }

    #[test]
    fn runtime_wrappers_cover_success_paths_with_local_tracking() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_local_compose_with_services(tmpdir.path());
        let context = context_for(&compose, tmpdir.path());
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("local-wrapper.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-success-123",
            SubmissionBackend::Local,
        )
        .expect("record");
        write_submission_record(&record).expect("write record");

        for (service_name, log_path) in &record.service_logs {
            if let Some(parent) = log_path.parent() {
                fs::create_dir_all(parent).expect("log dir");
            }
            fs::write(log_path, format!("{service_name} ready\n")).expect("service log");
        }

        let state_path = state_path_for_record(&record);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("state dir");
        }
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "backend": SubmissionBackend::Local,
                "job_status": "COMPLETED",
                "job_exit_code": 0,
                "supervisor_pid": serde_json::Value::Null,
                "services": [
                    {
                        "service_name": "api",
                        "step_name": "hpc-compose:api",
                        "log_path": record.service_logs["api"],
                        "launch_index": 0,
                        "launcher_pid": serde_json::Value::Null,
                        "healthy": true,
                        "readiness_configured": true,
                        "failure_policy_mode": "fail_job",
                        "restart_count": 0,
                        "last_exit_code": 0
                    },
                    {
                        "service_name": "worker",
                        "step_name": "hpc-compose:worker",
                        "log_path": record.service_logs["worker"],
                        "launch_index": 1,
                        "launcher_pid": serde_json::Value::Null,
                        "healthy": false,
                        "readiness_configured": false,
                        "failure_policy_mode": "ignore",
                        "restart_count": 0,
                        "last_exit_code": 0
                    }
                ]
            }))
            .expect("state json"),
        )
        .expect("write state");

        status(
            context.clone(),
            Some(record.job_id.clone()),
            Some(OutputFormat::Json),
            false,
            false,
        )
        .expect("status");
        stats(
            context.clone(),
            Some(record.job_id.clone()),
            false,
            Some(StatsOutputFormat::Json),
            false,
        )
        .expect("stats");
        ps(
            context.clone(),
            Some(record.job_id.clone()),
            Some(OutputFormat::Json),
        )
        .expect("ps");
        logs(
            context.clone(),
            Some(record.job_id.clone()),
            Some("api".into()),
            false,
            10,
            None,
            None,
        )
        .expect("logs");
        watch(
            context.clone(),
            Some(record.job_id.clone()),
            Some("api".into()),
            10,
            WatchMode::Line,
            HoldOnExit::Failure,
        )
        .expect("watch");
        cancel(
            context.clone(),
            Some(record.job_id.clone()),
            false,
            Some(OutputFormat::Json),
        )
        .expect("cancel");
        jobs_list(true, Some(OutputFormat::Json)).expect("jobs list");
        clean(
            context,
            Some(0),
            false,
            true,
            true,
            Some(OutputFormat::Json),
        )
        .expect("clean");
    }
}
