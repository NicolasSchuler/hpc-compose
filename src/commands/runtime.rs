use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{OutputFormat, StatsOutputFormat};
use hpc_compose::cluster::{discover_cluster_profile_path, load_cluster_profile};
use hpc_compose::context::ResolvedContext;
#[cfg(test)]
use hpc_compose::job::build_submission_record_with_backend;
use hpc_compose::job::{
    ArtifactExportOptions, CleanupMode, RequestedWalltime, SchedulerOptions, StatsOptions,
    SubmissionBackend, SubmissionKind, SubmissionRecord, SubmissionRecordBuildOptions,
    build_cleanup_report, build_ps_snapshot, build_stats_snapshot, build_status_snapshot,
    build_submission_record_with_backend_and_options, build_submission_record_with_options,
    export_artifacts, find_submission_record_in_repo, latest_record_path_for,
    latest_run_record_path_for, load_submission_record, print_logs, remove_submission_record,
    run_cleanup_report, runtime_job_root_for_record, scan_job_inventory, scan_job_records,
    state_path_for_record, watch_submission, write_submission_record,
};
use hpc_compose::planner::{ExecutionSpec, ImageSource, ServicePlacementMode};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{
    PrepareOptions, RuntimePlan, base_image_path_for_backend, prepare_runtime_plan,
};
use hpc_compose::render::{
    RenderOptions, log_file_name_for_service, render_local_script, render_script_with_options,
};
use hpc_compose::spec::{ServiceFailureMode, parse_slurm_time_limit};

use crate::output;
use crate::progress::{PrepareProgress, ProgressReporter};
use crate::watch_ui;

fn watch_with_fallback(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    service: Option<&str>,
    lines: usize,
) -> Result<hpc_compose::job::WatchOutcome> {
    if watch_ui::can_use_watch_ui() {
        match watch_ui::run_watch_ui(record, options, service, lines) {
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
    }
}

fn default_run_script_path(compose_file: &Path, service_name: &str) -> PathBuf {
    let parent = compose_file.parent().unwrap_or_else(|| Path::new("."));
    let service_token = log_file_name_for_service(service_name)
        .trim_end_matches(".log")
        .to_string();
    parent.join(format!("hpc-compose-run-{service_token}.sbatch"))
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

fn requested_walltime(plan: &RuntimePlan) -> Option<RequestedWalltime> {
    let raw = plan.slurm.time.as_deref()?;
    let seconds = parse_slurm_time_limit(raw).ok()?;
    Some(RequestedWalltime {
        original: raw.to_string(),
        seconds,
    })
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
        bail!("resume config drift detected; rerun with --allow-resume-changes to submit anyway");
    }
    Ok(false)
}

fn resolve_tracked_record(
    context: &ResolvedContext,
    job_id: Option<&str>,
) -> Result<Option<SubmissionRecord>> {
    match job_id {
        Some(job_id) => {
            if let Ok(record) = load_submission_record(&context.compose_file.value, Some(job_id)) {
                return Ok(Some(record));
            }
            match find_submission_record_in_repo(&context.cwd, job_id) {
                Ok(record) => Ok(Some(record)),
                Err(_) => Ok(None),
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
                "--local does not support x-slurm.extra_srun_args; service '{}' sets: {}",
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
    #[cfg(unix)]
    {
        if pid == 0 || pid > i32::MAX as u32 {
            bail!("failed to signal pid {pid}");
        }

        // Use libc directly so invalid test PIDs cannot be reinterpreted by `/bin/kill`.
        let status = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if status == 0 {
            return Ok(());
        }

        let detail = std::io::Error::last_os_error().to_string();
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
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn submit(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    keep_failed_prep: bool,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    watch: bool,
    local: bool,
    allow_resume_changes: bool,
    resume_diff_only: bool,
    dry_run: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let effective_config =
        output::load_effective_config_with_interpolation_vars(&file, &context.interpolation_vars)?;
    let effective_config_yaml = output::effective_config_yaml(&effective_config)?;
    let runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
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

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    }

    if !no_preflight {
        let cluster_profile = load_discovered_cluster_profile(&context)?;
        let report = progress.run_result("Running preflight checks", || {
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
                    cluster_profile,
                },
            ))
        })?;
        if !quiet {
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
                    .context("failed to serialize submit output")?
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
                    .context("failed to serialize submit output")?
                );
            }
        }

        if watch {
            output::finish_watch(
                &record.job_id,
                watch_with_fallback(
                    &record,
                    &SchedulerOptions {
                        squeue_bin: context.binaries.squeue.value.clone(),
                        sacct_bin: context.binaries.sacct.value.clone(),
                    },
                    None,
                    100,
                )?,
            )?;
        }
        return Ok(());
    }

    let output_result = progress.run_result("Submitting job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
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
    let tracked_submission = if let Some(job_id) = output::extract_job_id(stdout.trim()) {
        let record = build_submission_record_with_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            job_id,
            &record_options,
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

    match output_format {
        OutputFormat::Text => {
            print!("{stdout}");
            output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
            if let Some((record, persisted)) = tracked_submission.as_ref() {
                if *persisted {
                    let meta_path = latest_record_path(record);
                    output::print_submit_summary_box(
                        &runtime_plan,
                        &record.job_id,
                        &script_path,
                        Some(&meta_path),
                    );
                } else {
                    println!(
                        "note: tracking metadata could not be written, so later status/logs commands will not auto-discover this submission"
                    );
                }
            } else {
                println!(
                    "note: submit output did not include a numeric Slurm job id, so status/logs/watch are not trackable for this submission"
                );
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::SubmitOutput {
                    backend: SubmissionBackend::Slurm,
                    compose_file: file.clone(),
                    script_path: script_path.clone(),
                    cache_dir: runtime_plan.cache_dir.clone(),
                    dry_run: false,
                    launched: false,
                    submitted: true,
                    sbatch_stdout: Some(stdout.trim().to_string()),
                    job_id: tracked_submission
                        .as_ref()
                        .map(|(record, _)| record.job_id.clone()),
                    tracking_persisted: tracked_submission
                        .as_ref()
                        .is_some_and(|(_, persisted)| *persisted),
                    tracked_metadata_path,
                })
                .context("failed to serialize submit output")?
            );
        }
    }

    if watch {
        let Some((record, _)) = tracked_submission.as_ref() else {
            println!("note: skipping watch because the submission is not trackable");
            return Ok(());
        };
        output::finish_watch(
            &record.job_id,
            watch_with_fallback(
                record,
                &SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
                None,
                100,
            )?,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn up(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    keep_failed_prep: bool,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    local: bool,
    allow_resume_changes: bool,
    resume_diff_only: bool,
    dry_run: bool,
    quiet: bool,
) -> Result<()> {
    submit(
        context,
        script_out,
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
        true,
        local,
        allow_resume_changes,
        resume_diff_only,
        dry_run,
        Some(OutputFormat::Text),
        quiet,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_service(
    context: ResolvedContext,
    service_name: String,
    command: Vec<String>,
    script_out: Option<PathBuf>,
    keep_failed_prep: bool,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    quiet: bool,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let progress = ProgressReporter::new(!quiet);
    let mut runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
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

    if !no_preflight {
        let cluster_profile = load_discovered_cluster_profile(&context)?;
        let report = progress.run_result("Running preflight checks", || {
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
                    cluster_profile,
                },
            ))
        })?;
        if !quiet {
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
        render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
            },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| default_run_script_path(&file, &service_name));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    let output_result = progress.run_result("Submitting run job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
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
            "note: submit output did not include a numeric Slurm job id, so this run is not trackable"
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
        &record.job_id,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            Some(&service_name),
            100,
        )?,
    )
}

pub(crate) fn status(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    json: bool,
) -> Result<()> {
    let snapshot = build_status_snapshot(
        &context.compose_file.value,
        job_id.as_deref(),
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value,
            sacct_bin: context.binaries.sacct.value,
        },
    )?;
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
) -> Result<()> {
    let snapshot = build_stats_snapshot(
        &context.compose_file.value,
        job_id.as_deref(),
        &StatsOptions {
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            sstat_bin: context.binaries.sstat.value,
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

pub(crate) fn logs(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    follow: bool,
    lines: usize,
) -> Result<()> {
    let record = load_submission_record(&context.compose_file.value, job_id.as_deref())?;
    print_logs(&record, service.as_deref(), lines, follow)
}

pub(crate) fn ps(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let snapshot = build_ps_snapshot(
        &context.compose_file.value,
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
) -> Result<()> {
    let record = load_submission_record(&context.compose_file.value, job_id.as_deref())?;
    output::finish_watch(
        &record.job_id,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            service.as_deref(),
            lines,
        )?,
    )
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
            kill_pid(pid)
                .with_context(|| format!("failed to cancel local job {resolved_job_id}"))?;
            true
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
            binaries: ResolvedBinaries {
                enroot: resolved_string("/definitely/missing-enroot"),
                apptainer: resolved_string("/definitely/missing-apptainer"),
                singularity: resolved_string("/definitely/missing-singularity"),
                sbatch: resolved_string("/definitely/missing-sbatch"),
                srun: resolved_string("/definitely/missing-srun"),
                scontrol: resolved_string("/definitely/missing-scontrol"),
                sinfo: resolved_string("/definitely/missing-sinfo"),
                squeue: resolved_string("/definitely/missing-squeue"),
                sacct: resolved_string("/definitely/missing-sacct"),
                sstat: resolved_string("/definitely/missing-sstat"),
                scancel: resolved_string("/definitely/missing-scancel"),
            },
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
        }
    }

    #[test]
    fn runtime_command_wrappers_cover_success_and_error_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_compose(tmpdir.path());
        let context = context_for(&compose, tmpdir.path());
        let local_compose = write_local_compose(tmpdir.path());

        submit(
            context_for(&local_compose, tmpdir.path()),
            Some(tmpdir.path().join("job.sbatch")),
            false,
            true,
            false,
            true,
            false,
            false,
            false,
            false,
            true,
            None,
            false,
        )
        .expect("submit dry run");
        submit(
            context_for(&local_compose, tmpdir.path()),
            Some(tmpdir.path().join("job.json.sbatch")),
            false,
            true,
            false,
            true,
            false,
            false,
            false,
            false,
            true,
            Some(OutputFormat::Json),
            false,
        )
        .expect("submit dry run json");

        let status_err = status(
            context.clone(),
            Some("12345".into()),
            Some(OutputFormat::Json),
            false,
        )
        .expect_err("status should require tracked metadata");
        assert!(
            status_err
                .to_string()
                .contains("no tracked submission metadata exists")
        );

        let stats_err = stats(
            context.clone(),
            Some("12345".into()),
            false,
            Some(StatsOutputFormat::Json),
        )
        .expect_err("stats should surface scheduler execution failure");
        assert!(stats_err.to_string().contains("failed to execute"));

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

        let logs_err = logs(context.clone(), None, None, false, 10)
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
        let submit_err = submit(
            sbatch_context,
            Some(tmpdir.path().join("submit-fail.sbatch")),
            false,
            true,
            false,
            true,
            false,
            false,
            false,
            false,
            false,
            None,
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
        )
        .expect("status");
        stats(
            context.clone(),
            Some(record.job_id.clone()),
            false,
            Some(StatsOutputFormat::Json),
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
        )
        .expect("logs");
        watch(
            context.clone(),
            Some(record.job_id.clone()),
            Some("api".into()),
            10,
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
