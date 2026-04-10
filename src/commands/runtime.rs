use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{OutputFormat, StatsOutputFormat};
use hpc_compose::context::ResolvedContext;
use hpc_compose::job::{
    ArtifactExportOptions, CleanupMode, SchedulerOptions, StatsOptions, SubmissionBackend,
    SubmissionRecord, build_cleanup_report, build_ps_snapshot, build_stats_snapshot,
    build_status_snapshot, build_submission_record, build_submission_record_with_backend,
    export_artifacts, load_submission_record, print_logs, remove_submission_record,
    run_cleanup_report, runtime_job_root_for_record, scan_job_inventory, state_path_for_record,
    watch_submission, write_submission_record,
};
use hpc_compose::planner::ServicePlacementMode;
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, RuntimePlan, prepare_runtime_plan};
use hpc_compose::render::{log_file_name_for_service, render_local_script, render_script};
use hpc_compose::spec::ServiceFailureMode;

use crate::output;
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

fn generate_local_job_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("local-{timestamp}-{}", std::process::id())
}

fn ensure_local_submit_supported(plan: &RuntimePlan) -> Result<()> {
    if env::consts::OS != "linux" {
        bail!("--local is only supported on Linux hosts");
    }
    for service in &plan.ordered_services {
        if service.placement.mode == ServicePlacementMode::Distributed {
            bail!(
                "--local does not support distributed placement; service '{}' spans the full allocation",
                service.name
            );
        }
        if !service.slurm.extra_srun_args.is_empty() {
            bail!(
                "--local does not support x-slurm.extra_srun_args; service '{}' sets: {}",
                service.name,
                service.slurm.extra_srun_args.join(" ")
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
    bail!("failed to signal pid {pid}: {detail}")
}

fn rollback_local_tracking(record: &SubmissionRecord, supervisor_pid: Option<u32>) {
    if let Some(pid) = supervisor_pid {
        if let Err(err) = kill_pid(pid) {
            let _ = writeln!(
                io::stderr(),
                "warning: failed to stop local supervisor {} during rollback: {err}",
                pid
            );
        }
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
    dry_run: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let output_format = output::resolve_output_format(format, false);
    let backend = if local {
        SubmissionBackend::Local
    } else {
        SubmissionBackend::Slurm
    };
    let local_job_id = local.then(generate_local_job_id);

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    }

    if !no_preflight {
        let report = run_preflight(
            &runtime_plan,
            &PreflightOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                sbatch_bin: context.binaries.sbatch.value.clone(),
                srun_bin: context.binaries.srun.value.clone(),
                scontrol_bin: "scontrol".to_string(),
                require_submit_tools: !local,
                skip_prepare,
            },
        );
        output::print_report(&report, false);
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before submitting");
        }
    }

    if !skip_prepare {
        let summary = prepare_runtime_plan(
            &runtime_plan,
            &PrepareOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                keep_failed_prep,
                force_rebuild,
            },
        )?;
        if output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = if let Some(job_id) = local_job_id.as_deref() {
        render_local_script(&runtime_plan, job_id, &context.binaries.enroot.value)?
    } else {
        render_script(&runtime_plan)?
    };
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
        let record = build_submission_record_with_backend(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            local_job_id
                .as_deref()
                .context("missing synthetic local job id")?,
            SubmissionBackend::Local,
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
                println!(
                    "tracked job metadata: {}",
                    hpc_compose::job::latest_record_path_for(&record.compose_file).display()
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
                        tracked_metadata_path: Some(hpc_compose::job::latest_record_path_for(
                            &record.compose_file,
                        )),
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

    let output_result = Command::new(&context.binaries.sbatch.value)
        .arg(&script_path)
        .output()
        .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            String::from_utf8_lossy(&output_result.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout);
    let tracked_submission = if let Some(job_id) = output::extract_job_id(stdout.trim()) {
        let record =
            build_submission_record(&file, &submit_dir, &script_path, &runtime_plan, job_id)?;
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
    let tracked_metadata_path = tracked_submission.as_ref().and_then(|(record, persisted)| {
        persisted.then(|| hpc_compose::job::latest_record_path_for(&record.compose_file))
    });

    match output_format {
        OutputFormat::Text => {
            print!("{stdout}");
            output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
            if let Some((record, persisted)) = tracked_submission.as_ref() {
                if *persisted {
                    println!(
                        "tracked job metadata: {}",
                        hpc_compose::job::latest_record_path_for(&record.compose_file).display()
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
        OutputFormat::Text => output::print_status_snapshot(&snapshot),
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
        StatsOutputFormat::Text => output::print_stats_snapshot(&snapshot),
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
        OutputFormat::Text => output::print_artifact_export_report(&report),
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
        OutputFormat::Text => output::print_ps_snapshot(&snapshot),
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
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = match job_id.as_deref() {
        Some(job_id) => load_submission_record(&context.compose_file.value, Some(job_id)).ok(),
        None => Some(load_submission_record(&context.compose_file.value, None)?),
    };
    let resolved_job_id = record
        .as_ref()
        .map(|record| record.job_id.clone())
        .or(job_id)
        .context("missing job id for cancel")?;

    if record
        .as_ref()
        .is_some_and(|record| record.backend == SubmissionBackend::Local)
    {
        let record = record.as_ref().expect("checked above");
        let cancelled = if let Some(pid) = read_local_supervisor_pid(&record)? {
            kill_pid(pid)
                .with_context(|| format!("failed to cancel local job {resolved_job_id}"))?;
            true
        } else {
            false
        };
        return match output::resolve_output_format(format, false) {
            OutputFormat::Text => {
                if cancelled {
                    println!("cancelled job: {resolved_job_id}");
                } else {
                    println!("local job is not running: {resolved_job_id}");
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
                    })
                    .context("failed to serialize cancel output")?
                );
                Ok(())
            }
        };
    }

    match output::resolve_output_format(format, false) {
        OutputFormat::Text => output::cancel_job(&resolved_job_id, &context.binaries.scancel.value),
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
            println!(
                "{}",
                serde_json::to_string_pretty(&output::CancelOutput {
                    job_id: resolved_job_id,
                    cancelled: true,
                    command_stdout: (!stdout.is_empty()).then_some(stdout),
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
        OutputFormat::Text => output::print_job_inventory_scan(&report, disk_usage),
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
        OutputFormat::Text => output::print_cleanup_report(&report, disk_usage),
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
                sbatch: resolved_string("/definitely/missing-sbatch"),
                srun: resolved_string("/definitely/missing-srun"),
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
            true,
            None,
        )
        .expect("submit dry run");

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
    }
}
