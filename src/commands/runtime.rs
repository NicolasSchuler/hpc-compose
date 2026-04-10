use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{OutputFormat, StatsOutputFormat};
use hpc_compose::context::ResolvedContext;
use hpc_compose::job::{
    ArtifactExportOptions, CleanupMode, SchedulerOptions, StatsOptions, build_cleanup_report,
    build_stats_snapshot, build_status_snapshot, build_submission_record, export_artifacts,
    load_submission_record, print_logs, run_cleanup_report, scan_job_inventory, watch_submission,
    write_submission_record,
};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, prepare_runtime_plan};
use hpc_compose::render::render_script;

use crate::output;

#[allow(clippy::too_many_arguments)]
pub(crate) fn submit(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    keep_failed_prep: bool,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    watch: bool,
    dry_run: bool,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;

    if !no_preflight {
        let report = run_preflight(
            &runtime_plan,
            &PreflightOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                sbatch_bin: context.binaries.sbatch.value.clone(),
                srun_bin: context.binaries.srun.value.clone(),
                scontrol_bin: "scontrol".to_string(),
                require_submit_tools: true,
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
        output::print_prepare_summary(&summary);
    }

    let script = render_script(&runtime_plan)?;
    let script_path = script_out.unwrap_or_else(|| output::default_script_path(&file));
    fs::write(&script_path, script).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        println!("  script: {}", script_path.display());
        println!("  cache:  {}", runtime_plan.cache_dir.display());
        println!("dry run: skipping sbatch submission");
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
    print!("{stdout}");
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
    if watch {
        let Some((record, _)) = tracked_submission.as_ref() else {
            println!("note: skipping watch because the submission is not trackable");
            return Ok(());
        };
        output::finish_watch(
            &record.job_id,
            watch_submission(
                record,
                &SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
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

pub(crate) fn cancel(context: ResolvedContext, job_id: Option<String>) -> Result<()> {
    let resolved_job_id = match job_id {
        Some(job_id) => job_id,
        None => load_submission_record(&context.compose_file.value, None)?.job_id,
    };
    output::cancel_job(&resolved_job_id, &context.binaries.scancel.value)
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
