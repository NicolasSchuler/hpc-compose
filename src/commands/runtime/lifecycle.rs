use std::env;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::{ResolvedContext, ValueSource};
use hpc_compose::job::{
    ArtifactExportOptions, CleanupMode, CleanupReport, SubmissionBackend, SubmissionRecord,
    build_cleanup_report, build_deep_cleanup_report, export_artifacts, remove_submission_record,
    run_cleanup_report, run_deep_cleanup_report, runtime_job_root_for_record, scan_job_inventory,
};

use super::{
    cached_artifacts_for_teardown, kill_pid_if_running, latest_record_path, purge_cached_artifacts,
    read_local_supervisor_pid, resolve_tracked_record,
};
use crate::commands::load;
use crate::output;

/// Exports tracked artifacts to the configured `export_dir` before teardown reaps
/// the runtime payload, returning the export directory when an export ran. No-ops
/// when `--no-export` is set, no record was found, export is not configured, or
/// the teardown manifest is absent (nothing collected yet). Failing to export is
/// an error so the destructive reap does not silently discard results — the user
/// can retry or pass `--no-export`.
fn maybe_auto_export_artifacts(
    record: Option<&SubmissionRecord>,
    no_export: bool,
) -> Result<Option<std::path::PathBuf>> {
    if no_export {
        return Ok(None);
    }
    let Some(record) = record else {
        return Ok(None);
    };
    if record.artifact_export_dir.is_none()
        || !crate::job::artifact_manifest_path_for_record(record).exists()
    {
        return Ok(None);
    }
    let report = export_artifacts(
        &record.compose_file,
        Some(&record.job_id),
        &ArtifactExportOptions::default(),
    )
    .with_context(|| {
        format!(
            "failed to auto-export artifacts for job {} before teardown; re-run `hpc-compose artifacts` or pass --no-export to tear down without exporting",
            record.job_id
        )
    })?;
    Ok(Some(report.export_dir))
}

pub(crate) fn cancel(
    context: ResolvedContext,
    job_id: Option<String>,
    purge_cache: bool,
    no_export: bool,
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

    // Export tracked artifacts before remove_submission_record (below) reaps the
    // runtime root and its collected payload. Without this, a job with
    // x-slurm.artifacts.export_dir configured silently loses its results if the
    // user forgets to run `hpc-compose artifacts` first. --no-export opts out.
    if let Some(export_dir) = maybe_auto_export_artifacts(record.as_ref(), no_export)? {
        hpc_compose::diagnostics::notice(format!(
            "exported tracked artifacts to {} before teardown (pass --no-export to skip)",
            export_dir.display()
        ));
    }

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
        return match output::resolve_output_format(format) {
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
                    crate::output::to_pretty_json(&output::CancelOutput {
                        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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

    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            crate::job::cancel_job(&resolved_job_id, &context.binaries.scancel.value)?;
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
                crate::output::to_pretty_json(&output::CancelOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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

pub(crate) fn cancel_confirmation_details(
    context: &ResolvedContext,
    job_id: Option<&str>,
    purge_cache: bool,
    no_export: bool,
) -> Result<Vec<String>> {
    let record = resolve_tracked_record(context, job_id)?;
    let resolved_job_id = record
        .as_ref()
        .map(|record| record.job_id.clone())
        .or_else(|| job_id.map(ToOwned::to_owned))
        .context("missing job id for cancel")?;
    let scheduler_action = match record.as_ref().map(|record| record.backend) {
        Some(SubmissionBackend::Local) => "local supervisor cancellation".to_string(),
        _ => format!("scancel {}", resolved_job_id),
    };
    let mut details = vec![
        format!("job id: {resolved_job_id}"),
        format!("scheduler action: {scheduler_action}"),
    ];
    if let Some(record) = record.as_ref() {
        details.push(format!(
            "tracked metadata: {}",
            latest_record_path(record).display()
        ));
        details.push(format!("compose file: {}", record.compose_file.display()));
        details.push(format!("submit dir: {}", record.submit_dir.display()));
        details.push(format!(
            "runtime root: {}",
            runtime_job_root_for_record(record).display()
        ));
        if no_export {
            details.push("artifact export: skipped by --no-export".to_string());
        } else if record.artifact_export_dir.is_some() {
            details.push("artifact export: auto-export if collected artifacts exist".to_string());
        } else {
            details.push("artifact export: not configured".to_string());
        }
    } else {
        details.push("tracked metadata: not found".to_string());
        details.push("artifact export: unavailable without tracked metadata".to_string());
    }
    if purge_cache {
        let cache_paths = cached_artifacts_for_teardown(record.as_ref())?;
        details.push(format!("purge cache paths: {}", cache_paths.len()));
        details.push(format!(
            "estimated purge bytes: {}",
            crate::commands::confirm::estimate_paths_bytes(&cache_paths)
        ));
        for path in cache_paths {
            details.push(format!("purge path: {}", path.display()));
        }
    } else {
        details.push("purge cache: no".to_string());
    }
    Ok(details)
}

pub(crate) fn jobs_list(
    disk_usage: bool,
    tags: Vec<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let cwd = env::current_dir().context("failed to determine current working directory")?;
    let mut report = scan_job_inventory(&cwd, disk_usage)?;
    // --tag is an AND filter: keep only jobs carrying every requested tag.
    if !tags.is_empty() {
        report
            .jobs
            .retain(|job| tags.iter().all(|tag| job.tags.iter().any(|t| t == tag)));
    }
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_job_inventory_scan(&report, disk_usage)
                .context("failed to write jobs list output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&output::contract::JobListOutput::new(report))
                    .context("failed to serialize jobs list output")?
            );
        }
    }
    Ok(())
}
#[cfg(test)]
pub(crate) fn clean(
    context: ResolvedContext,
    age: Option<u64>,
    all: bool,
    dry_run: bool,
    deep: bool,
    disk_usage: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let report = build_clean_report(&context, age, all, deep, disk_usage, dry_run)?;
    finish_clean(report, dry_run, deep, disk_usage, format)
}

pub(crate) fn build_clean_report(
    context: &ResolvedContext,
    age: Option<u64>,
    all: bool,
    deep: bool,
    disk_usage: bool,
    dry_run: bool,
) -> Result<CleanupReport> {
    let mode = if let Some(days) = age {
        CleanupMode::Age { age_days: days }
    } else {
        debug_assert!(all);
        CleanupMode::AllExceptLatest
    };
    let report = if deep {
        let cache_dir = active_cleanup_cache_dir(context)?;
        build_deep_cleanup_report(
            &context.compose_file.value,
            &cache_dir,
            mode,
            disk_usage,
            dry_run,
        )?
    } else {
        build_cleanup_report(&context.compose_file.value, mode, disk_usage, dry_run)?
    };
    Ok(report)
}

pub(crate) fn clean_confirmation_details(
    report: &CleanupReport,
    deep: bool,
    disk_usage: bool,
) -> Vec<String> {
    let mut details = vec![
        format!("compose file: {}", report.compose_file.display()),
        format!("mode: {}", report.mode),
        format!("selected jobs: {}", report.removed_job_ids.len()),
        format!(
            "kept latest job: {}",
            report
                .latest_job_id_after
                .as_deref()
                .or(report.latest_job_id_before.as_deref())
                .unwrap_or("<none>")
        ),
    ];
    if !report.removed_job_ids.is_empty() {
        details.push(format!(
            "selected job ids: {}",
            report.removed_job_ids.join(",")
        ));
    }
    if !report.kept_job_ids.is_empty() {
        details.push(format!("kept job ids: {}", report.kept_job_ids.join(",")));
    }
    if disk_usage {
        details.push(format!(
            "estimated bytes: {}",
            report.total_bytes_reclaimed.unwrap_or(0)
        ));
    }
    let selected_path_count = report
        .jobs
        .iter()
        .filter(|job| job.selected)
        .map(|job| job.removable_paths.len())
        .sum::<usize>();
    details.push(format!("selected tracked paths: {selected_path_count}"));
    if deep && let Some(deep) = &report.deep {
        details.push(format!(
            "expired rendezvous records: {}",
            deep.rendezvous.removed.len()
        ));
        details.push(format!(
            "orphan runtime dirs: {}",
            deep.orphan_runtime_dirs
                .iter()
                .filter(|entry| entry.selected)
                .count()
        ));
    }
    details
}

pub(crate) fn finish_clean(
    report: CleanupReport,
    dry_run: bool,
    deep: bool,
    disk_usage: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    if !dry_run {
        if deep {
            run_deep_cleanup_report(&report)?;
        } else {
            run_cleanup_report(&report)?;
        }
    }
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_cleanup_report(&report, disk_usage)
                .context("failed to write clean output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&output::contract::CleanOutput::new(report))
                    .context("failed to serialize clean output")?
            );
        }
    }
    Ok(())
}

fn active_cleanup_cache_dir(context: &ResolvedContext) -> Result<PathBuf> {
    if context.compose_file.source == ValueSource::Builtin && !context.compose_file.value.exists() {
        return Ok(context.cache_dir.value.clone());
    }
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    Ok(runtime_plan.cache_dir)
}
