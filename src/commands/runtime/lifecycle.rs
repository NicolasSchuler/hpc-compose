use super::*;

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
        eprintln!(
            "exported tracked artifacts to {} before teardown (pass --no-export to skip)",
            export_dir.display()
        );
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
    match output::resolve_output_format(format) {
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
    match output::resolve_output_format(format) {
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
