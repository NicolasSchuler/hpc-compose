use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::context::repo_root_or_cwd;

use super::*;

/// Returns the `.hpc-compose` metadata directory for a compose file.
pub fn metadata_root_for(spec_path: &Path) -> PathBuf {
    tracked_paths::metadata_root_for(spec_path)
}

/// Returns the tracked job-record directory for a compose file.
pub fn jobs_dir_for(spec_path: &Path) -> PathBuf {
    tracked_paths::jobs_dir_for(spec_path)
}

/// Returns the path to the "latest tracked job" record file.
pub fn latest_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_record_path_for(spec_path)
}

/// Returns the path to the "latest tracked run job" record file.
pub fn latest_run_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_run_record_path_for(spec_path)
}

/// Builds and persists a new submission record for a submitted job.
pub fn persist_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    let record = build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )?;
    write_submission_record(&record)?;
    Ok(record)
}

/// Builds the submission record structure for a job without writing it.
pub fn build_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )
}

/// Builds the submission record structure for a job without writing it, with
/// extra tracked metadata.
pub fn build_submission_record_with_options(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    options: &SubmissionRecordBuildOptions,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        options,
    )
}

/// Builds the submission record structure for a job without writing it.
pub fn build_submission_record_with_backend(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    backend: SubmissionBackend,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        backend,
        &SubmissionRecordBuildOptions::default(),
    )
}

/// Builds the submission record structure for a job without writing it, with
/// an explicit backend and extra tracked metadata.
pub fn build_submission_record_with_backend_and_options(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    backend: SubmissionBackend,
    options: &SubmissionRecordBuildOptions,
) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let submit_dir = absolute_path(submit_dir)?;
    let script_path = absolute_path(script_path)?;
    let log_dir =
        tracked_paths::latest_logs_dir(&tracked_paths::runtime_job_root(&submit_dir, job_id));
    let service_logs = plan
        .ordered_services
        .iter()
        .map(|service| {
            (
                service.name.clone(),
                log_dir.join(log_file_name_for_service(&service.name)),
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(SubmissionRecord {
        schema_version: SUBMISSION_SCHEMA_VERSION,
        backend,
        kind: options.kind,
        job_id: job_id.to_string(),
        submitted_at: unix_timestamp_now(),
        compose_file,
        submit_dir: submit_dir.clone(),
        script_path,
        cache_dir: plan.cache_dir.clone(),
        batch_log: batch_log_path_for_backend(plan, &submit_dir, job_id, backend),
        service_logs,
        artifact_export_dir: plan
            .slurm
            .artifacts
            .as_ref()
            .and_then(|artifacts| artifacts.export_dir.clone()),
        resume_dir: plan.slurm.resume_dir().map(PathBuf::from),
        service_name: options.service_name.clone(),
        command_override: options.command_override.clone(),
        requested_walltime: options.requested_walltime.clone(),
        config_snapshot_yaml: options.config_snapshot_yaml.clone(),
        cached_artifacts: options.cached_artifacts.clone(),
    })
}

/// Writes a submission record to the jobs directory and latest pointer.
pub fn write_submission_record(record: &SubmissionRecord) -> Result<()> {
    let jobs_dir = jobs_dir_for(&record.compose_file);
    fs::create_dir_all(&jobs_dir).context(format!("failed to create {}", jobs_dir.display()))?;
    write_json(&jobs_dir.join(format!("{}.json", record.job_id)), record)?;
    let latest_path = match record.kind {
        SubmissionKind::Main => latest_record_path_for(&record.compose_file),
        SubmissionKind::Run => latest_run_record_path_for(&record.compose_file),
    };
    write_json(&latest_path, record)?;
    Ok(())
}

/// Removes one tracked submission record and repairs the latest pointer.
pub fn remove_submission_record(record: &SubmissionRecord) -> Result<()> {
    let record_path = jobs_dir_for(&record.compose_file).join(format!("{}.json", record.job_id));
    remove_path_if_present(&record_path)?;
    remove_path_if_present(&runtime_job_root_for_record(record))?;
    repair_latest_records(&record.compose_file)
}

/// Loads every tracked job record for the given compose file.
pub fn scan_job_records(spec_path: &Path) -> Result<Vec<SubmissionRecord>> {
    let compose_file = absolute_path(spec_path)?;
    Ok(
        scan_job_records_with_paths(&metadata_root_for(&compose_file))?
            .into_iter()
            .map(|(_, record)| record)
            .collect(),
    )
}

/// Removes tracked job metadata older than the given age in days.
pub fn clean_by_age(spec_path: &Path, age_days: u64) -> Result<CleanupReport> {
    let report = build_cleanup_report(spec_path, CleanupMode::Age { age_days }, false, false)?;
    run_cleanup_report(&report)?;
    Ok(report)
}

/// Removes all tracked job metadata except the latest record.
pub fn clean_all_except_latest(spec_path: &Path) -> Result<CleanupReport> {
    let report = build_cleanup_report(spec_path, CleanupMode::AllExceptLatest, false, false)?;
    run_cleanup_report(&report)?;
    Ok(report)
}

/// Scans the repo tree for tracked job records.
pub fn scan_job_inventory(scan_start: &Path, include_disk_usage: bool) -> Result<JobInventoryScan> {
    let scan_root = repo_root_or_cwd(scan_start);
    let now = unix_timestamp_now();
    let mut jobs = Vec::new();
    scan_inventory_recursive(&scan_root, include_disk_usage, now, &mut jobs)?;
    jobs.sort_by(|left, right| {
        right
            .submitted_at
            .cmp(&left.submitted_at)
            .then_with(|| left.compose_file.cmp(&right.compose_file))
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(JobInventoryScan { scan_root, jobs })
}

/// Resolves one tracked record by job id by scanning from the nearest repo
/// root or current directory.
pub fn find_submission_record_in_repo(scan_start: &Path, job_id: &str) -> Result<SubmissionRecord> {
    let inventory = scan_job_inventory(scan_start, false)?;
    let matches = inventory
        .jobs
        .into_iter()
        .filter(|entry| entry.job_id == job_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => bail!(
            "no tracked submission metadata exists for job '{}' under {}",
            job_id,
            inventory.scan_root.display()
        ),
        [entry] => read_json(&entry.record_path),
        _ => bail!(
            "multiple tracked submissions with job id '{}' were found under {}; pass -f/--file to disambiguate",
            job_id,
            inventory.scan_root.display()
        ),
    }
}

/// Builds the tracked-job cleanup report for a compose context.
pub fn build_cleanup_report(
    spec_path: &Path,
    mode: CleanupMode,
    include_disk_usage: bool,
    dry_run: bool,
) -> Result<CleanupReport> {
    let compose_file = absolute_path(spec_path)?;
    let metadata_root = metadata_root_for(&compose_file);
    let now = unix_timestamp_now();
    let latest_pointer_job_id_before =
        read_latest_pointer_job_id(&metadata_root, SubmissionKind::Main);
    let inventory =
        build_inventory_entries_for_metadata_root(&metadata_root, include_disk_usage, now)?;
    let latest_job_id_before = inventory
        .iter()
        .find(|entry| entry.kind == SubmissionKind::Main && entry.is_latest)
        .map(|entry| entry.job_id.clone());
    let cutoff = match mode {
        CleanupMode::Age { age_days } => Some(now.saturating_sub(age_days * 86_400)),
        CleanupMode::AllExceptLatest => None,
    };

    let jobs = inventory
        .into_iter()
        .map(|entry| {
            let selected = match mode {
                CleanupMode::Age { .. } => entry.submitted_at < cutoff.unwrap_or(0),
                CleanupMode::AllExceptLatest => !entry.is_latest,
            };
            let removable_paths = removable_paths_for_job(&entry);
            let bytes_reclaimed = if selected && include_disk_usage {
                entry.disk_usage_bytes
            } else {
                None
            };
            CleanupJobReport {
                inventory: entry,
                selected,
                bytes_reclaimed,
                removable_paths,
            }
        })
        .collect::<Vec<_>>();

    let removed_job_ids = jobs
        .iter()
        .filter(|job| job.selected)
        .map(|job| job.inventory.job_id.clone())
        .collect::<Vec<_>>();
    let kept_job_ids = jobs
        .iter()
        .filter(|job| !job.selected)
        .map(|job| job.inventory.job_id.clone())
        .collect::<Vec<_>>();
    let latest_job_id_after = jobs
        .iter()
        .filter(|job| !job.selected && job.inventory.kind == SubmissionKind::Main)
        .max_by(|left, right| compare_records(&left.inventory, &right.inventory))
        .map(|job| job.inventory.job_id.clone());
    let total_bytes_reclaimed = if include_disk_usage {
        Some(
            jobs.iter()
                .filter_map(|job| job.bytes_reclaimed)
                .fold(0_u64, u64::saturating_add),
        )
    } else {
        None
    };

    Ok(CleanupReport {
        compose_file,
        mode: cleanup_mode_label(mode).to_string(),
        dry_run,
        removed_job_ids,
        kept_job_ids,
        latest_pointer_job_id_before,
        latest_job_id_before,
        latest_job_id_after,
        total_bytes_reclaimed,
        jobs,
    })
}

/// Executes the tracked-job cleanup report generated by [`build_cleanup_report`].
pub fn run_cleanup_report(report: &CleanupReport) -> Result<()> {
    for job in report.jobs.iter().filter(|job| job.selected) {
        for path in &job.removable_paths {
            remove_path_if_present(path)?;
        }
    }
    repair_latest_records(&report.compose_file)
}

/// Loads one tracked submission record, defaulting to the latest job.
pub fn load_submission_record(spec_path: &Path, job_id: Option<&str>) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let path = match job_id {
        Some(job_id) => jobs_dir_for(&compose_file).join(format!("{job_id}.json")),
        None => latest_record_path_for(&compose_file),
    };
    if !path.exists() {
        if let Some(job_id) = job_id {
            bail!(
                "no tracked submission metadata exists for job '{}' under {}; run 'hpc-compose submit' for {} first",
                job_id,
                metadata_root_for(&compose_file).display(),
                compose_file.display()
            );
        }
        bail!(
            "no tracked submission metadata exists for {}; run 'hpc-compose submit' first",
            compose_file.display()
        );
    }
    read_json(&path)
}

/// Returns the tracked log directory for a submission record.
pub fn log_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    record
        .service_logs
        .values()
        .next()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| {
            tracked_paths::latest_logs_dir(&tracked_paths::runtime_job_root(
                &record.submit_dir,
                &record.job_id,
            ))
        })
}

/// Returns the tracked runtime root for a submission record.
pub fn runtime_job_root_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::runtime_job_root(&record.submit_dir, &record.job_id)
}

/// Returns the tracked runtime state path for a submission record.
pub fn state_path_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::latest_state_path(&runtime_job_root_for_record(record))
}

fn scan_inventory_recursive(
    root: &Path,
    include_disk_usage: bool,
    now: u64,
    jobs: &mut Vec<JobInventoryEntry>,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(root).context(format!("failed to read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .context(format!("failed to stat {}", path.display()))?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == ".git" || name == "target" {
            continue;
        }
        if name == tracked_paths::METADATA_DIR_NAME {
            jobs.extend(build_inventory_entries_for_metadata_root(
                &path,
                include_disk_usage,
                now,
            )?);
            continue;
        }
        scan_inventory_recursive(&path, include_disk_usage, now, jobs)?;
    }

    Ok(())
}

fn build_inventory_entries_for_metadata_root(
    metadata_root: &Path,
    include_disk_usage: bool,
    now: u64,
) -> Result<Vec<JobInventoryEntry>> {
    let records = scan_job_records_with_paths(metadata_root)?;
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let latest_main_job_id = resolved_latest_job_id(metadata_root, &records, SubmissionKind::Main);
    let latest_run_job_id = resolved_latest_job_id(metadata_root, &records, SubmissionKind::Run);
    let mut inventory = Vec::with_capacity(records.len());
    for (record_path, record) in records {
        let runtime_job_root = tracked_paths::runtime_job_root(&record.submit_dir, &record.job_id);
        let legacy_runtime_job_root = metadata_root.join(&record.job_id);
        let removable_paths =
            removable_paths_from_paths(&record_path, &runtime_job_root, &legacy_runtime_job_root);
        let disk_usage_bytes = if include_disk_usage {
            Some(size_of_paths(&removable_paths)?)
        } else {
            None
        };
        inventory.push(JobInventoryEntry {
            compose_file: record.compose_file.clone(),
            compose_metadata_root: metadata_root.to_path_buf(),
            job_id: record.job_id.clone(),
            kind: record.kind,
            is_latest: match record.kind {
                SubmissionKind::Main => {
                    latest_main_job_id.as_deref() == Some(record.job_id.as_str())
                }
                SubmissionKind::Run => latest_run_job_id.as_deref() == Some(record.job_id.as_str()),
            },
            submitted_at: record.submitted_at,
            age_seconds: now.saturating_sub(record.submitted_at),
            submit_dir: record.submit_dir.clone(),
            record_path,
            runtime_job_root_present: runtime_job_root.exists(),
            runtime_job_root,
            legacy_runtime_job_root_present: legacy_runtime_job_root.exists(),
            legacy_runtime_job_root,
            disk_usage_bytes,
        });
    }

    Ok(inventory)
}

fn scan_job_records_with_paths(metadata_root: &Path) -> Result<Vec<(PathBuf, SubmissionRecord)>> {
    let jobs_dir = metadata_root.join(tracked_paths::JOBS_DIR_NAME);
    if !jobs_dir.is_dir() {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    for entry in
        fs::read_dir(&jobs_dir).context(format!("failed to read {}", jobs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if let Ok(record) = read_json::<SubmissionRecord>(&path) {
            records.push((path, record));
        }
    }
    Ok(records)
}

fn read_latest_pointer_job_id(metadata_root: &Path, kind: SubmissionKind) -> Option<String> {
    let latest_path = match kind {
        SubmissionKind::Main => metadata_root.join(tracked_paths::LATEST_RECORD_FILE_NAME),
        SubmissionKind::Run => metadata_root.join(tracked_paths::RUN_LATEST_RECORD_FILE_NAME),
    };
    read_json::<SubmissionRecord>(&latest_path)
        .ok()
        .map(|record| record.job_id)
}

fn resolved_latest_job_id(
    metadata_root: &Path,
    records: &[(PathBuf, SubmissionRecord)],
    kind: SubmissionKind,
) -> Option<String> {
    let filtered = records
        .iter()
        .filter(|(_, record)| record.kind == kind)
        .collect::<Vec<_>>();
    let newest = filtered
        .iter()
        .max_by(|(_, left), (_, right)| compare_submission_records(left, right))?;
    let latest_path = match kind {
        SubmissionKind::Main => metadata_root.join(tracked_paths::LATEST_RECORD_FILE_NAME),
        SubmissionKind::Run => metadata_root.join(tracked_paths::RUN_LATEST_RECORD_FILE_NAME),
    };
    if latest_path.exists()
        && let Ok(latest) = read_json::<SubmissionRecord>(&latest_path)
        && let Some((_, pointed_record)) = filtered
            .iter()
            .find(|(_, record)| record.job_id == latest.job_id)
        && compare_submission_records(pointed_record, &newest.1) != Ordering::Less
    {
        return Some(latest.job_id);
    }

    Some(newest.1.job_id.clone())
}

fn cleanup_mode_label(mode: CleanupMode) -> &'static str {
    match mode {
        CleanupMode::Age { .. } => "age",
        CleanupMode::AllExceptLatest => "all_except_latest",
    }
}

fn compare_records(left: &JobInventoryEntry, right: &JobInventoryEntry) -> Ordering {
    left.submitted_at
        .cmp(&right.submitted_at)
        .then_with(|| left.job_id.cmp(&right.job_id))
}

fn compare_submission_records(left: &SubmissionRecord, right: &SubmissionRecord) -> Ordering {
    left.submitted_at
        .cmp(&right.submitted_at)
        .then_with(|| left.job_id.cmp(&right.job_id))
}

fn removable_paths_for_job(job: &JobInventoryEntry) -> Vec<PathBuf> {
    removable_paths_from_paths(
        &job.record_path,
        &job.runtime_job_root,
        &job.legacy_runtime_job_root,
    )
}

fn removable_paths_from_paths(
    record_path: &Path,
    runtime_job_root: &Path,
    legacy_runtime_job_root: &Path,
) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for path in [record_path, runtime_job_root, legacy_runtime_job_root] {
        let normalized = normalize_path(path.to_path_buf());
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    out
}

fn size_of_paths(paths: &[PathBuf]) -> Result<u64> {
    paths.iter().try_fold(0_u64, |total, path| {
        Ok(total.saturating_add(size_of_path(path)?))
    })
}

fn size_of_path(path: &Path) -> Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).context(format!("failed to stat {}", path.display())),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() || file_type.is_file() {
        return Ok(metadata.len());
    }
    if !file_type.is_dir() {
        return Ok(0);
    }

    let mut total = metadata.len();
    for entry in fs::read_dir(path).context(format!("failed to read {}", path.display()))? {
        let entry = entry?;
        total = total.saturating_add(size_of_path(&entry.path())?);
    }
    Ok(total)
}

fn remove_path_if_present(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context(format!("failed to stat {}", path.display())),
    };

    let file_type = metadata.file_type();
    if file_type.is_dir() && !file_type.is_symlink() {
        fs::remove_dir_all(path).context(format!("failed to remove {}", path.display()))?;
    } else {
        fs::remove_file(path).context(format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

fn repair_latest_records(compose_file: &Path) -> Result<()> {
    let records = scan_job_records(compose_file)?;
    repair_latest_record_for_kind(compose_file, &records, SubmissionKind::Main)?;
    repair_latest_record_for_kind(compose_file, &records, SubmissionKind::Run)
}

fn repair_latest_record_for_kind(
    compose_file: &Path,
    records: &[SubmissionRecord],
    kind: SubmissionKind,
) -> Result<()> {
    let latest_path = match kind {
        SubmissionKind::Main => latest_record_path_for(compose_file),
        SubmissionKind::Run => latest_run_record_path_for(compose_file),
    };
    if let Some(latest) = records
        .iter()
        .filter(|record| record.kind == kind)
        .max_by(|left, right| compare_submission_records(left, right))
    {
        write_json(&latest_path, latest)
    } else if latest_path.exists() {
        fs::remove_file(&latest_path).context(format!("failed to remove {}", latest_path.display()))
    } else {
        Ok(())
    }
}
