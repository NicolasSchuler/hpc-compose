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

/// Builds and persists a new submission record for a submitted job.
pub fn persist_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    let record = build_submission_record(spec_path, submit_dir, script_path, plan, job_id)?;
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
        job_id: job_id.to_string(),
        submitted_at: unix_timestamp_now(),
        compose_file,
        submit_dir: submit_dir.clone(),
        script_path,
        cache_dir: plan.cache_dir.clone(),
        batch_log: batch_log_path_for(plan, &submit_dir, job_id),
        service_logs,
        artifact_export_dir: plan
            .slurm
            .artifacts
            .as_ref()
            .and_then(|artifacts| artifacts.export_dir.clone()),
        resume_dir: plan.slurm.resume_dir().map(PathBuf::from),
    })
}

/// Writes a submission record to the jobs directory and latest pointer.
pub fn write_submission_record(record: &SubmissionRecord) -> Result<()> {
    let jobs_dir = jobs_dir_for(&record.compose_file);
    fs::create_dir_all(&jobs_dir).context(format!("failed to create {}", jobs_dir.display()))?;
    write_json(&jobs_dir.join(format!("{}.json", record.job_id)), record)?;
    write_json(&latest_record_path_for(&record.compose_file), record)?;
    Ok(())
}

/// Loads every tracked job record for the given compose file.
pub fn scan_job_records(spec_path: &Path) -> Result<Vec<SubmissionRecord>> {
    let compose_file = absolute_path(spec_path)?;
    let jobs_dir = jobs_dir_for(&compose_file);
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
            records.push(record);
        }
    }
    Ok(records)
}

/// Removes tracked job metadata older than the given age in days.
pub fn clean_by_age(spec_path: &Path, age_days: u64) -> Result<CleanResult> {
    let compose_file = absolute_path(spec_path)?;
    let records = scan_job_records(&compose_file)?;
    let cutoff = unix_timestamp_now().saturating_sub(age_days * 86400);
    let mut removed = Vec::new();
    for record in &records {
        if record.submitted_at < cutoff {
            remove_job_artifacts(&compose_file, &record.job_id)?;
            removed.push(record.job_id.clone());
        }
    }
    Ok(CleanResult {
        removed_jobs: removed,
    })
}

/// Removes all tracked job metadata except the latest record.
pub fn clean_all_except_latest(spec_path: &Path) -> Result<CleanResult> {
    let compose_file = absolute_path(spec_path)?;
    let latest_path = latest_record_path_for(&compose_file);
    let latest_job_id = if latest_path.exists() {
        read_json::<SubmissionRecord>(&latest_path)
            .ok()
            .map(|record| record.job_id)
    } else {
        None
    };

    let records = scan_job_records(&compose_file)?;
    let mut removed = Vec::new();
    for record in &records {
        if latest_job_id.as_deref() == Some(&record.job_id) {
            continue;
        }
        remove_job_artifacts(&compose_file, &record.job_id)?;
        removed.push(record.job_id.clone());
    }
    Ok(CleanResult {
        removed_jobs: removed,
    })
}

fn remove_job_artifacts(compose_file: &Path, job_id: &str) -> Result<()> {
    let jobs_dir = jobs_dir_for(compose_file);
    let record_path = jobs_dir.join(format!("{job_id}.json"));
    if record_path.exists() {
        fs::remove_file(&record_path)
            .context(format!("failed to remove {}", record_path.display()))?;
    }
    let job_dir = metadata_root_for(compose_file).join(job_id);
    if job_dir.is_dir() {
        fs::remove_dir_all(&job_dir).context(format!("failed to remove {}", job_dir.display()))?;
    }
    Ok(())
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
                "no tracked submission metadata exists for job '{}' under {}; run 'hpc-compose submit -f {}' first",
                job_id,
                metadata_root_for(&compose_file).display(),
                compose_file.display()
            );
        }
        bail!(
            "no tracked submission metadata exists for {}; run 'hpc-compose submit -f {}' first",
            compose_file.display(),
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
