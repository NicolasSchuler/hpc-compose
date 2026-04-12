use super::scheduler::build_status_snapshot;
use super::*;

/// Compose-style per-service snapshot returned by `ps`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsSnapshot {
    pub record: SubmissionRecord,
    pub scheduler: SchedulerStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_diagnostics: Option<QueueDiagnostics>,
    pub log_dir: PathBuf,
    pub services: Vec<PsServiceRow>,
    pub attempt: Option<u32>,
    pub is_resume: Option<bool>,
    pub resume_dir: Option<PathBuf>,
}

/// Builds the tracked per-service snapshot used by `hpc-compose ps`.
pub fn build_ps_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
) -> Result<PsSnapshot> {
    let status = build_status_snapshot(spec_path, job_id, options)?;
    Ok(PsSnapshot {
        record: status.record,
        scheduler: status.scheduler,
        queue_diagnostics: status.queue_diagnostics,
        log_dir: status.log_dir,
        services: status.services,
        attempt: status.attempt,
        is_resume: status.is_resume,
        resume_dir: status.resume_dir,
    })
}
