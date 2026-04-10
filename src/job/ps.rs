use super::scheduler::build_status_snapshot;
use super::*;

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
