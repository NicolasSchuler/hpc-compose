//! Shared path layout helpers for tracked runtime state and submission metadata.

use std::path::{Path, PathBuf};

pub(crate) const METADATA_DIR_NAME: &str = ".hpc-compose";
pub(crate) const JOBS_DIR_NAME: &str = "jobs";
pub(crate) const LATEST_RECORD_FILE_NAME: &str = "latest.json";
pub(crate) const RUN_LATEST_RECORD_FILE_NAME: &str = "latest-run.json";
pub(crate) const ATTEMPTS_DIR_NAME: &str = "attempts";
pub(crate) const LOGS_DIR_NAME: &str = "logs";
pub(crate) const METRICS_DIR_NAME: &str = "metrics";
pub(crate) const ARTIFACTS_DIR_NAME: &str = "artifacts";
pub(crate) const ARTIFACT_PAYLOAD_DIR_NAME: &str = "payload";
pub(crate) const ARTIFACT_MANIFEST_FILE_NAME: &str = "manifest.json";
pub(crate) const STATE_FILE_NAME: &str = "state.json";
pub(crate) const ALLOCATION_DIR_NAME: &str = "allocation";
pub(crate) const PRIMARY_NODE_FILE_NAME: &str = "primary_node";
pub(crate) const NODELIST_FILE_NAME: &str = "nodes.txt";
pub(crate) const RESUME_METADATA_DIR_NAME: &str = "_hpc-compose";

#[must_use]
pub(crate) fn metadata_root_for(spec_path: &Path) -> PathBuf {
    let parent = match spec_path.parent() {
        Some(parent) => parent,
        None => Path::new("."),
    };
    parent.join(METADATA_DIR_NAME)
}

#[must_use]
pub(crate) fn jobs_dir_for(spec_path: &Path) -> PathBuf {
    metadata_root_for(spec_path).join(JOBS_DIR_NAME)
}

#[must_use]
pub(crate) fn latest_record_path_for(spec_path: &Path) -> PathBuf {
    metadata_root_for(spec_path).join(LATEST_RECORD_FILE_NAME)
}

#[must_use]
pub(crate) fn latest_run_record_path_for(spec_path: &Path) -> PathBuf {
    metadata_root_for(spec_path).join(RUN_LATEST_RECORD_FILE_NAME)
}

#[must_use]
pub(crate) fn runtime_job_root(submit_dir: &Path, job_id: &str) -> PathBuf {
    submit_dir.join(METADATA_DIR_NAME).join(job_id)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempts_dir(job_root: &Path) -> PathBuf {
    job_root.join(ATTEMPTS_DIR_NAME)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempt_root(job_root: &Path, attempt: u32) -> PathBuf {
    attempts_dir(job_root).join(attempt.to_string())
}

#[must_use]
pub(crate) fn latest_logs_dir(job_root: &Path) -> PathBuf {
    job_root.join(LOGS_DIR_NAME)
}

#[must_use]
pub(crate) fn latest_metrics_dir(job_root: &Path) -> PathBuf {
    job_root.join(METRICS_DIR_NAME)
}

#[must_use]
pub(crate) fn latest_artifacts_dir(job_root: &Path) -> PathBuf {
    job_root.join(ARTIFACTS_DIR_NAME)
}

#[must_use]
pub(crate) fn latest_state_path(job_root: &Path) -> PathBuf {
    job_root.join(STATE_FILE_NAME)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempt_logs_dir(attempt_root: &Path) -> PathBuf {
    attempt_root.join(LOGS_DIR_NAME)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempt_metrics_dir(attempt_root: &Path) -> PathBuf {
    attempt_root.join(METRICS_DIR_NAME)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempt_artifacts_dir(attempt_root: &Path) -> PathBuf {
    attempt_root.join(ARTIFACTS_DIR_NAME)
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn attempt_state_path(attempt_root: &Path) -> PathBuf {
    attempt_root.join(STATE_FILE_NAME)
}

#[must_use]
pub(crate) fn artifact_manifest_path(artifacts_dir: &Path) -> PathBuf {
    artifacts_dir.join(ARTIFACT_MANIFEST_FILE_NAME)
}

#[must_use]
pub(crate) fn artifact_payload_dir(artifacts_dir: &Path) -> PathBuf {
    artifacts_dir.join(ARTIFACT_PAYLOAD_DIR_NAME)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn compose_level_metadata_paths_match_expected_layout() {
        let spec_path = Path::new("/tmp/project/compose.yaml");

        assert_eq!(
            metadata_root_for(spec_path),
            Path::new("/tmp/project/.hpc-compose")
        );
        assert_eq!(
            jobs_dir_for(spec_path),
            Path::new("/tmp/project/.hpc-compose/jobs")
        );
        assert_eq!(
            latest_record_path_for(spec_path),
            Path::new("/tmp/project/.hpc-compose/latest.json")
        );
        assert_eq!(
            latest_run_record_path_for(spec_path),
            Path::new("/tmp/project/.hpc-compose/latest-run.json")
        );
    }

    #[test]
    fn runtime_job_paths_cover_latest_and_attempt_views() {
        let job_root = runtime_job_root(Path::new("/submit"), "12345");
        let attempt_root = attempt_root(&job_root, 2);

        assert_eq!(job_root, Path::new("/submit/.hpc-compose/12345"));
        assert_eq!(
            attempts_dir(&job_root),
            Path::new("/submit/.hpc-compose/12345/attempts")
        );
        assert_eq!(
            attempt_root,
            Path::new("/submit/.hpc-compose/12345/attempts/2")
        );

        assert_eq!(
            latest_logs_dir(&job_root),
            Path::new("/submit/.hpc-compose/12345/logs")
        );
        assert_eq!(
            latest_metrics_dir(&job_root),
            Path::new("/submit/.hpc-compose/12345/metrics")
        );
        assert_eq!(
            latest_artifacts_dir(&job_root),
            Path::new("/submit/.hpc-compose/12345/artifacts")
        );
        assert_eq!(
            latest_state_path(&job_root),
            Path::new("/submit/.hpc-compose/12345/state.json")
        );

        assert_eq!(
            attempt_logs_dir(&attempt_root),
            Path::new("/submit/.hpc-compose/12345/attempts/2/logs")
        );
        assert_eq!(
            attempt_metrics_dir(&attempt_root),
            Path::new("/submit/.hpc-compose/12345/attempts/2/metrics")
        );
        assert_eq!(
            attempt_artifacts_dir(&attempt_root),
            Path::new("/submit/.hpc-compose/12345/attempts/2/artifacts")
        );
        assert_eq!(
            attempt_state_path(&attempt_root),
            Path::new("/submit/.hpc-compose/12345/attempts/2/state.json")
        );
        assert_eq!(
            artifact_manifest_path(&latest_artifacts_dir(&job_root)),
            Path::new("/submit/.hpc-compose/12345/artifacts/manifest.json")
        );
        assert_eq!(
            artifact_payload_dir(&latest_artifacts_dir(&job_root)),
            Path::new("/submit/.hpc-compose/12345/artifacts/payload")
        );
    }
}
