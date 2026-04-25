use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Metadata persisted for a submitted job tracked under `.hpc-compose/`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionRecord {
    pub schema_version: u32,
    #[serde(default = "default_submission_backend")]
    pub backend: SubmissionBackend,
    #[serde(default = "default_submission_kind")]
    pub kind: SubmissionKind,
    pub job_id: String,
    pub submitted_at: u64,
    pub compose_file: PathBuf,
    pub submit_dir: PathBuf,
    pub script_path: PathBuf,
    pub cache_dir: PathBuf,
    pub batch_log: PathBuf,
    pub service_logs: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub artifact_export_dir: Option<String>,
    #[serde(default)]
    pub resume_dir: Option<PathBuf>,
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub command_override: Option<Vec<String>>,
    #[serde(default)]
    pub requested_walltime: Option<RequestedWalltime>,
    #[serde(default)]
    pub config_snapshot_yaml: Option<String>,
    #[serde(default)]
    pub cached_artifacts: Vec<PathBuf>,
}

/// Backend used to execute a tracked submission.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionBackend {
    /// The job was submitted to Slurm.
    #[default]
    Slurm,
    /// The job was launched locally without Slurm.
    Local,
}

/// High-level submission flow used to create a tracked job.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionKind {
    /// A normal compose application submission from `up`.
    #[default]
    Main,
    /// A one-off `run` submission scoped to one service.
    Run,
}

/// Parsed requested allocation walltime persisted with a tracked record.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestedWalltime {
    pub original: String,
    pub seconds: u64,
}

/// Optional metadata attached when building a tracked submission record.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default)]
pub struct SubmissionRecordBuildOptions {
    pub kind: SubmissionKind,
    pub service_name: Option<String>,
    pub command_override: Option<Vec<String>>,
    pub requested_walltime: Option<RequestedWalltime>,
    pub config_snapshot_yaml: Option<String>,
    pub cached_artifacts: Vec<PathBuf>,
}

/// Source used to determine scheduler state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerSource {
    /// State came from `squeue`.
    Squeue,
    /// State came from `sacct`.
    Sacct,
    /// No scheduler data was available; only local tracking data exists.
    LocalOnly,
}

fn default_submission_backend() -> SubmissionBackend {
    SubmissionBackend::Slurm
}

fn default_submission_kind() -> SubmissionKind {
    SubmissionKind::Main
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_record_defaults_backend_and_kind_for_legacy_metadata() {
        let raw = r#"{
            "schema_version": 1,
            "job_id": "12345",
            "submitted_at": 42,
            "compose_file": "/tmp/compose.yaml",
            "submit_dir": "/tmp",
            "script_path": "/tmp/job.sbatch",
            "cache_dir": "/tmp/cache",
            "batch_log": "/tmp/slurm-12345.out",
            "service_logs": {}
        }"#;

        let record: SubmissionRecord = serde_json::from_str(raw).expect("legacy record");
        assert_eq!(record.backend, SubmissionBackend::Slurm);
        assert_eq!(record.kind, SubmissionKind::Main);
        assert!(record.cached_artifacts.is_empty());
    }
}
