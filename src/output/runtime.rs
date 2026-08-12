#![allow(unused_imports)]

use std::path::{Path, PathBuf};

use hpc_compose::job::{ArtifactManifest, EfficiencyScoreReport, JobNote, JobProvenance};
use hpc_compose::when::WhenConditionSummary;
use serde::Serialize;

use crate::output;

pub(crate) use super::{
    CancelOutput, SubmitOutput, finish_watch, print_artifact_export_report, print_cleanup_report,
    print_job_diff_report, print_job_inventory_scan, print_ps_snapshot, print_stats_snapshot,
    print_status_snapshot, print_submit_details, write_stats_snapshot_csv,
    write_stats_snapshot_jsonl,
};

/// Machine-readable output for `pull --format json`.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct PullOutput {
    pub(crate) schema_version: u32,
    pub(crate) job_id: String,
    pub(crate) bundles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) login_host: Option<String>,
    pub(crate) cluster_path: String,
    pub(crate) into: String,
    pub(crate) files: usize,
    pub(crate) bytes: u64,
    pub(crate) suggested_command: String,
    pub(crate) ssh_multiplex_hint: String,
}

/// Machine-readable output for `reach --format json`.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ReachOutput {
    pub(crate) schema_version: u32,
    pub(crate) service: String,
    pub(crate) job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) compute_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) login_host: Option<String>,
    pub(crate) local_port: u16,
    pub(crate) remote_port: u16,
    pub(crate) url: String,
    pub(crate) ssh_command: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct RendezvousRegisterOutput {
    pub(crate) schema_version: u32,
    pub(crate) cache_dir: PathBuf,
    pub(crate) record_path: PathBuf,
    pub(crate) record: hpc_compose::rendezvous::RendezvousRecord,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct NotebookDryRunOutput {
    pub(crate) schema_version: u32,
    pub(crate) dry_run: bool,
    pub(crate) submitted: bool,
    pub(crate) kind: String,
    pub(crate) script_path: PathBuf,
    pub(crate) cache_dir: PathBuf,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct GerminateOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) compose_file: &'a Path,
    pub(crate) script_path: &'a Path,
    pub(crate) cache_dir: &'a Path,
    pub(crate) dry_run: bool,
    pub(crate) job_id: Option<&'a str>,
    pub(crate) tracked_metadata_path: Option<PathBuf>,
    pub(crate) yaml_patch: Option<String>,
    pub(crate) report: Option<&'a hpc_compose::job::RightsizeReport>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WhenSubmitOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) triggered: bool,
    // `WhenConditionSummary` (in `hpc_compose::when`) does not derive `JsonSchema`
    // and is outside this task's editable scope, so describe the array as
    // permissive JSON values in the published schema. Serde output is unchanged.
    #[schemars(with = "Vec<serde_json::Value>")]
    pub(crate) conditions: &'a [WhenConditionSummary],
    pub(crate) submission: &'a output::SubmitOutput,
}

/// Machine-readable form of [`NotebookConnection`] for `--format json`.
///
/// Mirrors the human-readable output. `compute_node` and `login_host` are the
/// resolved hosts used to render the tunnel hint; they are descriptive only —
/// nothing here opens a connection.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct NotebookConnectionOutput {
    /// Version of this output document's schema.
    pub(crate) schema_version: u32,
    /// The URL to open (localhost for Jupyter, scraped link for VS Code).
    pub(crate) url: String,
    /// SSH tunnel hint, when one is needed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tunnel_hint: Option<String>,
    /// Resolved compute node the server runs on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) compute_node: Option<String>,
    /// Resolved SSH login/jump host.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) login_host: Option<String>,
    /// Tracked Slurm (or local) job id.
    pub(crate) job_id: String,
    /// Suggested follow-up commands an agent can run next.
    pub(crate) next_commands: Vec<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct DebugLogTail {
    pub(crate) service_name: Option<String>,
    pub(crate) path: PathBuf,
    pub(crate) present: bool,
    pub(crate) lines: Vec<String>,
    pub(crate) note: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct DebugSummary {
    pub(crate) scheduler_state: Option<String>,
    pub(crate) failed_service: Option<String>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) log_path: Option<PathBuf>,
    pub(crate) next_command: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct DebugReport {
    pub(crate) schema_version: u32,
    pub(crate) tracked: bool,
    pub(crate) compose_file: PathBuf,
    pub(crate) job_id: Option<String>,
    pub(crate) summary: DebugSummary,
    pub(crate) status: Option<hpc_compose::job::StatusSnapshot>,
    pub(crate) ps: Option<hpc_compose::job::PsSnapshot>,
    pub(crate) batch_log: Option<DebugLogTail>,
    pub(crate) service_logs: Vec<DebugLogTail>,
    pub(crate) notes: Vec<String>,
    pub(crate) recommendation: String,
    pub(crate) preflight: Option<serde_json::Value>,
}

/// One JSON object aggregating the read-only state of a single tracked run.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentShowOutput {
    pub(crate) schema_version: u32,
    pub(crate) job_id: String,
    pub(crate) name: String,
    pub(crate) state: String,
    pub(crate) services: Vec<ExperimentService>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provenance: Option<JobProvenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) results: Option<ArtifactManifest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) efficiency: Option<EfficiencyScoreReport>,
    /// User-assigned labels on the tracked record (see `experiment tag`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<String>,
    /// Append-only timestamped observations (see `experiment note`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) notes: Vec<JobNote>,
    pub(crate) next_commands: Vec<String>,
}

/// `experiment tag` result (`--format json`): the record's full tag set after
/// the change.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentTagOutput {
    pub(crate) schema_version: u32,
    pub(crate) job_id: String,
    pub(crate) tags: Vec<String>,
}

/// `experiment note` result (`--format json`): the record's full note list
/// after the append.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentNoteOutput {
    pub(crate) schema_version: u32,
    pub(crate) job_id: String,
    pub(crate) notes: Vec<JobNote>,
}

/// Per-service slice of the aggregate: tracked placement plus a printable tunnel
/// hint when the service exposes a TCP/HTTP readiness port.
#[derive(Debug, Serialize, PartialEq, Eq, schemars::JsonSchema)]
pub(crate) struct ExperimentService {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nodelist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tunnel_hint: Option<String>,
}
