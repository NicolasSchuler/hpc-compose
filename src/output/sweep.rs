use std::path::PathBuf;

use hpc_compose::job::{StatsSnapshot, SweepManifest};
use serde::Serialize;

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepSubmitOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) dry_run: bool,
    /// True when this run resumed an existing sweep (`--resume`) rather than
    /// submitting a fresh one.
    pub(crate) resumed: bool,
    /// Number of trials (re)submitted by a resume run. Omitted for a fresh submit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resubmitted: Option<usize>,
    /// Number of trials a resume run left untouched because they already had a
    /// job. Omitted for a fresh submit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) skipped_already_submitted: Option<usize>,
    pub(crate) manifest_path: Option<PathBuf>,
    pub(crate) manifest: &'a SweepManifest,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepListOutput {
    pub(crate) schema_version: u32,
    pub(crate) compose_file: PathBuf,
    pub(crate) sweeps: Vec<SweepManifest>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStopOutput {
    pub(crate) schema_version: u32,
    pub(crate) sweep_id: String,
    pub(crate) cancelled_count: usize,
    pub(crate) skipped_count: usize,
    pub(crate) cancelled_trials: Vec<String>,
    pub(crate) skipped_trials: Vec<String>,
    pub(crate) stopped_at: u64,
    pub(crate) stop_reason: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStatsOutput {
    pub(crate) schema_version: u32,
    pub(crate) sweep_id: String,
    pub(crate) trials: Vec<SweepStatsTrial>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStatsTrial {
    pub(crate) trial_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(crate) config_key: String,
    pub(crate) replicate: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot: Option<StatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}
