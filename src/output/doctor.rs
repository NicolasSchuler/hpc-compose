use std::path::Path;

use hpc_compose::cluster::ClusterProfile;
use hpc_compose::diagnostics::GroupedReport;

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub(crate) struct ReadinessDoctorOutput {
    pub(crate) schema_version: u32,
    pub(crate) ok: bool,
    pub(crate) service: String,
    #[serde(rename = "type")]
    pub(crate) probe_type: &'static str,
    pub(crate) mode: &'static str,
    // `ReadinessProbeTarget` (in `hpc_compose::readiness_util`) does not derive
    // `JsonSchema` and is outside this task's editable scope, so describe it as a
    // permissive JSON value in the published schema. Serde output is unchanged.
    #[schemars(with = "serde_json::Value")]
    pub(crate) target: hpc_compose::readiness_util::ReadinessProbeTarget,
    pub(crate) timeout_seconds: u64,
    pub(crate) ran: bool,
    pub(crate) passed: bool,
    pub(crate) elapsed_seconds: Option<f64>,
    pub(crate) diagnostics: Vec<String>,
    pub(crate) next_steps: Vec<String>,
    pub(crate) required_tool: Option<&'static str>,
    pub(crate) generated_behavior: String,
}

/// `doctor --cluster-report --format json` output. Hoisted from a
/// function-local struct so it is a published-schema-ready named DTO.
#[derive(serde::Serialize, schemars::JsonSchema)]
pub(crate) struct ClusterReportJsonOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) path: Option<&'a Path>,
    pub(crate) wrote: bool,
    pub(crate) profile: &'a ClusterProfile,
    pub(crate) diagnostics: GroupedReport,
}
