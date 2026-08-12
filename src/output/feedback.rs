use serde::Serialize;

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct FeedbackOutput {
    pub(crate) schema_version: u32,
    pub(crate) kind: String,
    pub(crate) issue_url: String,
    pub(crate) report: FeedbackReport,
    pub(crate) telemetry_sent: bool,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct FeedbackReport {
    pub(crate) package: String,
    pub(crate) version: String,
    pub(crate) repository: String,
    pub(crate) build_rev: Option<String>,
    pub(crate) build_dirty: bool,
    pub(crate) os: String,
    pub(crate) arch: String,
}
