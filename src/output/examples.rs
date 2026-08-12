use hpc_compose::examples::{ExampleInfo, ExampleRecommendation};
use serde::Serialize;

/// `examples list` / `search` / `coverage` JSON output.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExamplesListOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) examples: &'a [ExampleInfo],
}

/// `examples recommend` JSON output.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExamplesRecommendOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) query: Option<&'a str>,
    pub(crate) required_tags: &'a [String],
    pub(crate) safe_authoring_note: &'static str,
    pub(crate) recommendations: &'a [ExampleRecommendation],
}
