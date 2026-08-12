use serde::Serialize;

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct LessonListOutput {
    pub(crate) schema_version: u32,
    pub(crate) lessons: Vec<LessonDescriptionOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct LessonDescriptionOutput {
    pub(crate) schema_version: u32,
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) step_count: usize,
    pub(crate) steps: Vec<StepDescriptionOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct StepDescriptionOutput {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) concepts: Vec<String>,
    pub(crate) source_templates: Vec<String>,
}
