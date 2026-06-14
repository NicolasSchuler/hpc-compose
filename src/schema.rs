//! JSON Schema surface for hpc-compose compose and settings files.

/// Checked-in JSON Schema for the supported hpc-compose spec surface.
pub const HPC_COMPOSE_SCHEMA_JSON: &str = include_str!("../schema/hpc-compose.schema.json");

/// Checked-in JSON Schema for the hpc-compose settings.toml file.
pub const HPC_COMPOSE_SETTINGS_SCHEMA_JSON: &str =
    include_str!("../schema/hpc-compose-settings.schema.json");

/// Returns the checked-in JSON Schema for compose authoring tools.
#[must_use]
pub fn schema_json() -> &'static str {
    HPC_COMPOSE_SCHEMA_JSON
}

/// Returns the checked-in JSON Schema for settings.toml authoring tools.
#[must_use]
pub fn settings_schema_json() -> &'static str {
    HPC_COMPOSE_SETTINGS_SCHEMA_JSON
}
