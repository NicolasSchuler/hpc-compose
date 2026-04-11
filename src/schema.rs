//! JSON Schema surface for hpc-compose compose files.

/// Checked-in JSON Schema for the supported hpc-compose spec surface.
pub const HPC_COMPOSE_SCHEMA_JSON: &str = include_str!("../schema/hpc-compose.schema.json");

/// Returns the checked-in JSON Schema for compose authoring tools.
#[must_use]
pub fn schema_json() -> &'static str {
    HPC_COMPOSE_SCHEMA_JSON
}
