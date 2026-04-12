use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_norway::Value;

use super::ComposeSpec;
use super::validation::validate_root;

pub(super) fn load_raw_spec(path: &Path) -> Result<ComposeSpec> {
    let raw =
        fs::read_to_string(path).context(format!("failed to read spec at {}", path.display()))?;
    let value: Value = serde_norway::from_str(&raw)
        .context(format!("failed to parse YAML at {}", path.display()))?;
    validate_root(&value)?;
    serde_norway::from_value(value)
        .context(format!("failed to deserialize spec at {}", path.display()))
}
