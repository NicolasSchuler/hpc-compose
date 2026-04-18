use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_norway::Value;

use super::ComposeSpec;
use super::validation::validate_root;
use crate::spec_error::SpecError;

pub(super) fn load_raw_spec(path: &Path) -> Result<ComposeSpec> {
    let raw = fs::read_to_string(path).map_err(|source| -> anyhow::Error {
        SpecError::LoadFailed {
            path: path.to_path_buf(),
            source: source.into(),
        }
        .into()
    })?;
    let value: Value = serde_norway::from_str(&raw)
        .with_context(|| format!("failed to parse YAML at {}", path.display()))?;
    validate_root(&value)?;
    serde_norway::from_value(value)
        .with_context(|| format!("failed to deserialize spec at {}", path.display()))
}
