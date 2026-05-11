use std::fs;
use std::path::Path;

use anyhow::bail;
use anyhow::{Context, Result};
use serde_norway::{Mapping, Value};

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
    let mut value: Value = serde_norway::from_str(&raw)
        .with_context(|| format!("failed to parse YAML at {}", path.display()))?;
    validate_root(&value)?;
    normalize_raw_spec(&mut value)?;
    serde_norway::from_value(value)
        .with_context(|| format!("failed to deserialize spec at {}", path.display()))
}

fn normalize_raw_spec(value: &mut Value) -> Result<()> {
    let root = value
        .as_mapping_mut()
        .context("top-level YAML document must be a mapping")?;
    rename_steps_to_services(root);
    normalize_modules_alias("root", root)?;

    let Some(services) = root
        .get_mut(Value::String("services".into()))
        .and_then(Value::as_mapping_mut)
    else {
        return Ok(());
    };

    for (name, service) in services {
        let service_name = name.as_str().unwrap_or("<non-string>");
        let Some(service_mapping) = service.as_mapping_mut() else {
            continue;
        };
        normalize_modules_alias(&format!("service '{service_name}'"), service_mapping)?;
    }

    Ok(())
}

fn rename_steps_to_services(root: &mut Mapping) {
    let steps_key = Value::String("steps".into());
    let services_key = Value::String("services".into());
    if root.contains_key(&services_key) {
        return;
    }
    if let Some(steps) = root.remove(&steps_key) {
        root.insert(services_key, steps);
    }
}

fn normalize_modules_alias(scope: &str, mapping: &mut Mapping) -> Result<()> {
    let modules_key = Value::String("modules".into());
    let Some(modules) = mapping.remove(&modules_key) else {
        return Ok(());
    };
    validate_module_list(scope, &modules)?;

    let x_env_key = Value::String("x-env".into());
    let x_env = mapping
        .entry(x_env_key)
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    let Some(x_env_mapping) = x_env.as_mapping_mut() else {
        bail!("{scope} sets modules shorthand but x-env is not a mapping");
    };
    x_env_mapping.insert(Value::String("modules".into()), modules);
    Ok(())
}

fn validate_module_list(scope: &str, value: &Value) -> Result<()> {
    let Value::Sequence(items) = value else {
        bail!("{scope} modules shorthand must be a list of strings");
    };
    for (index, item) in items.iter().enumerate() {
        if !matches!(item, Value::String(_)) {
            bail!("{scope} modules[{index}] must be a string");
        }
    }
    Ok(())
}
