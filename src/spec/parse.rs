use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use anyhow::{bail, ensure};
use serde_norway::{Mapping, Value};

use super::ComposeSpec;
use super::validation::validate_root;
use crate::domain::{MountParts, split_mount_parts};
use crate::spec_error::SpecError;

pub(super) fn load_raw_spec(path: &Path) -> Result<ComposeSpec> {
    load_raw_spec_with_root_text(path, None)
}

pub(super) fn load_raw_spec_from_str(path: &Path, raw: &str) -> Result<ComposeSpec> {
    load_raw_spec_with_root_text(path, Some(raw))
}

#[cfg(feature = "fuzzing")]
pub(super) fn load_fuzz_raw_spec_from_str(raw: &str) -> Result<ComposeSpec> {
    let mut value: Value =
        serde_norway::from_str(raw).context("failed to parse YAML from fuzz input")?;
    validate_root(&value)?;
    if value
        .as_mapping()
        .is_some_and(|root| root.contains_key(string_key("extends")))
    {
        bail!("fuzz parser does not resolve root extends");
    }
    normalize_raw_spec(&mut value)?;
    serde_norway::from_value(value).context("failed to deserialize fuzz spec")
}

fn load_raw_spec_with_root_text(path: &Path, root_text: Option<&str>) -> Result<ComposeSpec> {
    let mut stack = Vec::new();
    let mut value = load_resolved_value(path, &mut stack, root_text)?;
    validate_root(&value)?;
    normalize_raw_spec(&mut value)?;
    // Deserialize through a path tracker so a type mismatch names the exact
    // field (e.g. `services.app.x-slurm.nodes`), not just the file.
    serde_path_to_error::deserialize(value).map_err(|err| {
        let field = err.path().to_string();
        let inner = anyhow::Error::from(err.into_inner());
        if field == "." {
            inner.context(format!("failed to deserialize spec at {}", path.display()))
        } else {
            inner.context(format!(
                "failed to deserialize spec at {} (field '{field}')",
                path.display()
            ))
        }
    })
}

fn load_value(path: &Path, is_root: bool, root_text: Option<&str>) -> Result<Value> {
    if is_root && let Some(raw) = root_text {
        return serde_norway::from_str(raw)
            .with_context(|| format!("failed to parse YAML at {}", path.display()));
    }
    let raw = fs::read_to_string(path).map_err(|source| -> anyhow::Error {
        // A missing top-level spec is the most common first-run failure; point at
        // the scaffolding commands. Missing `extends` targets keep the generic load
        // error, since the new/evolve hint would be misleading there.
        if is_root && source.kind() == std::io::ErrorKind::NotFound {
            return SpecError::SpecFileNotFound {
                path: path.to_path_buf(),
            }
            .into();
        }
        SpecError::LoadFailed {
            path: path.to_path_buf(),
            source: source.into(),
        }
        .into()
    })?;
    serde_norway::from_str(&raw)
        .with_context(|| format!("failed to parse YAML at {}", path.display()))
}

fn load_resolved_value(
    path: &Path,
    stack: &mut Vec<PathBuf>,
    root_text: Option<&str>,
) -> Result<Value> {
    let identity = spec_identity(path);
    if stack.contains(&identity) {
        bail!(
            "extends cycle detected while loading {}; stack: {}",
            path.display(),
            stack
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        );
    }
    stack.push(identity);
    let result = (|| {
        // `stack` already holds this path's identity, so len == 1 is the root spec.
        let mut value = load_value(path, stack.len() == 1, root_text)?;
        resolve_top_level_extends(path, &mut value, stack)?;
        normalize_steps_alias(&mut value)?;
        resolve_service_extends(path, &mut value, stack)?;
        Ok(value)
    })();
    stack.pop();
    result
}

fn spec_identity(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn resolve_top_level_extends(
    path: &Path,
    value: &mut Value,
    stack: &mut Vec<PathBuf>,
) -> Result<()> {
    let Some(root) = value.as_mapping_mut() else {
        return Ok(());
    };
    let extends_key = string_key("extends");
    let Some(raw_extends) = root.remove(&extends_key) else {
        return Ok(());
    };
    let Some(base_path) = raw_extends.as_str() else {
        bail!("root extends must be a file path string");
    };
    let base_path = resolve_extends_path(path, base_path);
    let base = load_resolved_value(&base_path, stack, None)
        .with_context(|| format!("failed to resolve root extends {}", base_path.display()))?;
    normalize_steps_alias(value)?;
    let child = std::mem::replace(value, Value::Mapping(Mapping::new()));
    *value = merge_root_values(base, child)?;
    Ok(())
}

fn normalize_steps_alias(value: &mut Value) -> Result<()> {
    let root = value
        .as_mapping_mut()
        .context("top-level YAML document must be a mapping")?;
    rename_steps_to_services(root);
    Ok(())
}

fn resolve_service_extends(path: &Path, value: &mut Value, stack: &mut Vec<PathBuf>) -> Result<()> {
    let Some(root) = value.as_mapping_mut() else {
        return Ok(());
    };
    let Some(services) = root
        .get_mut(string_key("services"))
        .and_then(Value::as_mapping_mut)
    else {
        return Ok(());
    };

    let mut marks = std::collections::BTreeMap::new();
    let names = services
        .keys()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for name in names {
        resolve_service_by_name(path, services, &name, stack, &mut marks)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ServiceMark {
    Temporary,
    Permanent,
}

fn resolve_service_by_name(
    path: &Path,
    services: &mut Mapping,
    name: &str,
    stack: &mut Vec<PathBuf>,
    marks: &mut std::collections::BTreeMap<String, ServiceMark>,
) -> Result<Value> {
    if matches!(marks.get(name), Some(ServiceMark::Permanent)) {
        return services
            .get(string_key(name))
            .cloned()
            .with_context(|| format!("service '{name}' disappeared while resolving extends"));
    }
    if matches!(marks.get(name), Some(ServiceMark::Temporary)) {
        bail!("service extends cycle detected around service '{name}'");
    }
    let service_key = string_key(name);
    let service = services
        .get(&service_key)
        .cloned()
        .with_context(|| format!("service '{name}' not found while resolving extends"))?;
    marks.insert(name.to_string(), ServiceMark::Temporary);
    let resolved = resolve_service_value(path, services, name, service, stack, marks)?;
    services.insert(service_key, resolved.clone());
    marks.insert(name.to_string(), ServiceMark::Permanent);
    Ok(resolved)
}

fn resolve_service_value(
    path: &Path,
    services: &mut Mapping,
    service_name: &str,
    service: Value,
    stack: &mut Vec<PathBuf>,
    marks: &mut std::collections::BTreeMap<String, ServiceMark>,
) -> Result<Value> {
    let Value::Mapping(mut child_mapping) = service else {
        return Ok(service);
    };
    let Some(raw_extends) = child_mapping.remove(string_key("extends")) else {
        return Ok(Value::Mapping(child_mapping));
    };
    let reference = parse_service_extends(service_name, &raw_extends)?;
    let base = match reference {
        ServiceExtendsRef::SameFile { service } => {
            resolve_service_by_name(path, services, &service, stack, marks)?
        }
        ServiceExtendsRef::External { file, service } => {
            let base_path = resolve_extends_path(path, &file);
            let mut base_root =
                load_resolved_value(&base_path, stack, None).with_context(|| {
                    format!(
                        "failed to resolve service '{service_name}' extends {}",
                        base_path.display()
                    )
                })?;
            let base_services = base_root
                .as_mapping_mut()
                .and_then(|root| {
                    root.get_mut(string_key("services"))
                        .and_then(Value::as_mapping_mut)
                })
                .with_context(|| {
                    format!(
                        "service '{service_name}' extends {}, but that file does not define services",
                        base_path.display()
                    )
                })?;
            base_services
                .get(string_key(&service))
                .cloned()
                .with_context(|| {
                    format!(
                        "service '{service_name}' extends service '{service}' from {}, but it was not found",
                        base_path.display()
                    )
                })?
        }
    };
    merge_service_values(base, Value::Mapping(child_mapping))
}

enum ServiceExtendsRef {
    SameFile { service: String },
    External { file: String, service: String },
}

fn parse_service_extends(service_name: &str, value: &Value) -> Result<ServiceExtendsRef> {
    if let Some(raw) = value.as_str() {
        if looks_like_extends_file(raw) {
            return Ok(ServiceExtendsRef::External {
                file: raw.to_string(),
                service: service_name.to_string(),
            });
        }
        return Ok(ServiceExtendsRef::SameFile {
            service: raw.to_string(),
        });
    }
    let Some(mapping) = value.as_mapping() else {
        bail!("service '{service_name}' extends must be a string or mapping");
    };
    let mut file = None;
    let mut service = None;
    for (key, value) in mapping {
        let Some(key) = key.as_str() else {
            bail!("service '{service_name}' extends contains a non-string key");
        };
        match key {
            "file" => {
                let Some(raw) = value.as_str() else {
                    bail!("service '{service_name}' extends.file must be a string");
                };
                file = Some(raw.to_string());
            }
            "service" => {
                let Some(raw) = value.as_str() else {
                    bail!("service '{service_name}' extends.service must be a string");
                };
                service = Some(raw.to_string());
            }
            other => bail!("service '{service_name}' extends uses unsupported key '{other}'"),
        }
    }
    let service = service.with_context(|| {
        format!("service '{service_name}' extends mapping must include a service")
    })?;
    Ok(match file {
        Some(file) => ServiceExtendsRef::External { file, service },
        None => ServiceExtendsRef::SameFile { service },
    })
}

fn looks_like_extends_file(value: &str) -> bool {
    value.ends_with(".yaml")
        || value.ends_with(".yml")
        || value.contains('/')
        || value.contains('\\')
}

fn resolve_extends_path(current_path: &Path, raw: &str) -> PathBuf {
    let path = Path::new(raw);
    if path.is_absolute() {
        return path.to_path_buf();
    }
    current_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}

fn merge_root_values(base: Value, child: Value) -> Result<Value> {
    merge_values(base, child, MergeScope::Root, None)
}

fn merge_service_values(base: Value, child: Value) -> Result<Value> {
    merge_values(base, child, MergeScope::Service, None)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MergeScope {
    Root,
    Services,
    Service,
    Generic,
}

fn merge_values(base: Value, child: Value, scope: MergeScope, key: Option<&str>) -> Result<Value> {
    match (base, child) {
        (Value::Mapping(mut base), Value::Mapping(child)) => {
            merge_mappings(&mut base, child, scope)?;
            Ok(Value::Mapping(base))
        }
        (Value::Sequence(base), Value::Sequence(child))
            if scope == MergeScope::Service && key == Some("volumes") =>
        {
            Ok(Value::Sequence(merge_volume_sequences(base, child)))
        }
        (Value::Sequence(mut base), Value::Sequence(child)) => {
            base.extend(child);
            Ok(Value::Sequence(base))
        }
        (_, child) => Ok(child),
    }
}

fn merge_mappings(base: &mut Mapping, child: Mapping, scope: MergeScope) -> Result<()> {
    for (key, child_value) in child {
        let key_name = key.as_str().map(ToString::to_string);
        let child_scope = child_scope(scope, key_name.as_deref());
        match base.remove(&key) {
            Some(base_value) => {
                let merged =
                    merge_values(base_value, child_value, child_scope, key_name.as_deref())?;
                base.insert(key, merged);
            }
            None => {
                base.insert(key, child_value);
            }
        }
    }
    Ok(())
}

fn child_scope(parent: MergeScope, key: Option<&str>) -> MergeScope {
    match (parent, key) {
        (MergeScope::Root, Some("services" | "steps")) => MergeScope::Services,
        (MergeScope::Services, _) => MergeScope::Service,
        (MergeScope::Service, _) => MergeScope::Service,
        _ => MergeScope::Generic,
    }
}

fn merge_volume_sequences(base: Vec<Value>, child: Vec<Value>) -> Vec<Value> {
    let mut out = base;
    for item in child {
        if let Some(target) = volume_target(&item)
            && let Some(index) = out
                .iter()
                .position(|existing| volume_target(existing).as_deref() == Some(target.as_str()))
        {
            out[index] = item;
            continue;
        }
        out.push(item);
    }
    out
}

fn volume_target(value: &Value) -> Option<String> {
    let raw = value.as_str()?;
    match split_mount_parts(raw) {
        MountParts::HostContainer { container, .. } => Some(container.trim().to_string()),
        MountParts::UnsupportedMode(_) | MountParts::InvalidShape => None,
    }
}

fn string_key(value: &str) -> Value {
    Value::String(value.to_string())
}

fn normalize_raw_spec(value: &mut Value) -> Result<()> {
    let root = value
        .as_mapping_mut()
        .context("top-level YAML document must be a mapping")?;
    ensure!(
        !root.contains_key(string_key("extends")),
        "internal error: root extends was not resolved"
    );
    rename_steps_to_services(root);
    normalize_modules_alias("root", root)?;

    let Some(services) = root
        .get_mut(string_key("services"))
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
    let steps_key = string_key("steps");
    let services_key = string_key("services");
    if root.contains_key(&services_key) {
        return;
    }
    if let Some(steps) = root.remove(&steps_key) {
        root.insert(services_key, steps);
    }
}

fn normalize_modules_alias(scope: &str, mapping: &mut Mapping) -> Result<()> {
    let modules_key = string_key("modules");
    let Some(modules) = mapping.remove(&modules_key) else {
        return Ok(());
    };
    validate_module_list(scope, &modules)?;

    let x_env_key = string_key("x-env");
    let x_env = mapping
        .entry(x_env_key)
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    let Some(x_env_mapping) = x_env.as_mapping_mut() else {
        bail!("{scope} sets modules shorthand but x-env is not a mapping");
    };
    x_env_mapping.insert(string_key("modules"), modules);
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
