use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_norway::{Mapping, Value};

use crate::spec_error::SpecError;
use crate::suggest;
use crate::tracked_paths::{
    JOB_CONTAINER_DIR, is_under_job_container_dir, under_job_container_dir,
};

const SUPPORTED_SPEC_VERSION: &str = "1";
const ROOT_ALLOWED_KEYS: &[&str] = &[
    "extends", "name", "modules", "runtime", "secrets", "services", "steps", "sweep", "version",
    "x-env", "x-slurm",
];
const SERVICE_ALLOWED_KEYS: &[&str] = &[
    "extends",
    "image",
    "command",
    "entrypoint",
    "script",
    "env_file",
    "environment",
    "modules",
    "volumes",
    "working_dir",
    "depends_on",
    "readiness",
    "healthcheck",
    "assert",
    "x-env",
    "x-slurm",
    "x-runtime",
    "x-enroot",
];

pub(super) fn validate_positive_u32(value: Option<u32>, field: &str) -> Result<()> {
    if matches!(value, Some(0)) {
        bail!("{field} must be at least 1");
    }
    Ok(())
}

/// Cross-checks a tensor/pipeline parallelism declaration against the GPU geometry.
///
/// `tensor` and `pipeline` must each be at least 1. When `gpus_per_node` is set,
/// `tensor * pipeline` must equal the total GPU count (`nodes * gpus_per_node`),
/// where `nodes` defaults to 1 when unset at this scope. The product is widened
/// to `u64` to guard against `u32` overflow. This is descriptive-only: it emits
/// no Slurm flags and never changes the allocation.
pub(super) fn validate_parallelism(
    parallelism: Option<&super::ParallelismConfig>,
    nodes: Option<u32>,
    gpus_per_node: Option<u32>,
    scope: &str,
) -> Result<()> {
    let Some(parallelism) = parallelism else {
        return Ok(());
    };
    if parallelism.tensor == 0 {
        return Err(SpecError::ParallelismNonPositive {
            scope: scope.to_string(),
            field: "tensor".to_string(),
        }
        .into());
    }
    if parallelism.pipeline == 0 {
        return Err(SpecError::ParallelismNonPositive {
            scope: scope.to_string(),
            field: "pipeline".to_string(),
        }
        .into());
    }
    let Some(gpus_per_node) = gpus_per_node else {
        return Ok(());
    };
    let nodes = nodes.unwrap_or(1);
    let product = u64::from(parallelism.tensor) * u64::from(parallelism.pipeline);
    let expected = u64::from(nodes) * u64::from(gpus_per_node);
    if product != expected {
        return Err(SpecError::ParallelismGpuMismatch {
            scope: scope.to_string(),
            tensor: parallelism.tensor,
            pipeline: parallelism.pipeline,
            nodes,
            gpus_per_node,
            product,
            expected,
        }
        .into());
    }
    Ok(())
}

pub(super) fn validate_sbatch_safe_string(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    if value.contains('\n') || value.contains('\r') {
        bail!("{field} must not contain line breaks");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    Ok(())
}

pub(super) fn validate_sbatch_safe_strings<'a>(
    values: impl IntoIterator<Item = &'a str>,
    field: &str,
) -> Result<()> {
    for (index, value) in values.into_iter().enumerate() {
        validate_sbatch_safe_string(Some(value), &format!("{field}[{index}]"))?;
    }
    Ok(())
}

pub(super) fn validate_service_assert_artifact_pattern(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.contains('\n') || value.contains('\r') {
        bail!("{field} must not contain line breaks");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    let normalized = if value.starts_with('/') {
        value.to_string()
    } else {
        under_job_container_dir(value)
    };
    if !is_under_job_container_dir(&normalized) {
        bail!("{field} must be relative or rooted under {JOB_CONTAINER_DIR}");
    }
    if normalized.split('/').any(|part| part == "..") {
        bail!("{field} must not contain '..' path components");
    }
    Ok(())
}

/// Validates an `x-slurm.output`/`x-slurm.error` filename pattern. Slurm fills
/// `%`-specifiers at runtime, so we replace them with a harmless placeholder
/// before checking literal path components. Rejects empty / whitespace-only
/// patterns, embedded NULs, and literal `..` traversal. Relative patterns are
/// allowed (resolved against the submit dir, like Slurm); absolute patterns are
/// allowed as-is. Intentionally minimal.
pub(super) fn validate_slurm_log_pattern(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    // Replace each Slurm specifier (including optional width, e.g. `%08j`) with
    // a placeholder path-safe token, while preserving escaped `%%` literals.
    let mut normalized = String::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            if matches!(chars.peek(), Some('%')) {
                chars.next();
                normalized.push('%');
                continue;
            }
            while matches!(chars.peek(), Some(next) if next.is_ascii_digit()) {
                chars.next();
            }
            if chars.peek().is_some() {
                chars.next();
            }
            normalized.push('x');
            continue;
        }
        normalized.push(ch);
    }
    for component in Path::new(&normalized).components() {
        if matches!(component, std::path::Component::ParentDir) {
            bail!("{field} '{value}' must not use '..' path traversal");
        }
    }
    Ok(())
}

pub(super) fn validate_slurm_array_spec(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    if value.chars().any(char::is_whitespace) {
        bail!("{field} must not contain whitespace");
    }

    let mut parts = value.split('%');
    let indexes = parts.next().unwrap_or_default();
    let concurrency = parts.next();
    if parts.next().is_some() {
        bail!("{field} must contain at most one '%' concurrency limit");
    }
    if let Some(limit) = concurrency {
        validate_positive_decimal(limit, &format!("{field} concurrency limit"))?;
    }
    if indexes.is_empty() {
        bail!("{field} must include at least one array index");
    }
    for item in indexes.split(',') {
        validate_slurm_array_item(item, field)?;
    }
    Ok(())
}

fn validate_slurm_array_item(item: &str, field: &str) -> Result<()> {
    if item.is_empty() {
        bail!("{field} must not contain empty array index items");
    }
    let mut step_parts = item.split(':');
    let range = step_parts.next().unwrap_or_default();
    let step = step_parts.next();
    if step_parts.next().is_some() {
        bail!("{field} array index item '{item}' contains more than one step separator");
    }
    if let Some(step) = step {
        validate_positive_decimal(step, &format!("{field} step"))?;
    }

    let mut range_parts = range.split('-');
    let start = range_parts.next().unwrap_or_default();
    let end = range_parts.next();
    if range_parts.next().is_some() {
        bail!("{field} array index item '{item}' contains more than one range separator");
    }
    let start = validate_non_negative_decimal(start, &format!("{field} range start"))?;
    match end {
        Some(raw_end) => {
            let end = validate_non_negative_decimal(raw_end, &format!("{field} range end"))?;
            if end < start {
                bail!("{field} range end must be greater than or equal to the start");
            }
        }
        None if step.is_some() => {
            bail!("{field} step syntax requires a range such as N-M:S");
        }
        None => {}
    }
    Ok(())
}

pub(super) fn validate_slurm_job_id(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{field}.id must not be empty");
    }
    if value.contains('\0') {
        bail!("{field}.id must not contain null bytes");
    }
    if value.chars().any(char::is_whitespace) {
        bail!("{field}.id must not contain whitespace");
    }
    let mut parts = value.split('_');
    let job_id = parts.next().unwrap_or_default();
    let task_id = parts.next();
    if parts.next().is_some() {
        bail!("{field}.id must be a Slurm job id like 12345 or array task id like 12345_7");
    }
    let job_id = validate_non_negative_decimal(job_id, &format!("{field}.id job id"))?;
    if job_id == 0 {
        bail!("{field}.id job id must be greater than 0");
    }
    if let Some(task_id) = task_id {
        validate_non_negative_decimal(task_id, &format!("{field}.id array task id"))?;
    }
    Ok(())
}

fn validate_non_negative_decimal(value: &str, field: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("{field} must be a non-negative integer");
    }
    value
        .parse::<u64>()
        .with_context(|| format!("{field} must be a valid non-negative integer"))
}

fn validate_positive_decimal(value: &str, field: &str) -> Result<u64> {
    let parsed = validate_non_negative_decimal(value, field)?;
    if parsed == 0 {
        bail!("{field} must be greater than 0");
    }
    Ok(parsed)
}

pub(super) fn validate_shell_hook_script(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field}.script must not be empty");
    }
    if value.contains('\0') {
        bail!("{field}.script must not contain null bytes");
    }
    Ok(())
}

pub(super) fn parse_healthcheck_argv(items: &[String]) -> Result<Vec<String>> {
    if items.is_empty() {
        bail!("healthcheck.test must not be empty");
    }
    match items[0].as_str() {
        "CMD" => {
            if items.len() < 2 {
                bail!("healthcheck.test CMD form must include a command");
            }
            Ok(items[1..].to_vec())
        }
        "CMD-SHELL" => {
            let Some(shell) = items.get(1) else {
                bail!("healthcheck.test CMD-SHELL form must include a shell command");
            };
            Ok(shell.split_whitespace().map(ToString::to_string).collect())
        }
        _ => Err(SpecError::HealthcheckInvalidTest.into()),
    }
}

pub(super) fn parse_nc_probe(argv: &[String]) -> Result<Option<(String, u16)>> {
    if argv.first().map(String::as_str) != Some("nc") {
        return Ok(None);
    }
    let mut non_flags = Vec::new();
    let mut has_zero_scan = false;
    let mut index = 1;
    while index < argv.len() {
        match argv[index].as_str() {
            "-z" => {
                has_zero_scan = true;
                index += 1;
            }
            flag if flag.starts_with('-') => {
                index += 1;
            }
            value => {
                non_flags.push(value.to_string());
                index += 1;
            }
        }
    }
    if !has_zero_scan {
        bail!("healthcheck nc probes must include '-z'; use explicit readiness otherwise");
    }
    if non_flags.len() != 2 {
        bail!("healthcheck nc probes must use 'nc -z HOST PORT'");
    }
    let port = non_flags[1]
        .parse::<u16>()
        .context("healthcheck nc probe port must be a valid TCP port")?;
    Ok(Some((non_flags[0].clone(), port)))
}

pub(super) fn parse_http_probe(argv: &[String]) -> Option<String> {
    match argv.first().map(String::as_str) {
        Some("curl") => argv.iter().rev().find(|item| looks_like_url(item)).cloned(),
        Some("wget") if argv.iter().any(|item| item == "--spider") => {
            argv.iter().rev().find(|item| looks_like_url(item)).cloned()
        }
        _ => None,
    }
}

pub(super) fn parse_duration_seconds(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("healthcheck duration must not be empty");
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return trimmed
            .parse::<u64>()
            .context("healthcheck duration must be a valid integer number of seconds");
    }

    let mut total = 0_u64;
    let mut number = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            bail!("unsupported healthcheck duration '{trimmed}'; use values like 30s or 2m");
        }
        let value = number
            .parse::<u64>()
            .context("healthcheck duration segment must be numeric")?;
        let factor = match ch {
            'h' => 3600,
            'm' => 60,
            's' => 1,
            _ => {
                bail!("unsupported healthcheck duration unit '{ch}' in '{trimmed}'; use h, m, or s")
            }
        };
        total = total.saturating_add(value.saturating_mul(factor));
        number.clear();
    }
    if !number.is_empty() {
        bail!("unsupported healthcheck duration '{trimmed}'; include a unit suffix");
    }
    Ok(total)
}

pub(super) fn validate_artifact_bundle_name(name: &str) -> Result<()> {
    if name == "default" {
        bail!("x-slurm.artifacts bundle name 'default' is reserved for top-level artifact paths");
    }
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        bail!("x-slurm.artifacts bundle names must match [A-Za-z0-9_-]+, got '{name}'");
    }
    Ok(())
}

pub(super) fn validate_artifact_path(path: &str) -> Result<()> {
    let candidate = Path::new(path);
    if !candidate.is_absolute() {
        return Err(SpecError::ArtifactsInvalidPath {
            path: path.to_string(),
        }
        .into());
    }

    let mut normalized = Vec::new();
    for component in candidate.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop().context(format!(
                    "x-slurm.artifacts.paths entry '{path}' escapes the root path"
                ))?;
            }
            std::path::Component::Normal(part) => {
                normalized.push(part.to_string_lossy().into_owned())
            }
            std::path::Component::Prefix(_) => {
                bail!("x-slurm.artifacts.paths entry '{path}' must use Unix-style absolute paths");
            }
        }
    }

    if normalized.first().map(String::as_str) != Some("hpc-compose")
        || normalized.get(1).map(String::as_str) != Some("job")
    {
        return Err(SpecError::ArtifactsInvalidPath {
            path: path.to_string(),
        }
        .into());
    }
    if normalized.get(2).map(String::as_str) == Some("artifacts") {
        return Err(SpecError::ArtifactsReadsExportTree.into());
    }
    Ok(())
}

pub(super) fn validate_resume_path(path: &str) -> Result<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("x-slurm.resume.path must not be empty");
    }

    let candidate = Path::new(trimmed);
    if !candidate.is_absolute() {
        return Err(SpecError::ResumeRelativePath {
            path: path.to_string(),
        }
        .into());
    }

    let mut normalized = Vec::new();
    for component in candidate.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop().context(format!(
                    "x-slurm.resume.path entry '{path}' escapes the root path"
                ))?;
            }
            std::path::Component::Normal(part) => {
                normalized.push(part.to_string_lossy().into_owned())
            }
            std::path::Component::Prefix(_) => {
                bail!("x-slurm.resume.path '{path}' must use Unix-style absolute paths");
            }
        }
    }

    if normalized.first().map(String::as_str) == Some("hpc-compose") {
        return Err(SpecError::ResumeContainerPath {
            path: path.to_string(),
        }
        .into());
    }
    Ok(())
}

pub(super) fn validate_root(value: &Value) -> Result<()> {
    let Some(root) = value.as_mapping() else {
        return Err(SpecError::InvalidFieldType {
            field: "root".into(),
            got: "non-mapping".into(),
        }
        .into());
    };
    validate_mapping_keys("root", root, ROOT_ALLOWED_KEYS)?;
    validate_spec_version(root)?;
    validate_modules_alias_conflict("root", root)?;
    validate_sweep_authoring_shape(root)?;
    let services_key = Value::String("services".into());
    let steps_key = Value::String("steps".into());
    let services = root.get(&services_key);
    let steps = root.get(&steps_key);
    if services.is_some() && steps.is_some() {
        bail!("spec must not define both top-level 'services' and 'steps'; use only one");
    }
    let Some(services) = services.or(steps) else {
        return Err(SpecError::MissingServices.into());
    };
    let Some(service_map) = services.as_mapping() else {
        return Err(SpecError::InvalidFieldType {
            field: "services".into(),
            got: "non-mapping".into(),
        }
        .into());
    };
    for (name, service) in service_map {
        let Some(service_name) = name.as_str() else {
            bail!("service names must be strings");
        };
        let Some(service_mapping) = service.as_mapping() else {
            return Err(SpecError::InvalidFieldType {
                field: service_name.to_string(),
                got: "non-mapping".into(),
            }
            .into());
        };
        validate_mapping_keys(
            &format!("service '{service_name}'"),
            service_mapping,
            SERVICE_ALLOWED_KEYS,
        )?;
        validate_modules_alias_conflict(&format!("service '{service_name}'"), service_mapping)?;
        validate_script_conflicts(service_name, service_mapping)?;
    }
    Ok(())
}

fn validate_sweep_authoring_shape(root: &Mapping) -> Result<()> {
    let Some(sweep) = root.get(Value::String("sweep".into())) else {
        return Ok(());
    };
    let Some(sweep) = sweep.as_mapping() else {
        bail!("top-level 'sweep' must be a mapping");
    };
    if sweep.contains_key(Value::String("spec".into())) {
        bail!("sweep.spec is not supported in v1; embed sweep in the compose file");
    }
    Ok(())
}

fn validate_spec_version(root: &Mapping) -> Result<()> {
    let version_key = Value::String("version".into());
    let Some(value) = root.get(&version_key) else {
        return Ok(());
    };
    let version = spec_version_label(value)?;
    if version == SUPPORTED_SPEC_VERSION {
        return Ok(());
    }
    Err(SpecError::UnsupportedSpecVersion {
        version: version.clone(),
        supported: SUPPORTED_SPEC_VERSION,
        help_text: migration_hint_for_version(&version).to_string(),
    }
    .into())
}

fn spec_version_label(value: &Value) -> Result<String> {
    match value {
        Value::String(version) => Ok(version.clone()),
        Value::Number(version) => {
            if version.as_u64() == Some(1) {
                Ok(SUPPORTED_SPEC_VERSION.to_string())
            } else {
                Ok(version.to_string())
            }
        }
        other => Err(SpecError::InvalidSpecVersion {
            got: value_kind(other).to_string(),
        }
        .into()),
    }
}

fn migration_hint_for_version(version: &str) -> &'static str {
    if version == "2" {
        return "steps was renamed to services in v2 - see docs/src/docker-compose-migration.md";
    }
    if looks_like_docker_compose_version(version) {
        return "Top-level `version` is the hpc-compose schema version, not a Docker Compose version. Use `version: \"1\"` or omit `version` after migrating; see docs/docker-compose-migration.md.";
    }
    "Use `version: \"1\"` or omit `version` for v1 specs. Upgrade hpc-compose if this file targets a newer schema."
}

fn looks_like_docker_compose_version(version: &str) -> bool {
    matches!(
        version,
        "3" | "3.0" | "3.1" | "3.2" | "3.3" | "3.4" | "3.5" | "3.6" | "3.7" | "3.8" | "3.9"
    ) || version.starts_with("2.")
        || version.starts_with("3.")
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Sequence(_) => "sequence",
        Value::Mapping(_) => "mapping",
        Value::Tagged(_) => "tagged value",
    }
}

fn validate_modules_alias_conflict(scope: &str, mapping: &Mapping) -> Result<()> {
    let modules_key = Value::String("modules".into());
    let x_env_key = Value::String("x-env".into());
    let Some(x_env) = mapping.get(&x_env_key) else {
        return Ok(());
    };
    let Some(x_env_mapping) = x_env.as_mapping() else {
        return Ok(());
    };
    if mapping.contains_key(&modules_key)
        && x_env_mapping.contains_key(Value::String("modules".into()))
    {
        bail!("{scope} sets both 'modules' and 'x-env.modules'; use only one spelling");
    }
    Ok(())
}

fn validate_script_conflicts(service_name: &str, mapping: &Mapping) -> Result<()> {
    if !mapping.contains_key(Value::String("script".into())) {
        return Ok(());
    }
    if mapping.contains_key(Value::String("command".into())) {
        return Err(SpecError::ScriptCommandConflict {
            service: service_name.to_string(),
            conflict: "command".into(),
        }
        .into());
    }
    if mapping.contains_key(Value::String("entrypoint".into())) {
        return Err(SpecError::ScriptCommandConflict {
            service: service_name.to_string(),
            conflict: "entrypoint".into(),
        }
        .into());
    }
    Ok(())
}

fn validate_mapping_keys(scope: &str, mapping: &Mapping, allowed: &[&str]) -> Result<()> {
    for key in mapping.keys() {
        let Some(key_name) = key.as_str() else {
            bail!("{scope} contains a non-string key");
        };
        if allowed.contains(&key_name) {
            continue;
        }
        let message = match key_name {
            "build" => {
                "build is not supported in v1; use image: plus x-runtime.prepare to customize an image before submission"
            }
            "ports" => {
                "ports are not supported; use host-network semantics and explicit readiness checks"
            }
            "networks" | "network_mode" => {
                "custom container networking is not supported under this Slurm/Enroot execution model"
            }
            "restart" => {
                "Compose restart policies are not supported; use services.<name>.x-slurm.failure_policy instead"
            }
            "deploy" => {
                "deploy is not supported; this tool targets one Slurm allocation, not a long-running orchestrator"
            }
            other => {
                let help_text = match suggest::nearest_default(other, allowed) {
                    Some(s) => {
                        format!("See the spec reference for supported keys. Did you mean \"{s}\"?")
                    }
                    None => "See the spec reference for supported keys.".to_string(),
                };
                return Err(SpecError::UnsupportedServiceKey {
                    scope: scope.to_string(),
                    key: other.to_string(),
                    help_text,
                }
                .into());
            }
        };
        return Err(SpecError::UnsupportedServiceKey {
            scope: scope.to_string(),
            key: key_name.to_string(),
            help_text: message.to_string(),
        }
        .into());
    }
    Ok(())
}

fn looks_like_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}
