use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_norway::{Mapping, Value};

const ROOT_ALLOWED_KEYS: &[&str] = &["name", "runtime", "services", "version", "x-env", "x-slurm"];
const SERVICE_ALLOWED_KEYS: &[&str] = &[
    "image",
    "command",
    "entrypoint",
    "environment",
    "volumes",
    "working_dir",
    "depends_on",
    "readiness",
    "healthcheck",
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
        _ => bail!("healthcheck.test must start with CMD or CMD-SHELL for Compose compatibility"),
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
        bail!(
            "x-slurm.artifacts.paths entries must be absolute paths under /hpc-compose/job, got '{path}'"
        );
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
        bail!("x-slurm.artifacts.paths entries must stay under /hpc-compose/job, got '{path}'");
    }
    if normalized.get(2).map(String::as_str) == Some("artifacts") {
        bail!("x-slurm.artifacts.paths must not read from /hpc-compose/job/artifacts");
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
        bail!("x-slurm.resume.path must be an absolute host path, got '{path}'");
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
        bail!("x-slurm.resume.path must be a host path, not a container-visible /hpc-compose path");
    }
    Ok(())
}

pub(super) fn validate_root(value: &Value) -> Result<()> {
    let Some(root) = value.as_mapping() else {
        bail!("top-level YAML document must be a mapping");
    };
    validate_mapping_keys("root", root, ROOT_ALLOWED_KEYS)?;
    let Some(services) = root.get(Value::String("services".into())) else {
        bail!("spec must contain a top-level 'services' mapping");
    };
    let Some(service_map) = services.as_mapping() else {
        bail!("'services' must be a mapping");
    };
    for (name, service) in service_map {
        let Some(service_name) = name.as_str() else {
            bail!("service names must be strings");
        };
        let Some(service_mapping) = service.as_mapping() else {
            bail!("service '{service_name}' must be a mapping");
        };
        validate_mapping_keys(
            &format!("service '{service_name}'"),
            service_mapping,
            SERVICE_ALLOWED_KEYS,
        )?;
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
            other => bail!("{scope} uses unsupported key '{other}'"),
        };
        bail!("{scope}: {message}");
    }
    Ok(())
}

fn looks_like_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}
