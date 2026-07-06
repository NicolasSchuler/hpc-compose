use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hpc_compose::job::{SubmissionKind, SubmissionRecord};
use serde_norway::{Mapping, Value};

const NOTEBOOK_CONTAINER_DIR: &str = "/hpc-compose/notebook-promote";
const REQUIREMENTS_CONTAINER_PATH: &str = "/hpc-compose/prepare/requirements.txt";

#[derive(Debug, Clone)]
pub(crate) struct PromoteArgs {
    pub(crate) notebook: PathBuf,
    pub(crate) record: Option<PathBuf>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) force: bool,
    pub(crate) image: Option<String>,
    pub(crate) volumes: Vec<String>,
    pub(crate) working_dir: Option<String>,
    pub(crate) requirements: Option<PathBuf>,
    pub(crate) prepare_commands: Vec<String>,
    pub(crate) params: Vec<String>,
}

pub(crate) fn promote(args: PromoteArgs, quiet: bool) -> Result<()> {
    let notebook = crate::path_util::absolute_path_cwd(&args.notebook)
        .with_context(|| format!("failed to resolve {}", args.notebook.display()))?;
    if !notebook.is_file() {
        bail!(
            "notebook {} does not exist or is not a file",
            notebook.display()
        );
    }
    let output = args
        .output
        .clone()
        .unwrap_or_else(|| default_output_path(&args.notebook));
    let record_path = args
        .record
        .clone()
        .unwrap_or_else(default_latest_record_path);
    let record_path = crate::path_util::absolute_path_cwd(&record_path)
        .with_context(|| format!("failed to resolve {}", record_path.display()))?;
    let record = read_notebook_record(&record_path)?;
    let params = parse_params(&args.params)?;
    let notebook_scan = scan_notebook_installs(&notebook)?;

    let build = build_promoted_yaml(&record, &args, &notebook, &params)?;
    let written = hpc_compose::init::write_initialized_template(&output, &build.yaml, args.force)?;

    if !quiet {
        for warning in notebook_scan.iter().chain(build.warnings.iter()) {
            eprintln!("warning: {warning}");
        }
        println!("promoted notebook batch spec: {}", written.display());
        println!("record: {}", record_path.display());
        println!("next: hpc-compose plan -f {}", written.display());
    }
    Ok(())
}

struct PromoteBuild {
    yaml: String,
    warnings: Vec<String>,
}

fn build_promoted_yaml(
    record: &SubmissionRecord,
    args: &PromoteArgs,
    notebook: &Path,
    params: &[(String, String)],
) -> Result<PromoteBuild> {
    let mut warnings = Vec::new();
    let mut root = if let Some(snapshot) = record.config_snapshot_yaml.as_deref() {
        serde_norway::from_str::<Value>(snapshot)
            .context("failed to parse config_snapshot_yaml from notebook record")?
    } else {
        warnings.push(
            "notebook record has no config_snapshot_yaml; generated spec uses persisted record fields and explicit promote overrides only".to_string(),
        );
        minimal_root(record, args, notebook)?
    };
    let root_map = root
        .as_mapping_mut()
        .context("notebook record config_snapshot_yaml must be a YAML mapping")?;

    let service_name = select_or_create_service(root_map, record)?;
    let service = service_mapping_mut(root_map, &service_name)?;

    if let Some(image) = nonempty_opt(args.image.as_deref()) {
        service.insert(key("image"), Value::String(image.to_string()));
    } else if !service.contains_key(&key("image")) {
        if let Some(image) = inferred_image(record, &service_name) {
            service.insert(key("image"), Value::String(image));
        } else {
            bail!(
                "record {} lacks config_snapshot_yaml with a service image and provenance does not include image_refs.{service_name}; pass --image",
                record.job_id
            );
        }
    }

    let command = papermill_command(notebook, params)?;
    service.insert(key("command"), string_sequence(command));
    service.remove(&key("readiness"));
    service.remove(&key("healthcheck"));

    let notebook_mount = notebook_mount(notebook)?;
    append_string_sequence(service, "volumes", [notebook_mount]);
    append_string_sequence(service, "volumes", args.volumes.iter().cloned());
    if let Some(working_dir) = nonempty_opt(args.working_dir.as_deref()) {
        service.insert(key("working_dir"), Value::String(working_dir.to_string()));
    } else if !service.contains_key(&key("working_dir")) {
        service.insert(
            key("working_dir"),
            Value::String(NOTEBOOK_CONTAINER_DIR.to_string()),
        );
    }

    let existing_prepare_commands = existing_prepare_commands(service);
    let (prepare_commands, prepare_mounts) =
        promoted_prepare(args, &existing_prepare_commands, &mut warnings)?;
    append_prepare(service, prepare_commands, prepare_mounts)?;

    let yaml =
        serde_norway::to_string(&root).context("failed to serialize promoted compose YAML")?;
    Ok(PromoteBuild { yaml, warnings })
}

fn read_notebook_record(path: &Path) -> Result<SubmissionRecord> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read notebook record {}", path.display()))?;
    let record: SubmissionRecord = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse notebook record {}", path.display()))?;
    if record.kind != SubmissionKind::Notebook {
        bail!(
            "record {} has kind {:?}, expected notebook",
            path.display(),
            record.kind
        );
    }
    Ok(record)
}

fn minimal_root(record: &SubmissionRecord, args: &PromoteArgs, notebook: &Path) -> Result<Value> {
    let name = promoted_name(notebook);
    let service_name = record
        .service_name
        .clone()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "notebook".to_string());
    let image = nonempty_opt(args.image.as_deref())
        .map(str::to_string)
        .or_else(|| inferred_image(record, &service_name))
        .with_context(|| {
            format!(
                "notebook record {} has no config_snapshot_yaml and no recoverable image; pass --image",
                record.job_id
            )
        })?;

    let mut slurm = Mapping::new();
    slurm.insert(key("job_name"), Value::String(name.clone()));
    if let Some(walltime) = &record.requested_walltime {
        slurm.insert(key("time"), Value::String(walltime.original.clone()));
    }

    let mut service = Mapping::new();
    service.insert(key("image"), Value::String(image));
    service.insert(
        key("working_dir"),
        Value::String(NOTEBOOK_CONTAINER_DIR.to_string()),
    );

    let mut services = Mapping::new();
    services.insert(Value::String(service_name), Value::Mapping(service));

    let mut root = Mapping::new();
    root.insert(key("name"), Value::String(name));
    root.insert(key("x-slurm"), Value::Mapping(slurm));
    root.insert(key("services"), Value::Mapping(services));
    Ok(Value::Mapping(root))
}

fn select_or_create_service(root: &mut Mapping, record: &SubmissionRecord) -> Result<String> {
    let services = root
        .entry(key("services"))
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()
        .context("promoted compose services must be a mapping")?;
    let recorded = record
        .service_name
        .as_deref()
        .filter(|name| !name.trim().is_empty());
    if let Some(name) = recorded {
        if services.contains_key(&key(name)) {
            return Ok(name.to_string());
        }
    }
    if services.contains_key(&key("notebook")) {
        return Ok("notebook".to_string());
    }
    if let Some(name) = services.keys().find_map(Value::as_str) {
        return Ok(name.to_string());
    }
    let name = recorded.unwrap_or("notebook").to_string();
    services.insert(Value::String(name.clone()), Value::Mapping(Mapping::new()));
    Ok(name)
}

fn service_mapping_mut<'a>(root: &'a mut Mapping, service_name: &str) -> Result<&'a mut Mapping> {
    let services = root
        .get_mut(&key("services"))
        .and_then(Value::as_mapping_mut)
        .context("promoted compose services must be a mapping")?;
    let service = services
        .entry(Value::String(service_name.to_string()))
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    if !service.is_mapping() {
        *service = Value::Mapping(Mapping::new());
    }
    service
        .as_mapping_mut()
        .context("promoted compose service must be a mapping")
}

fn papermill_command(notebook: &Path, params: &[(String, String)]) -> Result<Vec<String>> {
    let input_name = notebook
        .file_name()
        .and_then(|name| name.to_str())
        .context("notebook path must have a UTF-8 file name")?;
    let stem = notebook
        .file_stem()
        .and_then(|name| name.to_str())
        .context("notebook path must have a UTF-8 stem")?;
    let mut command = vec![
        "python".to_string(),
        "-m".to_string(),
        "papermill".to_string(),
        format!("{NOTEBOOK_CONTAINER_DIR}/{input_name}"),
        format!("{NOTEBOOK_CONTAINER_DIR}/{stem}.promoted.ipynb"),
    ];
    for (name, default) in params {
        command.push("-p".to_string());
        command.push(name.clone());
        command.push(format!("${{{name}:-{default}}}"));
    }
    Ok(command)
}

fn parse_params(params: &[String]) -> Result<Vec<(String, String)>> {
    let mut parsed = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in params {
        let Some((name, default)) = raw.split_once('=') else {
            bail!("--param entries must use NAME=DEFAULT syntax, got '{raw}'");
        };
        validate_param_name(name)?;
        if !seen.insert(name.to_string()) {
            bail!("duplicate --param name '{name}'");
        }
        parsed.push((name.to_string(), default.to_string()));
    }
    Ok(parsed)
}

fn validate_param_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("--param name must not be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        bail!("--param name '{name}' must match [A-Za-z_][A-Za-z0-9_]*");
    }
    Ok(())
}

fn promoted_prepare(
    args: &PromoteArgs,
    existing_commands: &[String],
    warnings: &mut Vec<String>,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut commands = Vec::new();
    let mut mounts = Vec::new();
    let mut papermill_handled = existing_commands
        .iter()
        .chain(args.prepare_commands.iter())
        .any(|command| command.to_ascii_lowercase().contains("papermill"));

    if let Some(requirements) = &args.requirements {
        let requirements = crate::path_util::absolute_path_cwd(requirements)
            .with_context(|| format!("failed to resolve {}", requirements.display()))?;
        if !requirements.is_file() {
            bail!(
                "requirements file {} does not exist or is not a file",
                requirements.display()
            );
        }
        let raw = fs::read_to_string(&requirements)
            .with_context(|| format!("failed to read {}", requirements.display()))?;
        papermill_handled |= requirements_mentions_papermill(&raw);
        mounts.push(format!(
            "{}:{REQUIREMENTS_CONTAINER_PATH}:ro",
            requirements.display()
        ));
        commands.push(format!(
            "pip install --no-cache-dir -r {REQUIREMENTS_CONTAINER_PATH}"
        ));
    }
    commands.extend(args.prepare_commands.iter().cloned());
    if !papermill_handled {
        commands.push("pip install --no-cache-dir papermill".to_string());
    } else if args.requirements.is_none()
        && args
            .prepare_commands
            .iter()
            .any(|command| command.to_ascii_lowercase().contains("papermill"))
    {
        warnings.push("papermill install is assumed from --prepare-command".to_string());
    }
    Ok((commands, mounts))
}

fn append_prepare(service: &mut Mapping, commands: Vec<String>, mounts: Vec<String>) -> Result<()> {
    let runtime = service
        .entry(key("x-runtime"))
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()
        .context("service x-runtime must be a mapping")?;
    let prepare = runtime
        .entry(key("prepare"))
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()
        .context("service x-runtime.prepare must be a mapping")?;
    append_string_sequence(prepare, "commands", commands);
    append_string_sequence(prepare, "mounts", mounts);
    Ok(())
}

fn existing_prepare_commands(service: &Mapping) -> Vec<String> {
    service
        .get(&key("x-runtime"))
        .and_then(Value::as_mapping)
        .and_then(|runtime| runtime.get(&key("prepare")))
        .and_then(Value::as_mapping)
        .and_then(|prepare| prepare.get(&key("commands")))
        .and_then(Value::as_sequence)
        .map(|commands| {
            commands
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn append_string_sequence<I>(mapping: &mut Mapping, field: &str, values: I)
where
    I: IntoIterator<Item = String>,
{
    let values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        return;
    }
    let entry = mapping
        .entry(key(field))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    if !entry.is_sequence() {
        *entry = Value::Sequence(Vec::new());
    }
    if let Some(sequence) = entry.as_sequence_mut() {
        sequence.extend(values.into_iter().map(Value::String));
    }
}

fn string_sequence(values: Vec<String>) -> Value {
    Value::Sequence(values.into_iter().map(Value::String).collect())
}

fn notebook_mount(notebook: &Path) -> Result<String> {
    let parent = notebook
        .parent()
        .context("notebook path must have a parent directory")?;
    Ok(format!("{}:{NOTEBOOK_CONTAINER_DIR}", parent.display()))
}

fn inferred_image(record: &SubmissionRecord, service_name: &str) -> Option<String> {
    let provenance = record.provenance.as_ref()?;
    provenance
        .image_refs
        .get(service_name)
        .cloned()
        .or_else(|| {
            (provenance.image_refs.len() == 1)
                .then(|| provenance.image_refs.values().next().cloned())
                .flatten()
        })
}

fn scan_notebook_installs(notebook: &Path) -> Result<Vec<String>> {
    let raw = fs::read_to_string(notebook)
        .with_context(|| format!("failed to read notebook {}", notebook.display()))?;
    let lower = raw.to_ascii_lowercase();
    let patterns = [
        "%pip install",
        "!pip install",
        "conda install",
        "mamba install",
    ];
    let matches = patterns
        .iter()
        .filter(|pattern| lower.contains(**pattern))
        .copied()
        .collect::<Vec<_>>();
    if matches.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(vec![format!(
            "notebook contains ad-hoc install cell(s): {}; promote dependencies with --requirements or --prepare-command",
            matches.join(", ")
        )])
    }
}

fn requirements_mentions_papermill(raw: &str) -> bool {
    raw.lines().any(|line| {
        let trimmed = line.trim();
        !trimmed.starts_with('#')
            && trimmed
                .to_ascii_lowercase()
                .split(['=', '<', '>', '~', '!', '[', ';', ' '])
                .next()
                == Some("papermill")
    })
}

fn default_latest_record_path() -> PathBuf {
    env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(crate::tracked_paths::METADATA_DIR_NAME)
        .join(crate::tracked_paths::NOTEBOOK_LATEST_RECORD_FILE_NAME)
}

fn default_output_path(notebook: &Path) -> PathBuf {
    let stem = notebook
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("notebook");
    PathBuf::from(format!("{stem}.promoted.yaml"))
}

fn promoted_name(notebook: &Path) -> String {
    let stem = notebook
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("notebook");
    format!("{stem}-promoted")
}

fn nonempty_opt(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn key(name: &str) -> Value {
    Value::String(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requirements_mentions_papermill_accepts_common_pins() {
        assert!(requirements_mentions_papermill("papermill==2.5\n"));
        assert!(requirements_mentions_papermill("papermill[all]>=2\n"));
        assert!(!requirements_mentions_papermill("# papermill\n"));
        assert!(!requirements_mentions_papermill("some-papermill-helper\n"));
    }
}
