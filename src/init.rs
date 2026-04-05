use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_yaml::{Mapping, Value};

pub struct Template {
    pub name: &'static str,
    pub description: &'static str,
    pub body: &'static str,
}

const DEFAULT_CACHE_DIR: &str = "/shared/$USER/hpc-compose-cache";

const TEMPLATES: &[Template] = &[
    Template {
        name: "dev-python-app",
        description: "Mounted source tree plus a small prepare step for iterative development.",
        body: include_str!("../examples/dev-python-app.yaml"),
    },
    Template {
        name: "app-redis-worker",
        description: "Multiple services with startup ordering and TCP readiness.",
        body: include_str!("../examples/app-redis-worker.yaml"),
    },
    Template {
        name: "llm-curl-workflow",
        description: "GPU-backed LLM service with a dependent curl client.",
        body: include_str!("../examples/llm-curl-workflow.yaml"),
    },
    Template {
        name: "llm-curl-workflow-workdir",
        description: "Home-directory LLM workflow for direct login-node use.",
        body: include_str!("../examples/llm-curl-workflow-workdir.yaml"),
    },
    Template {
        name: "llama-app",
        description: "GPU service with a dependent application workflow.",
        body: include_str!("../examples/llama-app.yaml"),
    },
    Template {
        name: "minimal-batch",
        description: "Simplest single-service batch job.",
        body: include_str!("../examples/minimal-batch.yaml"),
    },
    Template {
        name: "training-checkpoints",
        description: "GPU training with checkpoints written to shared storage.",
        body: include_str!("../examples/training-checkpoints.yaml"),
    },
    Template {
        name: "postgres-etl",
        description: "PostgreSQL plus a Python data processing job.",
        body: include_str!("../examples/postgres-etl.yaml"),
    },
    Template {
        name: "vllm-openai",
        description: "vLLM serving with an in-job Python client.",
        body: include_str!("../examples/vllm-openai.yaml"),
    },
    Template {
        name: "mpi-hello",
        description: "MPI hello world with Open MPI.",
        body: include_str!("../examples/mpi-hello.yaml"),
    },
    Template {
        name: "multi-stage-pipeline",
        description: "Two-stage data pipeline coordinating through shared job mount.",
        body: include_str!("../examples/multi-stage-pipeline.yaml"),
    },
    Template {
        name: "fairseq-preprocess",
        description: "CPU-heavy NLP data preprocessing pipeline.",
        body: include_str!("../examples/fairseq-preprocess.yaml"),
    },
];

#[derive(Debug, Clone)]
pub struct InitAnswers {
    pub template_name: String,
    pub app_name: String,
    pub cache_dir: String,
}

pub fn templates() -> &'static [Template] {
    TEMPLATES
}

pub fn default_cache_dir() -> &'static str {
    DEFAULT_CACHE_DIR
}

pub fn resolve_template(name: &str) -> Result<&'static Template> {
    let normalized = name.trim().trim_end_matches(".yaml");
    TEMPLATES
        .iter()
        .find(|template| template.name == normalized)
        .with_context(|| {
            format!(
                "unknown template '{}'; available templates: {}",
                name,
                TEMPLATES
                    .iter()
                    .map(|template| template.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

pub fn prompt_for_init() -> Result<InitAnswers> {
    let mut stdout = io::stdout();
    writeln!(stdout, "Choose a template:").ok();
    for (index, template) in TEMPLATES.iter().enumerate() {
        writeln!(
            stdout,
            "  {}. {} - {}",
            index + 1,
            template.name,
            template.description
        )
        .ok();
    }
    stdout.flush().ok();

    let selection = prompt("Template number", "1")?;
    let template_index = selection
        .parse::<usize>()
        .ok()
        .and_then(|value| value.checked_sub(1))
        .filter(|index| *index < TEMPLATES.len())
        .context("template selection must be one of the listed numbers")?;
    let template = &TEMPLATES[template_index];

    let app_name = prompt("Application name", template.name)?;
    let cache_dir = prompt("Cache dir", DEFAULT_CACHE_DIR)?;

    Ok(InitAnswers {
        template_name: template.name.to_string(),
        app_name,
        cache_dir,
    })
}

pub fn render_template(template_name: &str, app_name: &str, cache_dir: &str) -> Result<String> {
    let template = resolve_template(template_name)?;
    let mut value: Value = serde_yaml::from_str(template.body)
        .with_context(|| format!("failed to parse template {}", template.name))?;
    let root = value
        .as_mapping_mut()
        .context("template root must be a mapping")?;

    root.insert(
        Value::String("name".to_string()),
        Value::String(app_name.to_string()),
    );

    let slurm_key = Value::String("x-slurm".to_string());
    let slurm_value = root
        .entry(slurm_key)
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    let slurm = slurm_value
        .as_mapping_mut()
        .context("template x-slurm must be a mapping")?;
    slurm.insert(
        Value::String("job_name".to_string()),
        Value::String(app_name.to_string()),
    );
    slurm.insert(
        Value::String("cache_dir".to_string()),
        Value::String(cache_dir.to_string()),
    );

    serde_yaml::to_string(&value).context("failed to serialize initialized template")
}

pub fn write_initialized_template(output: &Path, rendered: &str, force: bool) -> Result<PathBuf> {
    let output = absolute_path(output)?;
    if output.exists() && !force {
        bail!(
            "refusing to overwrite {}; pass --force to replace it",
            output.display()
        );
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&output, rendered)
        .with_context(|| format!("failed to write {}", output.display()))?;
    Ok(output)
}

pub fn next_commands(output: &Path) -> Vec<String> {
    let path = output.display().to_string();
    vec![
        format!("hpc-compose validate -f {path}"),
        format!("hpc-compose inspect -f {path}"),
        format!("hpc-compose submit --watch -f {path}"),
    ]
}

fn prompt(label: &str, default: &str) -> Result<String> {
    let mut stdout = io::stdout();
    write!(stdout, "{label} [{default}]: ").ok();
    stdout.flush().ok();

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read interactive input")?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to determine current directory")?
        .join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ComposeSpec;

    #[test]
    fn templates_are_resolvable() {
        for template in templates() {
            let resolved = resolve_template(template.name).expect("resolve");
            assert_eq!(resolved.name, template.name);
            let resolved =
                resolve_template(&format!("{}.yaml", template.name)).expect("resolve yaml");
            assert_eq!(resolved.name, template.name);
        }
    }

    #[test]
    fn template_bodies_parse_as_valid_specs() {
        for template in templates() {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = tmpdir.path().join(format!("{}.yaml", template.name));
            fs::write(&path, template.body).expect("write template");
            ComposeSpec::load(&path).unwrap_or_else(|err| {
                panic!(
                    "template '{}' failed to parse as a valid ComposeSpec: {}",
                    template.name, err
                )
            });
        }
    }

    #[test]
    fn render_template_rewrites_name_job_and_cache_dir() {
        let rendered =
            render_template("dev-python-app", "custom-app", "/cache/path").expect("render");
        let value: Value = serde_yaml::from_str(&rendered).expect("yaml");
        let root = value.as_mapping().expect("root");
        assert_eq!(
            root.get(Value::String("name".to_string()))
                .and_then(Value::as_str),
            Some("custom-app")
        );
        let slurm = root
            .get(Value::String("x-slurm".to_string()))
            .and_then(Value::as_mapping)
            .expect("x-slurm");
        assert_eq!(
            slurm
                .get(Value::String("job_name".to_string()))
                .and_then(Value::as_str),
            Some("custom-app")
        );
        assert_eq!(
            slurm
                .get(Value::String("cache_dir".to_string()))
                .and_then(Value::as_str),
            Some("/cache/path")
        );
    }
}
