//! Interactive and non-interactive helpers for `hpc-compose new`.

use crate::term;

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_norway::{Mapping, Value};

/// A shipped compose template exposed by `hpc-compose new`.
#[derive(Debug)]
pub struct Template {
    /// Stable template identifier used on the CLI.
    pub name: &'static str,
    /// Short contributor-facing description shown in discovery output.
    pub description: &'static str,
    /// Raw YAML template body bundled into the binary.
    pub body: &'static str,
}

const CACHE_DIR_PLACEHOLDER: &str = "<shared-cache-dir>";

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
        name: "llama-uv-worker",
        description: "llama.cpp serving plus a source-mounted Python worker run through uv.",
        body: include_str!("../examples/llama-uv-worker.yaml"),
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
        name: "training-resume",
        description: "GPU training with a shared resume directory and attempt-aware checkpoints.",
        body: include_str!("../examples/training-resume.yaml"),
    },
    Template {
        name: "postgres-etl",
        description: "PostgreSQL plus a Python data processing job.",
        body: include_str!("../examples/postgres-etl.yaml"),
    },
    Template {
        name: "restart-policy",
        description: "Per-service restart_on_failure with bounded retries and a rolling-window crash-loop guard.",
        body: include_str!("../examples/restart-policy.yaml"),
    },
    Template {
        name: "vllm-openai",
        description: "vLLM serving with an in-job Python client.",
        body: include_str!("../examples/vllm-openai.yaml"),
    },
    Template {
        name: "vllm-uv-worker",
        description: "vLLM serving plus a source-mounted Python worker run through uv.",
        body: include_str!("../examples/vllm-uv-worker.yaml"),
    },
    Template {
        name: "mpi-hello",
        description: "MPI hello world with Open MPI.",
        body: include_str!("../examples/mpi-hello.yaml"),
    },
    Template {
        name: "mpi-pmix-v4-host-mpi",
        description: "Versioned PMIx launch plus host MPI bind/env configuration.",
        body: include_str!("../examples/mpi-pmix-v4-host-mpi.yaml"),
    },
    Template {
        name: "multi-node-mpi",
        description: "Primary-node helper plus one allocation-wide distributed MPI step.",
        body: include_str!("../examples/multi-node-mpi.yaml"),
    },
    Template {
        name: "multi-node-torchrun",
        description: "Allocation-wide GPU training with the primary node as rendezvous.",
        body: include_str!("../examples/multi-node-torchrun.yaml"),
    },
    Template {
        name: "multi-stage-pipeline",
        description: "Two-stage data pipeline coordinating through shared job mount.",
        body: include_str!("../examples/multi-stage-pipeline.yaml"),
    },
    Template {
        name: "pipeline-dag",
        description: "One-shot preprocess/train/postprocess DAG with completion dependencies.",
        body: include_str!("../examples/pipeline-dag.yaml"),
    },
    Template {
        name: "fairseq-preprocess",
        description: "CPU-heavy NLP data preprocessing pipeline.",
        body: include_str!("../examples/fairseq-preprocess.yaml"),
    },
];

#[derive(Debug, Clone)]
/// Answers gathered by the interactive template flow.
pub struct InitAnswers {
    /// Selected template identifier.
    pub template_name: String,
    /// Application name inserted into the rendered template.
    pub app_name: String,
    /// Cache directory inserted into `x-slurm.cache_dir`.
    pub cache_dir: String,
}

/// Returns the built-in compose templates bundled with the binary.
#[must_use]
pub fn templates() -> &'static [Template] {
    TEMPLATES
}

/// Returns the placeholder shown for the required scaffold cache directory.
#[must_use]
pub fn cache_dir_placeholder() -> &'static str {
    CACHE_DIR_PLACEHOLDER
}

/// Prompts on stdin/stdout for template, app name, and cache directory, using
/// the supplied cache directory as the interactive default when present.
///
/// # Errors
///
/// Returns an error when stdin/stdout interaction fails or the selected
/// template number is out of range.
pub fn prompt_for_init_with_cache_dir_default(
    default_cache_dir: Option<&str>,
) -> Result<InitAnswers> {
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout();
    prompt_for_init_with_io(&mut stdin, &mut stdout, default_cache_dir)
}

/// Resolves a template by name, accepting names with or without `.yaml`.
///
/// # Errors
///
/// Returns an error when the requested template does not exist.
pub fn resolve_template(name: &str) -> Result<&'static Template> {
    let normalized = name.trim().trim_end_matches(".yaml");
    TEMPLATES
        .iter()
        .find(|template| template.name == normalized)
        .context(format!(
            "unknown template '{}'; available templates: {}",
            name,
            TEMPLATES
                .iter()
                .map(|template| template.name)
                .collect::<Vec<_>>()
                .join(", ")
        ))
}

/// Prompts on stdin/stdout for template, app name, and cache directory.
///
/// # Errors
///
/// Returns an error when stdin/stdout interaction fails or the selected
/// template number is out of range.
pub fn prompt_for_init() -> Result<InitAnswers> {
    prompt_for_init_with_cache_dir_default(None)
}

fn prompt_for_init_with_io(
    input: &mut impl BufRead,
    output: &mut impl Write,
    default_cache_dir: Option<&str>,
) -> Result<InitAnswers> {
    writeln!(output, "{}", term::styled_bold("Choose a template:")).ok();
    for (index, template) in TEMPLATES.iter().enumerate() {
        writeln!(
            output,
            "  {}. {} - {}",
            index + 1,
            term::styled_bold(template.name),
            term::styled_dim(template.description)
        )
        .ok();
    }
    output.flush().ok();

    let selection = prompt(input, output, "Template number", "1")?;
    let template_index = selection
        .parse::<usize>()
        .ok()
        .and_then(|value| value.checked_sub(1))
        .filter(|index| *index < TEMPLATES.len())
        .context("template selection must be one of the listed numbers")?;
    let template = &TEMPLATES[template_index];

    let app_name = prompt(input, output, "Application name", template.name)?;
    let cache_dir = match default_cache_dir.filter(|value| !value.trim().is_empty()) {
        Some(default) => prompt(input, output, "Cache dir", default)?,
        None => prompt_required(
            input,
            output,
            "Cache dir",
            "choose a path visible from both the login node and the compute nodes",
        )?,
    };

    Ok(InitAnswers {
        template_name: template.name.to_string(),
        app_name,
        cache_dir,
    })
}

/// Renders a shipped template with the selected application name and cache directory.
///
/// ```rust
/// let rendered = hpc_compose::init::render_template(
///     "minimal-batch",
///     "demo-app",
///     "/cluster/shared/hpc-compose-cache",
/// )?;
/// assert!(rendered.contains("name: demo-app"));
/// assert!(rendered.contains("cache_dir: /cluster/shared/hpc-compose-cache"));
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Errors
///
/// Returns an error when the template name is unknown or the bundled template
/// cannot be parsed and rewritten as YAML.
pub fn render_template(template_name: &str, app_name: &str, cache_dir: &str) -> Result<String> {
    let template = resolve_template(template_name)?;
    render_template_body(template.body, template.name, app_name, cache_dir)
}

fn render_template_body(
    body: &str,
    template_name: &str,
    app_name: &str,
    cache_dir: &str,
) -> Result<String> {
    let mut value: Value = serde_norway::from_str(body)
        .context(format!("failed to parse template {template_name}"))?;
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

    serde_norway::to_string(&value).context("failed to serialize initialized template")
}

/// Writes a rendered template to disk and returns the absolute output path.
///
/// # Errors
///
/// Returns an error when the destination already exists without `force`, when
/// the parent directory cannot be created, or when the rendered template
/// cannot be written.
pub fn write_initialized_template(output: &Path, rendered: &str, force: bool) -> Result<PathBuf> {
    let output = crate::path_util::absolute_path_cwd(output)?;
    if output.exists() && !force {
        bail!(
            "refusing to overwrite {}; pass --force to replace it",
            output.display()
        );
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    fs::write(&output, rendered).context(format!("failed to write {}", output.display()))?;
    Ok(output)
}

/// Returns the next CLI commands shown after `new` writes a compose file.
#[must_use]
pub fn next_commands(output: &Path) -> Vec<String> {
    let path = output.display().to_string();
    vec![
        format!("hpc-compose up -f {path}"),
        format!("hpc-compose validate -f {path}"),
        format!("hpc-compose inspect -f {path}"),
    ]
}

fn prompt(
    input: &mut impl BufRead,
    output: &mut impl Write,
    label: &str,
    default: &str,
) -> Result<String> {
    write!(
        output,
        "{} [{}]: ",
        term::styled_bold(label),
        term::styled_dim(default)
    )
    .ok();
    output.flush().ok();
    let mut line = String::new();
    input
        .read_line(&mut line)
        .context("failed to read interactive input")?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn prompt_required(
    input: &mut impl BufRead,
    output: &mut impl Write,
    label: &str,
    guidance: &str,
) -> Result<String> {
    write!(output, "{}: ", term::styled_bold(label)).ok();
    output.flush().ok();
    let mut line = String::new();
    input
        .read_line(&mut line)
        .context("failed to read interactive input")?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        bail!("{label} cannot be empty; {guidance}");
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::spec::ComposeSpec;

    #[test]
    fn templates_are_resolvable() {
        assert!(!templates().is_empty());
        assert_eq!(templates()[0].name, "dev-python-app");
        for template in templates() {
            let resolved = resolve_template(template.name).expect("resolve");
            assert_eq!(resolved.name, template.name);
            let resolved =
                resolve_template(&format!("{}.yaml", template.name)).expect("resolve yaml");
            assert_eq!(resolved.name, template.name);
        }
    }

    #[test]
    fn cache_dir_placeholder_and_next_commands_match_expected_defaults() {
        assert_eq!(cache_dir_placeholder(), "<shared-cache-dir>");
        assert_eq!(
            next_commands(Path::new("/tmp/demo.yaml")),
            vec![
                "hpc-compose up -f /tmp/demo.yaml",
                "hpc-compose validate -f /tmp/demo.yaml",
                "hpc-compose inspect -f /tmp/demo.yaml",
            ]
        );
    }

    #[test]
    fn resolve_template_reports_unknown_name() {
        let err = resolve_template("missing-template").expect_err("missing");
        assert!(
            err.to_string()
                .contains("unknown template 'missing-template'")
        );
        assert!(err.to_string().contains("dev-python-app"));
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
        let value: Value = serde_norway::from_str(&rendered).expect("yaml");
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

    #[test]
    fn render_template_body_inserts_x_slurm_when_missing() {
        let rendered = render_template_body(
            "services:\n  app:\n    image: redis:7\n",
            "inline",
            "custom-app",
            "/cache/path",
        )
        .expect("render");
        let value: Value = serde_norway::from_str(&rendered).expect("yaml");
        let root = value.as_mapping().expect("root");
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
    }

    #[test]
    fn render_template_body_reports_invalid_shapes() {
        let err = render_template_body("[]\n", "inline", "custom-app", "/cache/path")
            .expect_err("non mapping root");
        assert!(err.to_string().contains("template root must be a mapping"));

        let err = render_template_body(
            "x-slurm: nope\nservices:\n  app:\n    image: redis:7\n",
            "inline",
            "custom-app",
            "/cache/path",
        )
        .expect_err("invalid x-slurm");
        assert!(
            err.to_string()
                .contains("template x-slurm must be a mapping")
        );
    }

    #[test]
    fn prompt_uses_defaults_and_custom_values() {
        let mut input = Cursor::new(b"\ncustom\n");
        let mut output = Vec::new();
        assert_eq!(
            prompt(&mut input, &mut output, "Application name", "demo").expect("prompt"),
            "demo"
        );
        assert_eq!(
            prompt(&mut input, &mut output, "Cache dir", "/cache").expect("prompt"),
            "custom"
        );
        let transcript = String::from_utf8(output).expect("utf8");
        assert!(transcript.contains("Application name"));
        assert!(transcript.contains("demo"));
        assert!(transcript.contains("Cache dir"));
        assert!(transcript.contains("/cache"));
    }

    #[test]
    fn prompt_required_rejects_blank_values() {
        let mut input = Cursor::new(b"\n");
        let mut output = Vec::new();
        let err = prompt_required(&mut input, &mut output, "Cache dir", "choose a shared path")
            .expect_err("blank required value");
        assert_eq!(
            err.to_string(),
            "Cache dir cannot be empty; choose a shared path"
        );
        let output_str = String::from_utf8(output).expect("utf8");
        assert!(output_str.contains("Cache dir"));
    }

    #[test]
    fn prompt_for_init_with_io_covers_defaults_custom_values_and_validation() {
        let mut defaults_input = Cursor::new(b"\n\n/shared/cache\n");
        let mut defaults_output = Vec::new();
        let answers = prompt_for_init_with_io(&mut defaults_input, &mut defaults_output, None)
            .expect("defaults");
        assert_eq!(answers.template_name, "dev-python-app");
        assert_eq!(answers.app_name, "dev-python-app");
        assert_eq!(answers.cache_dir, "/shared/cache");
        assert!(
            String::from_utf8(defaults_output)
                .expect("utf8")
                .contains("Choose a template:")
        );

        let mut custom_input = Cursor::new(b"2\ncustom-app\n/custom-cache\n");
        let mut custom_output = Vec::new();
        let answers =
            prompt_for_init_with_io(&mut custom_input, &mut custom_output, None).expect("custom");
        assert_eq!(answers.template_name, "app-redis-worker");
        assert_eq!(answers.app_name, "custom-app");
        assert_eq!(answers.cache_dir, "/custom-cache");

        let mut invalid_input = Cursor::new(b"99\n");
        let mut invalid_output = Vec::new();
        let err = prompt_for_init_with_io(&mut invalid_input, &mut invalid_output, None)
            .expect_err("invalid");
        assert!(
            err.to_string()
                .contains("template selection must be one of the listed numbers")
        );

        let mut blank_cache_input = Cursor::new(b"\n\n\n");
        let mut blank_cache_output = Vec::new();
        let err = prompt_for_init_with_io(&mut blank_cache_input, &mut blank_cache_output, None)
            .expect_err("blank cache dir");
        assert!(err.to_string().contains("Cache dir cannot be empty"));
    }

    #[test]
    fn prompt_for_init_with_io_uses_supplied_cache_dir_default() {
        let mut input = Cursor::new(b"2\ncustom-app\n\n");
        let mut output = Vec::new();
        let answers = prompt_for_init_with_io(
            &mut input,
            &mut output,
            Some("/cluster/shared/custom-cache"),
        )
        .expect("answers");
        assert_eq!(answers.template_name, "app-redis-worker");
        assert_eq!(answers.app_name, "custom-app");
        assert_eq!(answers.cache_dir, "/cluster/shared/custom-cache");
        let output_str = String::from_utf8(output).expect("utf8");
        assert!(output_str.contains("Cache dir"));
        assert!(output_str.contains("/cluster/shared/custom-cache"));
    }

    #[test]
    fn write_initialized_template_and_absolute_path_cover_relative_and_force_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let current_dir = std::env::current_dir().expect("current dir");

        assert_eq!(
            crate::path_util::absolute_path_cwd(Path::new("nested/compose.yaml"))
                .expect("absolute")
                .strip_prefix(&current_dir)
                .expect("relative to cwd"),
            Path::new("nested/compose.yaml")
        );
        assert_eq!(
            crate::path_util::absolute_path_cwd(Path::new("/tmp/absolute.yaml")).expect("absolute"),
            PathBuf::from("/tmp/absolute.yaml")
        );

        let relative = tmpdir.path().join("nested/compose.yaml");
        let written =
            write_initialized_template(&relative, "name: demo\n", false).expect("write relative");
        assert_eq!(written, relative);
        assert_eq!(fs::read_to_string(&written).expect("read"), "name: demo\n");

        let err =
            write_initialized_template(&relative, "name: other\n", false).expect_err("overwrite");
        assert!(err.to_string().contains("refusing to overwrite"));

        write_initialized_template(&relative, "name: forced\n", true).expect("force overwrite");
        assert_eq!(
            fs::read_to_string(&written).expect("read"),
            "name: forced\n"
        );
    }
}
