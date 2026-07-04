use std::fmt::Display;
use std::path::PathBuf;

#[derive(Debug, miette::Diagnostic, thiserror::Error)]
pub(crate) enum SpecError {
    #[error("spec must contain a top-level 'services' or 'steps' mapping")]
    #[diagnostic(
        code(hpc_compose::spec::missing_services),
        help("Add a top-level `services:` key with at least one service definition.")
    )]
    MissingServices,

    #[error("'{field}' must be a mapping, got {got}")]
    #[diagnostic(
        code(hpc_compose::spec::invalid_type),
        help("Ensure the field contains a YAML mapping (key: value pairs).")
    )]
    InvalidFieldType { field: String, got: String },

    #[error("{scope} uses unsupported key '{key}'")]
    #[diagnostic(code(hpc_compose::spec::unsupported_key), help("{help_text}"))]
    UnsupportedServiceKey {
        scope: String,
        key: String,
        help_text: String,
    },

    #[error(
        "unsupported hpc-compose spec version '{version}'; this build supports version '{supported}'. {help_text}"
    )]
    #[diagnostic(code(hpc_compose::spec::unsupported_version), help("{help_text}"))]
    UnsupportedSpecVersion {
        version: String,
        supported: &'static str,
        help_text: String,
    },

    #[error(
        "top-level 'version' must be string \"1\" or integer 1, got {got}. Use `version: \"1\"` or omit `version` for v1 specs."
    )]
    #[diagnostic(
        code(hpc_compose::spec::invalid_version),
        help("Use `version: \"1\"` or omit `version` for v1 specs.")
    )]
    InvalidSpecVersion { got: String },

    #[error("x-slurm.artifacts.export_dir is required when x-slurm.artifacts is present")]
    #[diagnostic(
        code(hpc_compose::spec::artifacts_missing_export_dir),
        help("Add `export_dir: ./results/${{SLURM_JOB_ID}}` under `x-slurm.artifacts`.")
    )]
    ArtifactsMissingExportDir,

    #[error(
        "x-slurm.artifacts.paths entries must be absolute paths under /hpc-compose/job, got '{path}'"
    )]
    #[diagnostic(
        code(hpc_compose::spec::artifacts_invalid_path),
        help(
            "Artifact paths must start with `/hpc-compose/job/`. This directory is automatically mounted inside every service."
        )
    )]
    ArtifactsInvalidPath { path: String },

    #[error("{scope}.parallelism.{field} must be at least 1")]
    #[diagnostic(
        code(hpc_compose::spec::parallelism_non_positive),
        help("Set `tensor` and `pipeline` to positive integers under `parallelism`.")
    )]
    ParallelismNonPositive { scope: String, field: String },

    #[error(
        "{scope}.parallelism tensor({tensor}) * pipeline({pipeline}) = {product} must equal nodes({nodes}) * gpus_per_node({gpus_per_node}) = {expected}"
    )]
    #[diagnostic(
        code(hpc_compose::spec::parallelism_gpu_mismatch),
        help(
            "Adjust `parallelism.tensor`/`parallelism.pipeline` or `nodes`/`gpus_per_node` so that tensor * pipeline equals the total GPU count (nodes * gpus_per_node)."
        )
    )]
    ParallelismGpuMismatch {
        scope: String,
        tensor: u32,
        pipeline: u32,
        nodes: u32,
        gpus_per_node: u32,
        product: u64,
        expected: u64,
    },

    #[error("x-slurm.resume.path must be an absolute host path, got '{path}'")]
    #[diagnostic(
        code(hpc_compose::spec::resume_relative_path),
        help(
            "Use an absolute path like `/shared/$USER/runs/my-run` that is visible from both the login node and compute nodes."
        )
    )]
    ResumeRelativePath { path: String },

    #[error("readiness and healthcheck are mutually exclusive for service '{service}'")]
    #[diagnostic(
        code(hpc_compose::spec::readiness_healthcheck_conflict),
        help(
            "Remove one of `readiness` or `healthcheck`. Use `readiness` for native probes or `healthcheck` for Compose-compatible syntax."
        )
    )]
    ReadinessHealthcheckConflict { service: String },

    #[error(
        "service '{service}' mixes {form_a}-form entrypoint with multi-line string command; use script or an explicit command list instead"
    )]
    #[diagnostic(
        code(hpc_compose::spec::mixed_command_forms),
        help(
            "Use either `script:` for multi-line shell, or matching `command`/`entrypoint` forms (both string or both list)."
        )
    )]
    MixedCommandForms {
        service: String,
        form_a: String,
        form_b: String,
    },

    #[error("healthcheck.test must start with CMD or CMD-SHELL for Compose compatibility")]
    #[diagnostic(
        code(hpc_compose::spec::healthcheck_invalid_test),
        help(
            "Use `[\"CMD\", \"nc\", \"-z\", \"localhost\", \"8080\"]` or `[\"CMD-SHELL\", \"curl -f http://localhost:8080/health\"]`."
        )
    )]
    HealthcheckInvalidTest,

    #[error("x-slurm.metrics.interval_seconds must be at least 1")]
    #[diagnostic(
        code(hpc_compose::spec::metrics_interval_too_low),
        help("Use a value of at least 1 second. The default is 5 seconds.")
    )]
    MetricsIntervalTooLow,

    #[error("service '{service}' sets both script and {conflict}; use only one")]
    #[diagnostic(
        code(hpc_compose::spec::script_command_conflict),
        help(
            "Remove either `script:` or the conflicting `command:`/`entrypoint:` key. `script` is shorthand for `/bin/sh -lc '<script>'`."
        )
    )]
    ScriptCommandConflict { service: String, conflict: String },

    #[error("service '{service}' env_file '{}' does not exist", path.display())]
    #[diagnostic(
        code(hpc_compose::spec::env_file_not_found),
        help(
            "env_file paths are read relative to the compose file's directory, on the machine running `hpc-compose` (the submit host) -- not inside the container and not staged to the compute node. Check the path, or commit the file if it is missing from version control."
        )
    )]
    EnvFileNotFound { service: String, path: PathBuf },

    #[error("service '{service}' env_file '{}' line {line}: {reason}", path.display())]
    #[diagnostic(
        code(hpc_compose::spec::env_file_malformed_line),
        help(
            "Each non-empty, non-comment line must be `KEY=VALUE`, optionally prefixed with `export `. Quote values containing spaces with single or double quotes."
        )
    )]
    EnvFileMalformedLine {
        service: String,
        path: PathBuf,
        line: usize,
        reason: String,
    },

    #[error(
        "service '{service}' sets both x-runtime.prepare and x-enroot.prepare; use only x-runtime.prepare for new specs"
    )]
    #[diagnostic(
        code(hpc_compose::spec::duplicate_prepare_hook),
        help(
            "`x-enroot.prepare` is a Pyxis/Enroot compatibility spelling. `x-runtime.prepare` works across all backends."
        )
    )]
    DuplicatePrepareHook { service: String },

    #[error(
        "service '{service}' uses x-enroot.prepare with runtime.backend={backend}; use x-runtime.prepare for non-Pyxis backends"
    )]
    #[diagnostic(
        code(hpc_compose::spec::enroot_prepare_requires_pyxis),
        help(
            "Switch to `x-runtime.prepare` which is backend-agnostic, or set `runtime.backend: pyxis`."
        )
    )]
    EnrootPrepareRequiresPyxis { service: String, backend: String },

    #[error(
        "depends_on condition for service '{service}' must be 'service_started', 'service_healthy', or 'service_completed_successfully', got '{got}'"
    )]
    #[diagnostic(
        code(hpc_compose::spec::invalid_dependency_condition),
        help("{help_text}")
    )]
    InvalidDependencyCondition {
        service: String,
        got: String,
        help_text: String,
    },

    #[error("environment list items must use KEY=VALUE syntax")]
    #[diagnostic(
        code(hpc_compose::spec::invalid_environment_entry),
        help(
            "Each list item under `environment:` must be a `KEY=VALUE` string, or switch to mapping form (`KEY: VALUE`)."
        )
    )]
    InvalidEnvironmentEntry,

    #[error("x-slurm.artifacts must contain at least one source path in paths or bundles")]
    #[diagnostic(
        code(hpc_compose::spec::artifacts_no_sources),
        help("Add at least one entry under `paths:` or define a named `bundles:` entry.")
    )]
    ArtifactsNoSources,

    #[error("x-slurm.artifacts.paths must not read from /hpc-compose/job/artifacts")]
    #[diagnostic(
        code(hpc_compose::spec::artifacts_reads_export_tree),
        help(
            "Artifact collection sources must not read from the export directory itself. Use a different `/hpc-compose/job/` subpath."
        )
    )]
    ArtifactsReadsExportTree,

    #[error("x-slurm.resume.path must be a host path, not a container-visible /hpc-compose path")]
    #[diagnostic(
        code(hpc_compose::spec::resume_container_path),
        help(
            "Use an absolute host path like `/shared/$USER/runs/my-run` that is visible from both the login node and compute nodes."
        )
    )]
    ResumeContainerPath { path: String },

    #[error(
        "healthcheck.{field} is not supported; use healthcheck.timeout or explicit readiness instead"
    )]
    #[diagnostic(
        code(hpc_compose::spec::healthcheck_unsupported_field),
        help(
            "Only `test`, `timeout`, and `disable` are supported in `healthcheck`. Use the native `readiness` block for interval/retries/start_period semantics."
        )
    )]
    HealthcheckUnsupportedField { service: String, field: String },

    #[error("{field} value '{value}' is not a valid Slurm time limit")]
    #[diagnostic(
        code(hpc_compose::spec::invalid_slurm_time),
        help(
            "Use one of Slurm's `--time` formats: minutes (`90`), MM:SS (`90:00`), HH:MM:SS (`1:00:00`), D-HH (`1-00`), D-HH:MM (`1-00:30`), or D-HH:MM:SS (`1-00:30:00`). A bare `1h`/`30m` is not accepted; write `1h` as `1:00:00` and `30m` as `30`."
        )
    )]
    InvalidSlurmTime { field: String, value: String },

    #[error("{field} has an invalid mount '{value}': {problem}")]
    #[diagnostic(
        code(hpc_compose::spec::invalid_mount_syntax),
        help(
            "Use `host_path:container_path[:ro|rw]`, e.g. `./data:/workspace/data` or `/scratch/models:/models:ro`. The container path must be absolute and the optional mode must be `ro` or `rw`."
        )
    )]
    InvalidMountSyntax {
        field: String,
        value: String,
        problem: String,
    },

    #[error("{scope} sets both gpus and a gpu gres request; they are contradictory")]
    #[diagnostic(
        code(hpc_compose::spec::gpus_gres_conflict),
        help(
            "Slurm renders `--gres` and ignores `gpus` when both are set. Keep only one: either `gpus: <n>` for a simple count, or `gres: gpu:<type?>:<n>` for a typed request."
        )
    )]
    GpusGresConflict { scope: String },

    #[error("x-slurm.notify.email.on includes 'array_tasks', but x-slurm.array is not set")]
    #[diagnostic(
        code(hpc_compose::spec::array_tasks_requires_array),
        help(
            "`array_tasks` tells Slurm to mail once per array task, so it only makes sense for a job array. Add `x-slurm.array: \"0-9\"` (or your index range), or remove `array_tasks` from `x-slurm.notify.email.on`."
        )
    )]
    ArrayTasksRequiresArray,

    #[error("{field} must not be empty")]
    #[diagnostic(
        code(hpc_compose::spec::empty_field),
        help("Provide a non-empty value or remove the field if it is optional.")
    )]
    EmptyField { field: String },

    #[error("{message}")]
    #[diagnostic(code(hpc_compose::spec::required_variable_unset), help("{help_text}"))]
    RequiredVariableUnset {
        name: String,
        message: String,
        help_text: String,
    },

    #[error("failed to load compose spec from {}", path.display())]
    #[diagnostic(
        code(hpc_compose::spec::load_failed),
        help(
            "Ensure the file exists and contains valid YAML. Run `hpc-compose schema` to see the expected structure."
        )
    )]
    LoadFailed {
        path: PathBuf,
        #[source]
        source: anyhow::Error,
    },

    #[error("compose spec not found at {}", path.display())]
    #[diagnostic(
        code(hpc_compose::spec::file_not_found),
        help(
            "No spec file at that path. Create one with `hpc-compose new` (run `hpc-compose new --help` for templates) or scaffold it interactively with `hpc-compose evolve`. The default path is `compose.yaml`; pass `-f <path>` to point elsewhere."
        )
    )]
    SpecFileNotFound { path: PathBuf },
}

pub(crate) fn cli_error_report(error: anyhow::Error) -> miette::Report {
    miette::Report::new(CliError(error))
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct CliError(anyhow::Error);

impl CliError {
    fn spec_error(&self) -> Option<&SpecError> {
        self.0.downcast_ref::<SpecError>()
    }
}

impl miette::Diagnostic for CliError {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        self.spec_error()
            .and_then(|error| error.code())
            .or_else(|| Some(Box::new("hpc_compose::error")))
    }

    fn severity(&self) -> Option<miette::Severity> {
        self.spec_error().and_then(|error| error.severity())
    }

    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        self.spec_error().and_then(|error| error.help())
    }

    fn url<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        self.spec_error().and_then(|error| error.url())
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        self.spec_error().and_then(|error| error.source_code())
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        self.spec_error().and_then(|error| error.labels())
    }

    fn related<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a dyn miette::Diagnostic> + 'a>> {
        self.spec_error().and_then(|error| error.related())
    }

    fn diagnostic_source(&self) -> Option<&dyn miette::Diagnostic> {
        self.spec_error()
            .and_then(|error| error.diagnostic_source())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use miette::Diagnostic;

    use super::*;

    fn load_failed_error() -> SpecError {
        SpecError::LoadFailed {
            path: PathBuf::from("missing.yaml"),
            source: io::Error::new(io::ErrorKind::NotFound, "missing file").into(),
        }
    }

    #[test]
    fn cli_error_report_preserves_inner_spec_diagnostic_metadata() {
        let report = cli_error_report(
            anyhow::Error::from(load_failed_error()).context("while loading the compose file"),
        );

        assert_eq!(report.to_string(), "while loading the compose file");
        assert_eq!(
            report.code().expect("diagnostic code").to_string(),
            "hpc_compose::spec::load_failed"
        );
        assert!(
            report
                .help()
                .expect("help text")
                .to_string()
                .contains("Ensure the file exists")
        );
    }

    #[test]
    fn cli_error_report_keeps_generic_errors_generic() {
        let report = cli_error_report(anyhow::anyhow!("plain failure"));

        assert_eq!(report.to_string(), "plain failure");
        assert_eq!(
            report.code().expect("generic code").to_string(),
            "hpc_compose::error"
        );
        assert!(report.help().is_none());
    }

    #[test]
    fn spec_error_variants_expose_expected_diagnostic_metadata() {
        let invalid_type = SpecError::InvalidFieldType {
            field: "services".into(),
            got: "sequence".into(),
        };
        assert_eq!(
            invalid_type.to_string(),
            "'services' must be a mapping, got sequence"
        );
        assert_eq!(
            invalid_type.code().expect("invalid type code").to_string(),
            "hpc_compose::spec::invalid_type"
        );
        assert!(
            invalid_type
                .help()
                .expect("invalid type help")
                .to_string()
                .contains("YAML mapping")
        );
        assert!(invalid_type.url().is_none());
        assert!(invalid_type.source_code().is_none());
        assert!(invalid_type.labels().is_none());
        assert!(invalid_type.related().is_none());
        assert!(invalid_type.diagnostic_source().is_none());

        let unsupported = SpecError::UnsupportedServiceKey {
            scope: "service 'api'".into(),
            key: "build".into(),
            help_text: "Use image instead.".into(),
        };
        assert_eq!(
            unsupported
                .code()
                .expect("unsupported key code")
                .to_string(),
            "hpc_compose::spec::unsupported_key"
        );
        assert_eq!(
            unsupported
                .help()
                .expect("unsupported key help")
                .to_string(),
            "Use image instead."
        );
    }

    #[test]
    fn required_variable_unset_variant_exposes_message_and_help() {
        let err = SpecError::RequiredVariableUnset {
            name: "HF_TOKEN".into(),
            message: "'HF_TOKEN' is required: set a token".into(),
            help_text: "Set `HF_TOKEN` before running this command.".into(),
        };
        assert_eq!(err.to_string(), "'HF_TOKEN' is required: set a token");
        assert_eq!(
            err.code().expect("required variable code").to_string(),
            "hpc_compose::spec::required_variable_unset"
        );
        assert!(
            err.help()
                .expect("required variable help")
                .to_string()
                .contains("HF_TOKEN")
        );
    }

    #[test]
    fn load_failed_variant_exposes_source_and_help() {
        let err = load_failed_error();
        assert!(err.source().is_some());
        assert_eq!(
            err.code().expect("load failed code").to_string(),
            "hpc_compose::spec::load_failed"
        );
        assert!(
            err.help()
                .expect("load failed help")
                .to_string()
                .contains("Ensure the file exists")
        );
    }
}
