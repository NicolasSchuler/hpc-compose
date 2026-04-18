use std::fmt::Display;
use std::path::PathBuf;

#[expect(
    dead_code,
    reason = "structured spec diagnostics are being introduced incrementally and not every variant is constructed yet"
)]
#[derive(Debug, miette::Diagnostic, thiserror::Error)]
pub(crate) enum SpecError {
    #[error("spec must contain a top-level 'services' mapping")]
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

    #[error("service '{service}' uses unsupported key '{key}'")]
    #[diagnostic(code(hpc_compose::spec::unsupported_key), help("{help_text}"))]
    UnsupportedServiceKey {
        service: String,
        key: String,
        help_text: String,
    },

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
        "service '{service}' has both 'command' ({form_a}) and 'entrypoint' ({form_b}); they must use the same form"
    )]
    #[diagnostic(
        code(hpc_compose::spec::mixed_command_forms),
        help(
            "Use either string form for both (`command: \"python app.py\"`) or list form for both (`command: [\"python\", \"app.py\"]`)."
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
            service: "api".into(),
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
