//! Static authoring diagnostics for editors and agents.
//!
//! This layer diagnoses one in-memory compose document without contacting
//! Slurm, SSH, the network, or prepare/render mutation paths. It intentionally
//! returns a small internal diagnostic model so protocol adapters can translate
//! it to LSP or other authoring surfaces.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use miette::Diagnostic as _;

use crate::cluster::{discover_cluster_profile_path, load_cluster_profile};
use crate::context::{ResolveRequest, resolve_with_compose_text};
use crate::lint::{LintLevel, lint_plan};
use crate::planner::{PlanOptions, build_plan_with_options};
use crate::runtime_plan::build_runtime_plan;
use crate::spec::{ComposeSpec, missing_defaulted_variables_from_str_at_path};
use crate::spec_error::SpecError;

const CODE_CONTEXT: &str = "hpc_compose::authoring::context";
const CODE_STRICT_ENV: &str = "hpc_compose::authoring::strict_env";
const CODE_LOAD: &str = "hpc_compose::authoring::load";
const CODE_PLAN: &str = "hpc_compose::authoring::plan";
const CODE_CLUSTER_PROFILE: &str = "hpc_compose::authoring::cluster_profile";

const REC_CONTEXT: &str =
    "Check --profile/--settings-file, settings env files, .env, and top-level secrets.";
const REC_STRICT_ENV: &str =
    "Set the missing variables or remove default fallback interpolation syntax.";
const REC_LOAD: &str = "Fix YAML, interpolation, unsupported keys, or semantic spec errors.";
const REC_PLAN: &str =
    "Adjust service dependencies, placement, resources, cache paths, or planning settings.";
const REC_CLUSTER_PROFILE: &str = "Fix .hpc-compose/cluster.toml or remove it while authoring.";

/// Diagnostic severity for static authoring feedback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthoringSeverity {
    /// A blocking parse, validation, or planning error.
    Error,
    /// A non-blocking lint or cluster-profile warning.
    Warning,
}

/// Zero-based source range in the diagnosed document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthoringRange {
    pub(crate) start_line: u32,
    pub(crate) start_character: u32,
    pub(crate) end_line: u32,
    pub(crate) end_character: u32,
}

/// One static authoring diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthoringDiagnostic {
    pub(crate) severity: AuthoringSeverity,
    pub(crate) message: String,
    pub(crate) code: Option<String>,
    pub(crate) field: Option<String>,
    pub(crate) recommendation: Option<String>,
    pub(crate) range: AuthoringRange,
}

/// Context used to diagnose one open compose document.
#[derive(Debug, Clone)]
pub(crate) struct AuthoringDiagnosticOptions {
    pub(crate) cwd: PathBuf,
    pub(crate) profile: Option<String>,
    pub(crate) settings_file: Option<PathBuf>,
    pub(crate) strict_env: bool,
}

/// Best-effort index from hpc-compose field paths to source ranges.
#[derive(Debug, Clone)]
pub(crate) struct YamlPathIndex {
    ranges: BTreeMap<String, AuthoringRange>,
    whole_document: AuthoringRange,
}

#[derive(Debug, Clone)]
struct StackEntry {
    indent: usize,
    segment: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct AuthoringPhaseError {
    code: &'static str,
    recommendation: &'static str,
    #[source]
    source: Error,
}

impl AuthoringPhaseError {
    fn new(code: &'static str, recommendation: &'static str, source: Error) -> Self {
        Self {
            code,
            recommendation,
            source,
        }
    }
}

impl YamlPathIndex {
    /// Builds a best-effort field-path index from YAML text.
    pub(crate) fn new(text: &str) -> Self {
        let mut ranges = BTreeMap::new();
        let mut stack: Vec<StackEntry> = Vec::new();
        let mut last_line = 0_u32;
        let mut last_character = 0_u32;

        for (line_no, line) in text.lines().enumerate() {
            last_line = line_no as u32;
            last_character = character_count(line) as u32;

            let Some(parsed) = ParsedYamlLine::parse(line) else {
                continue;
            };

            match parsed.kind {
                ParsedYamlLineKind::Key { key, has_value } => {
                    while matches!(stack.last(), Some(entry) if entry.indent >= parsed.indent) {
                        stack.pop();
                    }
                    let mut path = stack_path(&stack);
                    path.push(key.clone());
                    let field = path.join(".");
                    ranges.insert(field, line_range(line_no, parsed.key_start, line));
                    stack.push(StackEntry {
                        indent: parsed.indent,
                        segment: key,
                    });
                    if has_value {
                        continue;
                    }
                }
                ParsedYamlLineKind::ListScalar { value } => {
                    let mut path = stack_path_including_equal_indent(&stack, parsed.indent);
                    path.push(value);
                    ranges.insert(path.join("."), line_range(line_no, parsed.key_start, line));
                }
                ParsedYamlLineKind::ListKey { key, has_value } => {
                    let mut path = stack_path_including_equal_indent(&stack, parsed.indent);
                    path.push(key.clone());
                    ranges.insert(path.join("."), line_range(line_no, parsed.key_start, line));
                    if !has_value {
                        stack.push(StackEntry {
                            indent: parsed.indent + 2,
                            segment: key,
                        });
                    }
                }
            }
        }

        let whole_document = AuthoringRange {
            start_line: 0,
            start_character: 0,
            end_line: last_line,
            end_character: last_character.max(1),
        };

        Self {
            ranges,
            whole_document,
        }
    }

    /// Returns the best-known range for a field path, falling back to the
    /// nearest parent field and then to the whole document.
    pub(crate) fn range_for_field(&self, field: Option<&str>) -> AuthoringRange {
        let Some(field) = field.filter(|field| !field.is_empty()) else {
            return self.whole_document;
        };
        let mut candidate = field.to_string();
        loop {
            if let Some(range) = self.ranges.get(&candidate) {
                return *range;
            }
            let Some((parent, _)) = candidate.rsplit_once('.') else {
                break;
            };
            candidate = parent.to_string();
        }
        self.whole_document
    }
}

#[derive(Debug, Clone)]
struct ParsedYamlLine {
    indent: usize,
    key_start: usize,
    kind: ParsedYamlLineKind,
}

#[derive(Debug, Clone)]
enum ParsedYamlLineKind {
    Key { key: String, has_value: bool },
    ListScalar { value: String },
    ListKey { key: String, has_value: bool },
}

impl ParsedYamlLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        let indent = line.len() - trimmed.len();
        if let Some(rest) = trimmed.strip_prefix("- ") {
            if rest.trim().is_empty() || rest.trim_start().starts_with('#') {
                return None;
            }
            if let Some((raw_key, raw_value)) = split_key_value(rest) {
                let key = clean_yaml_key(raw_key)?;
                return Some(Self {
                    indent,
                    key_start: indent + 2,
                    kind: ParsedYamlLineKind::ListKey {
                        key,
                        has_value: !raw_value.trim().is_empty(),
                    },
                });
            }
            let value = clean_yaml_scalar(rest)?;
            return Some(Self {
                indent,
                key_start: indent + 2,
                kind: ParsedYamlLineKind::ListScalar { value },
            });
        }

        let (raw_key, raw_value) = split_key_value(trimmed)?;
        let key = clean_yaml_key(raw_key)?;
        Some(Self {
            indent,
            key_start: indent,
            kind: ParsedYamlLineKind::Key {
                key,
                has_value: !raw_value.trim().is_empty(),
            },
        })
    }
}

/// Diagnoses one in-memory hpc-compose YAML document.
pub(crate) fn diagnose_document(
    path: &Path,
    text: &str,
    options: &AuthoringDiagnosticOptions,
) -> Vec<AuthoringDiagnostic> {
    let index = YamlPathIndex::new(text);
    match diagnose_document_inner(path, text, options, &index) {
        Ok(diagnostics) => diagnostics,
        Err(error) => vec![blocking_error(error, &index)],
    }
}

fn diagnose_document_inner(
    path: &Path,
    text: &str,
    options: &AuthoringDiagnosticOptions,
    index: &YamlPathIndex,
) -> Result<Vec<AuthoringDiagnostic>> {
    let context = resolve_with_compose_text(
        &ResolveRequest {
            cwd: options.cwd.clone(),
            profile: options.profile.clone(),
            settings_file: options.settings_file.clone(),
            compose_file_override: Some(path.to_path_buf()),
            ..ResolveRequest::default()
        },
        text,
    )
    .map_err(|source| Error::from(AuthoringPhaseError::new(CODE_CONTEXT, REC_CONTEXT, source)))?;

    let spec =
        ComposeSpec::load_with_interpolation_vars_from_str(path, text, &context.interpolation_vars)
            .map_err(|source| Error::from(AuthoringPhaseError::new(CODE_LOAD, REC_LOAD, source)))?;
    let project_dir = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let plan = build_plan_with_options(
        path,
        spec,
        PlanOptions {
            cache_dir_default: Some(context.cache_dir.value.clone()),
            resource_profiles: context.resource_profiles.clone(),
            project_dir_override: Some(project_dir.clone()),
            allow_missing_spec_path: true,
        },
    )
    .map_err(|source| Error::from(AuthoringPhaseError::new(CODE_PLAN, REC_PLAN, source)))?;
    let runtime_plan = build_runtime_plan(&plan);
    if options.strict_env {
        let missing =
            missing_defaulted_variables_from_str_at_path(path, text, &context.interpolation_vars)
                .map_err(|source| {
                Error::from(AuthoringPhaseError::new(
                    CODE_STRICT_ENV,
                    REC_STRICT_ENV,
                    source,
                ))
            })?;
        if !missing.is_empty() {
            return Err(Error::from(AuthoringPhaseError::new(
                CODE_STRICT_ENV,
                REC_STRICT_ENV,
                anyhow::anyhow!(
                    "strict env validation failed; missing variables consumed default fallbacks: {}",
                    missing.into_iter().collect::<Vec<_>>().join(", ")
                ),
            )));
        }
    }
    let cluster_profile = load_discovered_cluster_profile(&project_dir).map_err(|source| {
        Error::from(AuthoringPhaseError::new(
            CODE_CLUSTER_PROFILE,
            REC_CLUSTER_PROFILE,
            source,
        ))
    })?;

    Ok(lint_plan(&plan, &runtime_plan, cluster_profile.as_ref())
        .into_iter()
        .map(|finding| {
            let field = finding.field.clone();
            AuthoringDiagnostic {
                severity: match finding.level {
                    LintLevel::Warning => AuthoringSeverity::Warning,
                    LintLevel::Error => AuthoringSeverity::Error,
                },
                message: finding.message,
                code: Some(finding.code),
                field: field.clone(),
                recommendation: finding.recommendation,
                range: index.range_for_field(field.as_deref()),
            }
        })
        .collect())
}

fn load_discovered_cluster_profile(start: &Path) -> Result<Option<crate::cluster::ClusterProfile>> {
    let Some(path) = discover_cluster_profile_path(start) else {
        return Ok(None);
    };
    Ok(Some(load_cluster_profile(&path)?))
}

fn blocking_error(error: Error, index: &YamlPathIndex) -> AuthoringDiagnostic {
    let spec_error = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<SpecError>());
    let phase_error = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<AuthoringPhaseError>());
    let field = spec_error.and_then(spec_error_field_path);
    let code = spec_error
        .and_then(diagnostic_code)
        .or_else(|| phase_error.map(|error| error.code.to_string()));
    let recommendation = spec_error
        .and_then(diagnostic_help)
        .or_else(|| phase_error.map(|error| error.recommendation.to_string()));
    AuthoringDiagnostic {
        severity: AuthoringSeverity::Error,
        message: error.to_string(),
        code,
        field: field.clone(),
        recommendation,
        range: index.range_for_field(field.as_deref()),
    }
}

fn spec_error_field_path(error: &SpecError) -> Option<String> {
    match error {
        SpecError::MissingServices => Some("services".to_string()),
        SpecError::InvalidFieldType { field, .. } => Some(scope_to_field_path(field)),
        SpecError::UnsupportedServiceKey { scope, key, .. } => {
            let scope = scope_to_field_path(scope);
            if scope == "root" {
                Some(key.clone())
            } else {
                Some(format!("{scope}.{key}"))
            }
        }
        SpecError::UnsupportedSpecVersion { .. } | SpecError::InvalidSpecVersion { .. } => {
            Some("version".to_string())
        }
        SpecError::ArtifactsMissingExportDir => Some("x-slurm.artifacts.export_dir".to_string()),
        SpecError::ArtifactsInvalidPath { .. }
        | SpecError::ArtifactsNoSources
        | SpecError::ArtifactsReadsExportTree => Some("x-slurm.artifacts.paths".to_string()),
        SpecError::ParallelismNonPositive { scope, field } => Some(format!(
            "{}.parallelism.{field}",
            scope_to_field_path(scope)
        )),
        SpecError::ParallelismGpuMismatch { scope, .. } => {
            Some(format!("{}.parallelism", scope_to_field_path(scope)))
        }
        SpecError::SignalDelayOutOfRange { .. } => Some("x-slurm.signal.at_seconds".to_string()),
        SpecError::ResumeRelativePath { .. } | SpecError::ResumeContainerPath { .. } => {
            Some("x-slurm.resume.path".to_string())
        }
        SpecError::ReadinessHealthcheckConflict { service } => {
            Some(format!("services.{service}.readiness"))
        }
        SpecError::MixedCommandForms { service, .. } => Some(format!("services.{service}.command")),
        SpecError::HealthcheckInvalidTest => None,
        SpecError::MetricsIntervalTooLow => Some("x-slurm.metrics.interval_seconds".to_string()),
        SpecError::ScriptCommandConflict { service, .. } => {
            Some(format!("services.{service}.script"))
        }
        SpecError::EnvFileNotFound { service, .. }
        | SpecError::EnvFileMalformedLine { service, .. } => {
            Some(format!("services.{service}.env_file"))
        }
        SpecError::DuplicatePrepareHook { service } => {
            Some(format!("services.{service}.x-runtime.prepare"))
        }
        SpecError::EnrootPrepareRequiresPyxis { service, .. } => {
            Some(format!("services.{service}.x-enroot.prepare"))
        }
        SpecError::InvalidDependencyCondition { service, .. } => {
            Some(format!("services.{service}.depends_on"))
        }
        SpecError::InvalidEnvironmentEntry => None,
        SpecError::HealthcheckUnsupportedField { service, field } => {
            Some(format!("services.{service}.healthcheck.{field}"))
        }
        SpecError::InvalidSlurmTime { field, .. }
        | SpecError::InvalidMountSyntax { field, .. }
        | SpecError::EmptyField { field } => Some(field.clone()),
        SpecError::GpusGresConflict { scope } => Some(scope_to_field_path(scope)),
        SpecError::ArrayTasksRequiresArray => Some("x-slurm.notify.email.on".to_string()),
        SpecError::RequiredVariableUnset { .. } => None,
        SpecError::SpecFileNotFound { .. } | SpecError::LoadFailed { .. } => None,
    }
}

fn diagnostic_code(error: &SpecError) -> Option<String> {
    error.code().map(|code| code.to_string())
}

fn diagnostic_help(error: &SpecError) -> Option<String> {
    error.help().map(|help| help.to_string())
}

fn scope_to_field_path(scope: &str) -> String {
    if scope == "root" {
        return "root".to_string();
    }
    if let Some(service) = scope
        .strip_prefix("service '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!("services.{service}");
    }
    scope.to_string()
}

fn split_key_value(line: &str) -> Option<(&str, &str)> {
    let mut in_single = false;
    let mut in_double = false;
    for (index, ch) in line.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ':' if !in_single && !in_double => {
                let rest = &line[index + ch.len_utf8()..];
                if rest.is_empty() || rest.starts_with(char::is_whitespace) || rest.starts_with('#')
                {
                    return Some((&line[..index], rest));
                }
            }
            _ => {}
        }
    }
    None
}

fn clean_yaml_key(raw: &str) -> Option<String> {
    clean_yaml_token(raw.trim())
}

fn clean_yaml_scalar(raw: &str) -> Option<String> {
    let raw = raw.split('#').next().unwrap_or(raw).trim();
    clean_yaml_token(raw)
}

fn clean_yaml_token(raw: &str) -> Option<String> {
    let value = raw.trim().trim_end_matches(',');
    if value.is_empty() || value.contains(['{', '}', '[', ']']) {
        return None;
    }
    let value = value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|rest| rest.strip_suffix('\''))
        })
        .unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn stack_path(stack: &[StackEntry]) -> Vec<String> {
    stack.iter().map(|entry| entry.segment.clone()).collect()
}

fn stack_path_including_equal_indent(stack: &[StackEntry], indent: usize) -> Vec<String> {
    stack
        .iter()
        .filter(|entry| entry.indent <= indent)
        .map(|entry| entry.segment.clone())
        .collect()
}

fn line_range(line_no: usize, start: usize, line: &str) -> AuthoringRange {
    AuthoringRange {
        start_line: line_no as u32,
        start_character: start as u32,
        end_line: line_no as u32,
        end_character: character_count(line).max(start + 1) as u32,
    }
}

fn character_count(line: &str) -> usize {
    line.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SPEC: &str = "\
services:
  app:
    image: alpine:3.20
    command: echo hi
";

    fn options(cwd: &Path) -> AuthoringDiagnosticOptions {
        AuthoringDiagnosticOptions {
            cwd: cwd.to_path_buf(),
            profile: None,
            settings_file: None,
            strict_env: false,
        }
    }

    #[test]
    fn yaml_path_index_maps_nested_fields_and_list_entries() {
        let index = YamlPathIndex::new(
            "\
name: demo
services:
  app:
    image: alpine:3.20
    depends_on:
      - api
    volumes:
      - /data:/data
",
        );

        assert_eq!(
            index.range_for_field(Some("name")).start_line,
            0,
            "top-level field"
        );
        assert_eq!(
            index.range_for_field(Some("services.app.image")).start_line,
            3,
            "nested service field"
        );
        assert_eq!(
            index
                .range_for_field(Some("services.app.depends_on.api"))
                .start_line,
            5,
            "list entry"
        );
        assert_eq!(
            index
                .range_for_field(Some("services.app.unknown.child"))
                .start_line,
            2,
            "unknown field falls back to nearest parent"
        );
    }

    #[test]
    fn valid_spec_returns_no_diagnostics() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(&path, VALID_SPEC, &options(tmp.path()));
        assert_eq!(diagnostics, Vec::new());
    }

    #[test]
    fn unsupported_service_key_maps_to_service_line() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "\
services:
  app:
    image: alpine:3.20
    ports:
      - 8080:8080
",
            &options(tmp.path()),
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].field.as_deref(), Some("services.app.ports"));
        assert_eq!(diagnostics[0].range.start_line, 3);
    }

    #[test]
    fn service_level_allocation_field_maps_to_nested_line() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "\
services:
  app:
    image: alpine:3.20
    x-slurm:
      partition: gpu
",
            &options(tmp.path()),
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].field.as_deref(),
            Some("services.app.x-slurm.partition")
        );
        assert_eq!(diagnostics[0].range.start_line, 4);
    }

    #[test]
    fn lint_warning_maps_to_depends_on_entry() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "\
services:
  api:
    image: alpine
  app:
    image: alpine
    depends_on:
      - api
",
            &options(tmp.path()),
        );

        let finding = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code.as_deref() == Some("HPC001"))
            .expect("HPC001");
        assert_eq!(
            finding.field.as_deref(),
            Some("services.app.depends_on.api")
        );
        assert_eq!(finding.range.start_line, 6);
    }

    #[test]
    fn malformed_yaml_returns_one_diagnostic() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(&path, "services:\n  app: [", &options(tmp.path()));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, AuthoringSeverity::Error);
        assert_eq!(diagnostics[0].code.as_deref(), Some(CODE_LOAD));
    }

    #[test]
    fn strict_env_preserves_malformed_yaml_load_diagnostic() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "services:\n  app: [",
            &AuthoringDiagnosticOptions {
                strict_env: true,
                ..options(tmp.path())
            },
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, AuthoringSeverity::Error);
        assert_eq!(diagnostics[0].code.as_deref(), Some(CODE_LOAD));
    }

    #[test]
    fn strict_env_reports_missing_default_after_valid_load() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "\
services:
  app:
    image: alpine:3.20
    command: [\"sh\", \"-lc\", \"echo ${MISSING:-fallback}\"]
",
            &AuthoringDiagnosticOptions {
                strict_env: true,
                ..options(tmp.path())
            },
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, AuthoringSeverity::Error);
        assert_eq!(diagnostics[0].code.as_deref(), Some(CODE_STRICT_ENV));
        assert!(diagnostics[0].message.contains("MISSING"));
    }

    #[test]
    fn context_errors_have_stable_authoring_code() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            VALID_SPEC,
            &AuthoringDiagnosticOptions {
                settings_file: Some(tmp.path().join("missing-settings.toml")),
                ..options(tmp.path())
            },
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code.as_deref(), Some(CODE_CONTEXT));
        assert_eq!(diagnostics[0].recommendation.as_deref(), Some(REC_CONTEXT));
    }

    #[test]
    fn planner_errors_have_stable_authoring_code() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let diagnostics = diagnose_document(
            &path,
            "\
services:
  app:
    image: alpine:3.20
    depends_on:
      - missing
",
            &options(tmp.path()),
        );

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code.as_deref(), Some(CODE_PLAN));
        assert_eq!(diagnostics[0].recommendation.as_deref(), Some(REC_PLAN));
    }

    #[test]
    fn unsaved_buffer_wins_over_stale_disk() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        std::fs::write(
            &path,
            "services:\n  app:\n    image: alpine:3.20\n    ports: []\n",
        )
        .expect("write stale file");

        let diagnostics = diagnose_document(&path, VALID_SPEC, &options(tmp.path()));
        assert_eq!(diagnostics, Vec::new());
    }

    #[test]
    fn open_buffer_secrets_drive_interpolation() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let secret_file = tmp.path().join("secret.txt");
        std::fs::write(&secret_file, "open-buffer-secret\n").expect("write secret");
        let path = tmp.path().join("compose.yaml");
        std::fs::write(
            &path,
            "\
secrets:
  token:
    env: MISSING_FROM_ENV
services:
  app:
    image: alpine:3.20
    environment:
      TOKEN: ${token}
",
        )
        .expect("write stale file");

        let diagnostics = diagnose_document(
            &path,
            &format!(
                "\
secrets:
  token:
    file: {}
services:
  app:
    image: alpine:3.20
    environment:
      TOKEN: ${{token}}
",
                secret_file.display()
            ),
            &options(tmp.path()),
        );
        assert_eq!(diagnostics, Vec::new());
    }
}
