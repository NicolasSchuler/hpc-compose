//! Stable JSON output contract for `--format json` command output.
//!
//! Every command that emits `--format json` serializes a dedicated output DTO
//! that carries a top-level `schema_version`. These DTOs decouple the JSON
//! contract consumed by scripts from the internal and persisted structs used for
//! domain logic and on-disk state, so an internal field rename can no longer
//! silently break the JSON surface.
//!
//! Most leaks use a **flatten envelope** ([`flatten_envelope!`]): the envelope
//! prepends `schema_version` and `#[serde(flatten)]`s the report, so existing
//! fields keep their exact bytes (guarded by
//! [`tests::score_output_only_prepends_schema_version`]). A few commands emit a
//! bare JSON array or a record that already carries its own version
//! (`cache list`, `rendezvous list`, `rendezvous resolve`, `metrics-probe`);
//! those stay byte-unchanged and only register a published schema — wrapping
//! them would be a breaking, non-additive shape change.
//!
//! The generated JSON Schemas are checked in under `schema/outputs/` and served
//! by `hpc-compose schema --output <command>`. The `bless_output_schemas` test
//! regenerates them and `checked_in_output_schemas_are_current` fails on drift
//! (mirroring the manpage generate/check pattern), which doubles as the
//! field-set pin: adding, removing, or renaming a field changes the generated
//! schema and forces a deliberate re-bless.

use schemars::JsonSchema;
use serde::Serialize;

use hpc_compose::cache::CacheEntryManifest;
use hpc_compose::diagnostics::Notice;
use hpc_compose::docs_search::DocsSearchOutput;
use hpc_compose::evolve::EvolveRunReport;
use hpc_compose::job::{
    ArtifactExportReport, CheckpointHistory, CleanupReport, EfficiencyScoreReport, JobDiffReport,
    JobInventoryScan, JobMatrixReport, MetricsProbeReport, PsSnapshot, ReplayReport,
    RightsizeReport, SpecDiffReport, StatsSnapshot, StatusSnapshot,
};
use hpc_compose::preflight::GroupedReport;
use hpc_compose::prepare::{PrepareSummary, RuntimePlan};
use hpc_compose::rendezvous::{RendezvousPruneReport, RendezvousRecord};
use hpc_compose::spec::EffectiveComposeConfig;
use hpc_compose::weather::WeatherReport;

// Owned output DTOs that carry their own `schema_version` field (no envelope).
use super::{
    CacheInspectReport, CachePruneReport, CancelOutput, DependencyGraphOutput,
    InterpolationVarsOutput, RenderOutput, SetupOutput, SubmitOutput, TemplateDescriptionOutput,
    TemplateListOutput, TemplateWriteOutput, ValidateOutput,
};

/// Defines a flatten-envelope output DTO: a named `$name` struct that prepends a
/// top-level `schema_version` and `#[serde(flatten)]`s `$inner`. The flatten is
/// byte-identical to the bare `$inner` except for the inserted `schema_version`
/// line, so existing consumers keep their exact field bytes.
macro_rules! flatten_envelope {
    ($(#[$meta:meta])* $name:ident, $inner:ty) => {
        $(#[$meta])*
        #[derive(Debug, Serialize, JsonSchema)]
        pub(crate) struct $name {
            pub(crate) schema_version: u32,
            #[serde(flatten)]
            pub(crate) inner: $inner,
        }

        impl $name {
            /// Output-contract version. Bump only on a removal/rename (additive
            /// fields do not bump); see `docs/src/json-output-stability.md`.
            pub(crate) const SCHEMA_VERSION: u32 = 1;

            pub(crate) fn new(inner: $inner) -> Self {
                Self {
                    schema_version: Self::SCHEMA_VERSION,
                    inner,
                }
            }
        }
    };
}

flatten_envelope!(
    /// `score` post-run efficiency report (`--format json`).
    ScoreOutput,
    EfficiencyScoreReport
);
flatten_envelope!(
    /// `jobs list` / `ls` tracked-job inventory (`--format json`).
    JobListOutput,
    JobInventoryScan
);
flatten_envelope!(
    /// `status` combined tracked-job status (`--format json`).
    StatusOutput,
    StatusSnapshot
);
flatten_envelope!(
    /// `ps` per-service snapshot (`--format json`).
    PsOutput,
    PsSnapshot
);
flatten_envelope!(
    /// `stats` metrics and scheduler view (`--format json`).
    StatsOutput,
    StatsSnapshot
);
flatten_envelope!(
    /// `artifacts` export report (`--format json`).
    ArtifactsOutput,
    ArtifactExportReport
);
flatten_envelope!(
    /// `diff` two-run comparison (`--format json`).
    DiffOutput,
    JobDiffReport
);
flatten_envelope!(
    /// `diff --matrix` N-way comparison (`--matrix-format json`).
    DiffMatrixOutput,
    JobMatrixReport
);
flatten_envelope!(
    /// `diff --against-spec` current-spec-vs-snapshot comparison (`--format json`).
    DiffSpecOutput,
    SpecDiffReport
);
flatten_envelope!(
    /// `replay` reconstructed run timeline (`--format json`).
    ReplayOutput,
    ReplayReport
);
flatten_envelope!(
    /// `clean` / `gc` tracked-job cleanup report (`--format json`).
    CleanOutput,
    CleanupReport
);
flatten_envelope!(
    /// `checkpoints` attempt/requeue history (`--format json`).
    CheckpointsOutput,
    CheckpointHistory
);
flatten_envelope!(
    /// `rendezvous prune` removal report (`--format json`).
    RendezvousPruneOutput,
    RendezvousPruneReport
);
flatten_envelope!(
    /// `spec prepare` image-preparation summary (`--format json`).
    PrepareOutput,
    PrepareSummary
);
flatten_envelope!(
    /// `spec preflight` grouped readiness report (`--format json`).
    PreflightOutput,
    GroupedReport
);
flatten_envelope!(
    /// `doctor` grouped environment diagnostics (`--format json`).
    DoctorOutput,
    GroupedReport
);
flatten_envelope!(
    /// `spec inspect --rightsize` right-sizing report (`--format json`).
    RightsizeOutput,
    RightsizeReport
);
flatten_envelope!(
    /// `evolve` optimization run report (`--format json`).
    EvolveOutput,
    EvolveRunReport
);
flatten_envelope!(
    /// `weather` cluster weather report (`--format json`).
    WeatherOutput,
    WeatherReport
);

/// The registry of `--format json` output schemas. Each entry maps a schema stem
/// (the CLI command path joined by `-`) to the type whose JSON Schema is checked
/// in and served by the `schema` subcommand. Envelope commands map to their DTO;
/// the bare-payload commands map to their raw serialized type so the schema is
/// still published and pinned. Adding a `--format json` command without
/// registering it here fails [`tests::registry_covers_known_commands`].
macro_rules! output_schemas {
    ($($command:literal => $ty:ty),+ $(,)?) => {
        /// Schema stems that have a registered output schema.
        pub(crate) fn output_schema_commands() -> Vec<&'static str> {
            vec![$($command),+]
        }

        /// Returns the JSON Schema for one command's `--format json` output, or
        /// `None` when the command has no registered output schema.
        pub(crate) fn output_schema_json(command: &str) -> Option<String> {
            match command {
                $($command => Some(schema_string::<$ty>()),)+
                _ => None,
            }
        }
    };
}

output_schemas! {
    "score" => ScoreOutput,
    "jobs-list" => JobListOutput,
    "status" => StatusOutput,
    "ps" => PsOutput,
    "stats" => StatsOutput,
    "artifacts" => ArtifactsOutput,
    "diff" => DiffOutput,
    "diff-matrix" => DiffMatrixOutput,
    "diff-spec" => DiffSpecOutput,
    "replay" => ReplayOutput,
    "clean" => CleanOutput,
    "checkpoints" => CheckpointsOutput,
    "rendezvous-prune" => RendezvousPruneOutput,
    "prepare" => PrepareOutput,
    "preflight" => PreflightOutput,
    "doctor" => DoctorOutput,
    "rightsize" => RightsizeOutput,
    "evolve" => EvolveOutput,
    "weather" => WeatherOutput,
    // Owned DTOs with their own `schema_version` field (registered directly).
    "validate" => ValidateOutput,
    "render" => RenderOutput,
    "cache-inspect" => CacheInspectReport,
    "cache-prune" => CachePruneReport,
    "up" => SubmitOutput,
    "cancel" => CancelOutput,
    "dependencies" => DependencyGraphOutput,
    "setup" => SetupOutput,
    "init-describe" => TemplateDescriptionOutput,
    "init-write" => TemplateWriteOutput,
    // Bare-payload commands: output stays byte-unchanged; schema still published.
    "metrics-probe" => MetricsProbeReport,
    "rendezvous-resolve" => RendezvousRecord,
    "rendezvous-list" => Vec<RendezvousRecord>,
    "cache-list" => Vec<CacheEntryManifest>,
    // Effective-config outputs: printed as a (redacted) value of these types.
    "spec-config" => EffectiveComposeConfig,
    "spec-inspect" => RuntimePlan,
    // Command-local DTOs (each carries its own `schema_version` field).
    "sweep-submit" => crate::commands::runtime::sweep::SweepSubmitOutput<'static>,
    "sweep-status" => crate::commands::runtime::sweep::SweepStatusOutput,
    "sweep-list" => crate::commands::runtime::sweep::SweepListOutput,
    "sweep-observe" => crate::commands::runtime::sweep::SweepObserveOutput,
    "sweep-stop" => crate::commands::runtime::sweep::SweepStopOutput,
    "sweep-results" => crate::commands::runtime::sweep::SweepResultsOutput,
    "sweep-score" => crate::commands::runtime::sweep::SweepScoreOutput,
    "sweep-stats" => crate::commands::runtime::sweep::SweepStatsOutput,
    "doctor-mpi-smoke" => crate::commands::doctor::MpiSmokeJsonOutput,
    "doctor-fabric-smoke" => crate::commands::doctor::FabricSmokeJsonOutput,
    "doctor-readiness" => crate::commands::doctor::ReadinessDoctorOutput,
    "doctor-cluster-report" => crate::commands::doctor::ClusterReportJsonOutput<'static>,
    "docs" => DocsSearchOutput,
    "feedback" => crate::commands::feedback::FeedbackOutput,
    "diagnostic-notice" => Notice,
    "experiment" => crate::commands::runtime::experiment::ExperimentShowOutput,
    "experiment-bundle" => hpc_compose::job::ExperimentBundleManifest,
    "experiment-tag" => crate::commands::runtime::experiment::ExperimentTagOutput,
    "experiment-note" => crate::commands::runtime::experiment::ExperimentNoteOutput,
    "germinate" => crate::commands::runtime::germinate::GerminateOutput<'static>,
    "when" => crate::commands::runtime::WhenSubmitOutput<'static>,
    "test" => crate::commands::runtime::SmokeTestOutput,
    "pull" => crate::commands::runtime::pull::PullOutput,
    "reach" => crate::commands::runtime::reach::ReachOutput,
    "notebook-dry-run" => crate::commands::runtime::exec::NotebookDryRunOutput,
    "notebook" => crate::commands::runtime::notebook::NotebookConnectionOutput,
    "debug" => crate::commands::runtime::debug::DebugReport,
    "rendezvous-register" => crate::commands::runtime::rendezvous_cmd::RendezvousRegisterOutput,
    "explain" => crate::commands::spec::ExplainOutput,
    "lint" => crate::commands::spec::LintOutput,
    "plan" => crate::commands::spec::PlanOutput,
    "context" => crate::commands::spec::ContextOutput,
    "workspace-status" => crate::commands::workspace::WorkspaceStatusOutput,
    "workspace-allocate" => crate::commands::workspace::WorkspaceAllocateOutput,
    "workspace-extend" => crate::commands::workspace::WorkspaceExtendOutput,
    "workspace-release" => crate::commands::workspace::WorkspaceReleaseOutput,
    "lessons-list" => crate::commands::evolve::LessonListOutput,
    "lessons-describe" => crate::commands::evolve::LessonDescriptionOutput,
    "examples-list" => crate::commands::examples::ExamplesListOutput<'static>,
    "examples-recommend" => crate::commands::examples::ExamplesRecommendOutput<'static>,
    "vars" => InterpolationVarsOutput,
    "init-list" => TemplateListOutput,
}

/// Pretty-prints the JSON Schema for `T` with a trailing newline. The same
/// helper backs both the checked-in files and the `schema --output` subcommand,
/// so the two are identical by construction.
fn schema_string<T: JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    let mut json = serde_json::to_string_pretty(&schema).expect("json schema serializes");
    json.push('\n');
    json
}

// The generate/check machinery is exercised only by the drift and bless tests
// (and, transitively, `cargo test`); it has no runtime caller, so it is gated to
// test builds to stay clear of `clippy -D warnings`.
#[cfg(test)]
mod contract_gen {
    use std::collections::BTreeSet;
    use std::ffi::OsStr;
    use std::fs;
    use std::path::Path;

    use anyhow::{Context, Result, bail};

    use super::{output_schema_commands, output_schema_json};

    /// Repo-relative directory holding the checked-in output schemas.
    pub(super) const DEFAULT_OUTPUT_SCHEMA_DIR: &str = "schema/outputs";

    /// One command's checked-in output schema.
    pub(super) struct RenderedOutputSchema {
        pub(super) file_name: String,
        pub(super) contents: String,
    }

    /// Renders the checked-in JSON Schema for every registered output.
    pub(super) fn render_output_schemas() -> Vec<RenderedOutputSchema> {
        output_schema_commands()
            .into_iter()
            .map(|command| RenderedOutputSchema {
                file_name: format!("{command}.schema.json"),
                contents: output_schema_json(command).expect("registered command has a schema"),
            })
            .collect()
    }

    fn is_schema_file(path: &Path) -> bool {
        path.file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.ends_with(".schema.json"))
    }

    /// Writes every registered output schema into `dir`, creating it if needed
    /// and removing stale `*.schema.json` files. Mirrors
    /// `manpages::write_manpages`.
    pub(super) fn write_output_schemas(dir: &Path) -> Result<()> {
        fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;

        let schemas = render_output_schemas();
        let expected: BTreeSet<_> = schemas.iter().map(|s| s.file_name.as_str()).collect();
        for entry in
            fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry under {}", dir.display()))?;
            let path = entry.path();
            if is_schema_file(&path)
                && let Some(name) = path.file_name().and_then(OsStr::to_str)
                && !expected.contains(name)
            {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove stale schema {}", path.display()))?;
            }
        }

        for schema in schemas {
            let path = dir.join(&schema.file_name);
            fs::write(&path, schema.contents)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        Ok(())
    }

    /// Fails when the checked-in schemas under `dir` do not match freshly
    /// generated output. Mirrors `manpages::check_manpages`.
    pub(super) fn check_output_schemas(dir: &Path) -> Result<()> {
        let schemas = render_output_schemas();
        let mut stale = Vec::new();
        let mut missing = Vec::new();

        for schema in &schemas {
            let path = dir.join(&schema.file_name);
            match fs::read_to_string(&path) {
                Ok(existing) if existing == schema.contents => {}
                Ok(_) => stale.push(schema.file_name.clone()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(schema.file_name.clone())
                }
                Err(err) => {
                    return Err(err).with_context(|| format!("failed to read {}", path.display()));
                }
            }
        }

        let expected: BTreeSet<_> = schemas.iter().map(|s| s.file_name.as_str()).collect();
        let mut unexpected = Vec::new();
        if dir.exists() {
            for entry in
                fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
            {
                let entry = entry
                    .with_context(|| format!("failed to read entry under {}", dir.display()))?;
                let path = entry.path();
                if is_schema_file(&path)
                    && let Some(name) = path.file_name().and_then(OsStr::to_str)
                    && !expected.contains(name)
                {
                    unexpected.push(name.to_string());
                }
            }
        }

        if stale.is_empty() && missing.is_empty() && unexpected.is_empty() {
            return Ok(());
        }

        let mut message = String::from(
            "output schemas are out of date; run `cargo test bless_output_schemas -- --ignored`",
        );
        if !missing.is_empty() {
            message.push_str(&format!("\nmissing: {}", missing.join(", ")));
        }
        if !stale.is_empty() {
            message.push_str(&format!("\nstale: {}", stale.join(", ")));
        }
        if !unexpected.is_empty() {
            message.push_str(&format!("\nunexpected: {}", unexpected.join(", ")));
        }
        bail!("{message}");
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use hpc_compose::job::EfficiencyScoreConfidence;

    use super::contract_gen::{
        DEFAULT_OUTPUT_SCHEMA_DIR, check_output_schemas, write_output_schemas,
    };
    use super::*;

    fn repo_schema_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_OUTPUT_SCHEMA_DIR)
    }

    fn sample_score_report() -> EfficiencyScoreReport {
        EfficiencyScoreReport {
            job_id: "12345".to_string(),
            scheduler_state: "COMPLETED".to_string(),
            scheduler_source: "sacct".to_string(),
            complete: true,
            score: 72,
            grade: "B".to_string(),
            components: Vec::new(),
            energy_kwh: Some(1.5),
            energy_basis: "measured".to_string(),
            confidence: EfficiencyScoreConfidence::High,
            tips: vec!["raise ntasks".to_string()],
            sources: vec!["sacct".to_string()],
            notes: Vec::new(),
        }
    }

    /// Representative guard for the flatten-envelope mechanism (every envelope
    /// uses the identical macro): the wrapper must be byte-identical to the bare
    /// report except for a single inserted `schema_version` line. If serde ever
    /// reorders flattened fields this fails loudly.
    #[test]
    fn score_output_only_prepends_schema_version() {
        let report = sample_score_report();
        let raw = serde_json::to_string_pretty(&report).expect("raw");
        let wrapped =
            serde_json::to_string_pretty(&ScoreOutput::new(report.clone())).expect("wrapped");
        let expected = raw.replacen("{\n", "{\n  \"schema_version\": 1,\n", 1);
        assert_eq!(wrapped, expected);
    }

    #[test]
    fn checked_in_output_schemas_are_current() {
        check_output_schemas(&repo_schema_dir())
            .expect("checked-in output schemas match generated");
    }

    /// Regenerates the checked-in schemas into the source tree. Ignored by
    /// default; run explicitly to re-bless after an intended contract change.
    #[test]
    #[ignore = "writes generated schemas into the source tree"]
    fn bless_output_schemas() {
        write_output_schemas(&repo_schema_dir()).expect("write output schemas");
    }

    /// Coverage guard: every registered command resolves a schema, and the set of
    /// registered commands matches the expected list (so adding or dropping a
    /// registration is a deliberate edit here).
    #[test]
    fn registry_covers_known_commands() {
        let mut commands = output_schema_commands();
        commands.sort_unstable();
        let mut expected = vec![
            "artifacts",
            "cache-inspect",
            "cache-list",
            "cache-prune",
            "cancel",
            "checkpoints",
            "clean",
            "dependencies",
            "diagnostic-notice",
            "diff",
            "diff-matrix",
            "diff-spec",
            "doctor",
            "evolve",
            "init-describe",
            "init-write",
            "jobs-list",
            "metrics-probe",
            "prepare",
            "preflight",
            "ps",
            "rendezvous-list",
            "rendezvous-prune",
            "rendezvous-resolve",
            "render",
            "replay",
            "rightsize",
            "score",
            "setup",
            "spec-config",
            "spec-inspect",
            "stats",
            "status",
            "up",
            "validate",
            "weather",
            "context",
            "debug",
            "doctor-cluster-report",
            "doctor-fabric-smoke",
            "doctor-mpi-smoke",
            "doctor-readiness",
            "docs",
            "examples-list",
            "examples-recommend",
            "experiment",
            "experiment-bundle",
            "experiment-note",
            "experiment-tag",
            "explain",
            "feedback",
            "germinate",
            "init-list",
            "lessons-describe",
            "lessons-list",
            "lint",
            "notebook",
            "notebook-dry-run",
            "plan",
            "pull",
            "reach",
            "rendezvous-register",
            "sweep-list",
            "sweep-observe",
            "sweep-results",
            "sweep-score",
            "sweep-stats",
            "sweep-status",
            "sweep-stop",
            "sweep-submit",
            "test",
            "vars",
            "when",
            "workspace-allocate",
            "workspace-extend",
            "workspace-release",
            "workspace-status",
        ];
        expected.sort_unstable();
        assert_eq!(commands, expected);
        for command in &commands {
            assert!(
                output_schema_json(command).is_some(),
                "no schema registered for {command}"
            );
        }
        assert!(output_schema_json("definitely-not-a-command").is_none());
    }
}
