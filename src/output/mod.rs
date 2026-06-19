use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use hpc_compose::cache::{CacheEntryKind, load_manifest_if_exists};
use hpc_compose::cli::{OutputFormat, StatsOutputFormat};
use hpc_compose::cluster::ClusterProfile;
use hpc_compose::context::ResourceProfile;
use hpc_compose::init::{
    cache_dir_placeholder as init_cache_dir_placeholder, resolve_template, template_category,
    templates,
};
use hpc_compose::job::{
    ArtifactExportReport, CleanupReport, EfficiencyScoreReport, JobDiffChange, JobDiffReport,
    JobInventoryScan, PsSnapshot, RightsizeConfidence, RightsizeReport, StatsSnapshot,
    StatusSnapshot, SubmissionBackend, WatchOutcome, scheduler_source_label,
};
use hpc_compose::planner::{
    ExecutionSpec, ImageSource, Plan, ServicePlacementMode, registry_host_for_remote,
};
use hpc_compose::planner::{PlanOptions, build_plan_with_options};
use hpc_compose::preflight::Report;
use hpc_compose::prepare::{
    ArtifactAction, PrepareSummary, RuntimePlan, RuntimeService, base_image_path_for_backend,
    build_runtime_plan,
};
use hpc_compose::render::{
    display_srun_command_for_backend, distributed_environment_names_for_service, execution_argv,
    log_file_name_for_service,
};
use hpc_compose::spec::{
    ComposeSpec, DependencyCondition, EffectiveComposeConfig, ServiceDependency,
    parse_slurm_time_limit,
};
use hpc_compose::term;
use serde::Serialize;

pub(crate) mod cache;
pub(crate) mod common;
pub(crate) mod init;
pub(crate) mod runtime;
pub(crate) mod spec;

#[derive(Debug, Serialize)]
pub(crate) struct ValidateOutput {
    pub(crate) valid: bool,
    pub(crate) compose_file: PathBuf,
    pub(crate) name: String,
    pub(crate) service_count: usize,
    pub(crate) services: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) cluster_warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RenderOutput {
    pub(crate) compose_file: PathBuf,
    pub(crate) output_path: Option<PathBuf>,
    pub(crate) script: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CacheArtifactInspect {
    pub(crate) path: PathBuf,
    pub(crate) artifact_present: bool,
    pub(crate) manifest_path: PathBuf,
    pub(crate) manifest: Option<hpc_compose::cache::CacheEntryManifest>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CacheInspectService {
    pub(crate) service_name: String,
    pub(crate) source_image: String,
    pub(crate) base_registry: Option<String>,
    pub(crate) base_artifact: Option<CacheArtifactInspect>,
    pub(crate) runtime_artifact: CacheArtifactInspect,
    pub(crate) current_reuse_expectation: String,
    pub(crate) note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CacheInspectReport {
    pub(crate) cache_dir: PathBuf,
    pub(crate) services: Vec<CacheInspectService>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CachePruneReport {
    pub(crate) cache_dir: PathBuf,
    pub(crate) mode: String,
    pub(crate) removed_count: usize,
    pub(crate) removed_paths: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SubmitOutput {
    pub(crate) backend: SubmissionBackend,
    pub(crate) compose_file: PathBuf,
    pub(crate) script_path: PathBuf,
    pub(crate) cache_dir: PathBuf,
    pub(crate) dry_run: bool,
    pub(crate) launched: bool,
    pub(crate) submitted: bool,
    pub(crate) sbatch_stdout: Option<String>,
    pub(crate) job_id: Option<String>,
    pub(crate) tracking_persisted: bool,
    pub(crate) tracked_metadata_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DependencyGraphOutput {
    pub(crate) nodes: Vec<DependencyGraphNode>,
    pub(crate) edges: Vec<DependencyGraphEdge>,
    pub(crate) roots: Vec<String>,
    pub(crate) leaves: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DependencyGraphNode {
    pub(crate) service: String,
    pub(crate) readiness: String,
    pub(crate) readiness_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DependencyGraphEdge {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) condition: String,
    pub(crate) readiness: String,
    pub(crate) readiness_type: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CancelOutput {
    pub(crate) job_id: String,
    pub(crate) cancelled: bool,
    pub(crate) command_stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tracking_removed: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) purged_cache_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TemplateInfoOutput {
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) description: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct TemplateDescriptionOutput {
    pub(crate) template: TemplateInfoOutput,
    pub(crate) cache_dir_required: bool,
    pub(crate) cache_dir_placeholder: String,
    pub(crate) command: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct TemplateWriteOutput {
    pub(crate) template_name: String,
    pub(crate) app_name: String,
    pub(crate) cache_dir: Option<String>,
    pub(crate) output_path: PathBuf,
    pub(crate) next_commands: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SetupOutput {
    pub(crate) settings_path: PathBuf,
    pub(crate) profile: String,
    pub(crate) default_profile: String,
    pub(crate) compose_file: String,
    pub(crate) env_files: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) binaries: hpc_compose::context::BinaryOverrides,
    pub(crate) cache_dir: Option<String>,
}

#[cfg(test)]
pub(crate) fn render_from_path(path: &Path) -> Result<String> {
    let runtime = load_runtime_plan(path)?;
    hpc_compose::render::render_script(&runtime)
}

pub(crate) fn resolve_output_format(format: Option<OutputFormat>, json: bool) -> OutputFormat {
    if json {
        OutputFormat::Json
    } else {
        format.unwrap_or(OutputFormat::Text)
    }
}

pub(crate) fn resolve_stats_output_format(
    format: Option<StatsOutputFormat>,
    json: bool,
) -> StatsOutputFormat {
    if json {
        StatsOutputFormat::Json
    } else {
        format.unwrap_or(StatsOutputFormat::Text)
    }
}

pub(crate) fn build_validate_output(plan: &Plan, cluster_warnings: Vec<String>) -> ValidateOutput {
    ValidateOutput {
        valid: true,
        compose_file: plan.spec_path.clone(),
        name: plan.name.clone(),
        service_count: plan.ordered_services.len(),
        services: plan
            .ordered_services
            .iter()
            .map(|service| service.name.clone())
            .collect(),
        cluster_warnings,
    }
}

#[cfg(test)]
pub(crate) fn load_plan(path: &Path) -> Result<Plan> {
    let spec = ComposeSpec::load(path)?;
    hpc_compose::planner::build_plan(path, spec)
}

#[allow(dead_code)]
pub(crate) fn load_plan_with_interpolation_vars_and_cache_default(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
) -> Result<Plan> {
    load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        path,
        vars,
        cache_dir_default,
        &BTreeMap::new(),
    )
}

pub(crate) fn load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
    resource_profiles: &BTreeMap<String, ResourceProfile>,
) -> Result<Plan> {
    let spec = ComposeSpec::load_with_interpolation_vars(path, vars)?;
    build_plan_with_options(
        path,
        spec,
        PlanOptions {
            cache_dir_default: cache_dir_default.map(Path::to_path_buf),
            resource_profiles: resource_profiles.clone(),
            ..PlanOptions::default()
        },
    )
}

#[cfg(test)]
pub(crate) fn load_runtime_plan(path: &Path) -> Result<RuntimePlan> {
    let plan = load_plan(path)?;
    Ok(build_runtime_plan(&plan))
}

#[allow(dead_code)]
pub(crate) fn load_runtime_plan_with_interpolation_vars_and_cache_default(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
) -> Result<RuntimePlan> {
    let plan = load_plan_with_interpolation_vars_and_cache_default(path, vars, cache_dir_default)?;
    Ok(build_runtime_plan(&plan))
}

pub(crate) fn load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
    resource_profiles: &BTreeMap<String, ResourceProfile>,
) -> Result<RuntimePlan> {
    let plan = load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        path,
        vars,
        cache_dir_default,
        resource_profiles,
    )?;
    Ok(build_runtime_plan(&plan))
}

#[allow(dead_code)]
pub(crate) fn load_effective_config_with_interpolation_vars_and_cache_default(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
) -> Result<EffectiveComposeConfig> {
    load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
        path,
        vars,
        cache_dir_default,
        &BTreeMap::new(),
    )
}

pub(crate) fn load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
    resource_profiles: &BTreeMap<String, ResourceProfile>,
) -> Result<EffectiveComposeConfig> {
    let mut spec = ComposeSpec::load_with_interpolation_vars(path, vars)?;
    let plan = build_plan_with_options(
        path,
        spec.clone(),
        PlanOptions {
            cache_dir_default: cache_dir_default.map(Path::to_path_buf),
            resource_profiles: resource_profiles.clone(),
            ..PlanOptions::default()
        },
    )?;
    spec.slurm = plan.slurm.clone();
    let normalized_policies = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.clone(), service.failure_policy.clone()))
        .collect::<BTreeMap<_, _>>();
    spec.effective_config(&plan.cache_dir, &normalized_policies)
}

/// Serializes the effective config as YAML for the persisted job-state
/// snapshot (and `diff` comparisons), redacting resolved secret values first.
///
/// The snapshot is written to `.hpc-compose/` on a shared filesystem, so it
/// must not carry cleartext secrets — `config`/`context`/`inspect` already
/// redact the same struct on display, and this keeps the at-rest copy
/// consistent. Pass the secret value set from
/// [`crate::redaction::secret_value_set`] so values referenced under benign
/// env names are caught in addition to name-based redaction.
pub(crate) fn effective_config_yaml(
    config: &EffectiveComposeConfig,
    secret_values: &std::collections::BTreeSet<String>,
) -> Result<String> {
    let value = crate::redaction::redacted_yaml_value(config, secret_values, false)
        .context("failed to redact effective config for snapshot")?;
    serde_norway::to_string(&value).context("failed to serialize effective config as yaml")
}

#[allow(dead_code)]
pub(crate) fn load_plan_and_runtime_with_interpolation_vars_and_cache_default(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
) -> Result<(Plan, RuntimePlan)> {
    let plan = load_plan_with_interpolation_vars_and_cache_default(path, vars, cache_dir_default)?;
    let runtime_plan = build_runtime_plan(&plan);
    Ok((plan, runtime_plan))
}

pub(crate) fn load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
    path: &Path,
    vars: &BTreeMap<String, String>,
    cache_dir_default: Option<&Path>,
    resource_profiles: &BTreeMap<String, ResourceProfile>,
) -> Result<(Plan, RuntimePlan)> {
    let plan = load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        path,
        vars,
        cache_dir_default,
        resource_profiles,
    )?;
    let runtime_plan = build_runtime_plan(&plan);
    Ok((plan, runtime_plan))
}

pub(crate) fn default_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.sbatch")
}

pub(crate) fn default_local_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.local.sh")
}

pub(crate) fn default_cache_dir() -> PathBuf {
    let home = match env::var_os("HOME") {
        Some(home) => PathBuf::from(home),
        None => PathBuf::from("."),
    };
    home.join(".cache/hpc-compose")
}

pub(crate) fn print_report(report: &Report, verbose: bool) {
    if report.items.is_empty() {
        return;
    }
    let text = if verbose {
        report.render_verbose()
    } else {
        report.render()
    };
    let _ = writeln!(io::stderr(), "{text}");
    let _ = io::stderr().flush();
}

pub(crate) fn print_prepare_summary(summary: &PrepareSummary) {
    for service in &summary.services {
        if let Some(base) = &service.base_image {
            println!(
                "[service {} {}] base image {}",
                term::styled_bold(&service.service_name),
                styled_action_label(base.action),
                term::styled_dim(&base.path.display().to_string())
            );
        }
        println!(
            "[service {} {}] {}",
            term::styled_bold(&service.service_name),
            styled_action_label(service.runtime_image.action),
            term::styled_dim(&service.runtime_image.path.display().to_string())
        );
        if let Some(note) = &service.runtime_image.note {
            println!(
                "  {} service '{}': {note}",
                term::styled_note("note"),
                service.service_name
            );
        }
    }
}

fn styled_action_label(action: ArtifactAction) -> String {
    match action {
        ArtifactAction::Present => term::styled_action_ok(),
        ArtifactAction::Reused => term::styled_action_reuse(),
        ArtifactAction::Built => term::styled_action_build(),
    }
}

#[cfg(test)]
fn action_label(action: ArtifactAction) -> &'static str {
    match action {
        ArtifactAction::Present => "OK",
        ArtifactAction::Reused => "REUSE",
        ArtifactAction::Built => "BUILD",
    }
}

#[cfg(test)]
fn artifact_role_label(name: &str) -> &'static str {
    match name {
        "base" => "cache artifact",
        "runtime" => "artifact",
        _ => "artifact",
    }
}

pub(crate) fn print_status_snapshot(snapshot: &StatusSnapshot) -> io::Result<()> {
    write_status_snapshot(&mut io::stdout(), snapshot)
}

pub(crate) fn print_ps_snapshot(snapshot: &PsSnapshot) -> io::Result<()> {
    write_ps_snapshot(&mut io::stdout(), snapshot)
}

pub(crate) fn print_stats_snapshot(snapshot: &StatsSnapshot) -> io::Result<()> {
    write_stats_snapshot(&mut io::stdout(), snapshot)
}

pub(crate) fn print_rightsize_report(report: &RightsizeReport) -> io::Result<()> {
    write_rightsize_report(&mut io::stdout(), report)
}

pub(crate) fn print_efficiency_score_report(report: &EfficiencyScoreReport) -> io::Result<()> {
    write_efficiency_score_report(&mut io::stdout(), report)
}

pub(crate) fn print_artifact_export_report(report: &ArtifactExportReport) -> io::Result<()> {
    write_artifact_export_report(&mut io::stdout(), report)
}

pub(crate) fn print_plan_inspect_verbose(
    plan: &Plan,
    runtime_plan: &RuntimePlan,
) -> io::Result<()> {
    write_plan_inspect_verbose(&mut io::stdout(), plan, runtime_plan, None)
}

pub(crate) fn print_plan_inspect_verbose_with_profile(
    plan: &Plan,
    runtime_plan: &RuntimePlan,
    cluster_profile: Option<&ClusterProfile>,
) -> io::Result<()> {
    write_plan_inspect_verbose(&mut io::stdout(), plan, runtime_plan, cluster_profile)
}

pub(crate) fn print_plan_inspect(plan: &RuntimePlan) -> io::Result<()> {
    write_plan_inspect(&mut io::stdout(), plan)
}

pub(crate) fn print_cache_inspect(report: &CacheInspectReport) -> Result<()> {
    write_cache_inspect(&mut io::stdout(), report)
}

pub(crate) fn print_job_inventory_scan(
    report: &JobInventoryScan,
    disk_usage: bool,
) -> io::Result<()> {
    write_job_inventory_scan(&mut io::stdout(), report, disk_usage)
}

pub(crate) fn print_cleanup_report(report: &CleanupReport, disk_usage: bool) -> io::Result<()> {
    write_cleanup_report(&mut io::stdout(), report, disk_usage)
}

pub(crate) fn print_job_diff_report(report: &JobDiffReport) -> io::Result<()> {
    write_job_diff_report(&mut io::stdout(), report)
}

fn write_job_inventory_scan(
    writer: &mut impl Write,
    report: &JobInventoryScan,
    disk_usage: bool,
) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_label("scan root", &report.scan_root.display().to_string())
    )?;
    if report.jobs.is_empty() {
        writeln!(writer, "no tracked jobs found")?;
        return Ok(());
    }

    for job in &report.jobs {
        let latest_marker = if job.is_latest {
            term::styled_success("*")
        } else {
            "-".to_string()
        };
        write!(
            writer,
            "{} {} kind={} compose={} {}={} {}={} {}={}",
            latest_marker,
            job.job_id,
            match job.kind {
                hpc_compose::job::SubmissionKind::Main => "main",
                hpc_compose::job::SubmissionKind::Run => "run",
                hpc_compose::job::SubmissionKind::Canary => "canary",
                hpc_compose::job::SubmissionKind::SweepTrial => "sweep_trial",
                hpc_compose::job::SubmissionKind::Notebook => "notebook",
            },
            term::styled_dim(&job.compose_file.display().to_string()),
            term::styled_bold("age"),
            format_age_seconds(job.age_seconds),
            term::styled_bold("submit_dir"),
            term::styled_dim(&job.submit_dir.display().to_string()),
            term::styled_bold("runtime"),
            runtime_presence_label(
                job.runtime_job_root_present,
                job.legacy_runtime_job_root_present,
            )
        )?;
        if disk_usage {
            write!(
                writer,
                " size={}",
                format_bytes(job.disk_usage_bytes.unwrap_or(0))
            )?;
        }
        writeln!(writer)?;
    }
    Ok(())
}

fn write_cleanup_report(
    writer: &mut impl Write,
    report: &CleanupReport,
    disk_usage: bool,
) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_label("compose file", &report.compose_file.display().to_string())
    )?;
    writeln!(writer, "{}", term::styled_label("mode", &report.mode))?;
    writeln!(
        writer,
        "{}",
        term::styled_label("dry run", yes_no(report.dry_run))
    )?;
    writeln!(
        writer,
        "effective latest before: {}",
        report.latest_job_id_before.as_deref().unwrap_or("<none>")
    )?;
    if let Some(job_id) = report.latest_pointer_job_id_before.as_deref() {
        writeln!(writer, "pointer before: {job_id}")?;
    }
    writeln!(
        writer,
        "effective latest after: {}",
        report.latest_job_id_after.as_deref().unwrap_or("<none>")
    )?;
    writeln!(writer, "selected jobs: {}", report.removed_job_ids.len())?;
    if !report.removed_job_ids.is_empty() {
        writeln!(writer, "selected ids: {}", report.removed_job_ids.join(","))?;
    }
    if !report.kept_job_ids.is_empty() {
        writeln!(writer, "kept ids: {}", report.kept_job_ids.join(","))?;
    }
    if disk_usage {
        writeln!(
            writer,
            "total bytes reclaimed: {}",
            format_bytes(report.total_bytes_reclaimed.unwrap_or(0))
        )?;
    }
    if report.removed_job_ids.is_empty() {
        writeln!(writer, "no tracked jobs matched cleanup criteria")?;
        return Ok(());
    }

    let action = if report.dry_run {
        "would remove"
    } else {
        "removed"
    };
    for job in report.jobs.iter().filter(|job| job.selected) {
        write!(
            writer,
            "{} {} submit_dir={} runtime={}",
            action,
            job.inventory.job_id,
            job.inventory.submit_dir.display(),
            runtime_presence_label(
                job.inventory.runtime_job_root_present,
                job.inventory.legacy_runtime_job_root_present,
            )
        )?;
        if disk_usage {
            write!(
                writer,
                " size={}",
                format_bytes(job.bytes_reclaimed.unwrap_or(0))
            )?;
        }
        writeln!(writer)?;
    }
    Ok(())
}

fn write_job_diff_report(writer: &mut impl Write, report: &JobDiffReport) -> io::Result<()> {
    writeln!(
        writer,
        "{} -> {}",
        term::styled_bold(&report.left.job_id),
        term::styled_bold(&report.right.job_id)
    )?;
    writeln!(writer, "{}", term::styled_section_header("Outcome:"))?;
    if report.outcome_changes.is_empty() {
        writeln!(writer, "  no outcome changes")?;
    } else {
        write_diff_changes(writer, &report.outcome_changes, usize::MAX)?;
    }
    writeln!(writer, "{}", term::styled_section_header("Resources:"))?;
    if report.resource_changes.is_empty() {
        writeln!(writer, "  no resource changes")?;
    } else {
        write_diff_changes(writer, &report.resource_changes, usize::MAX)?;
    }
    writeln!(writer, "{}", term::styled_section_header("Config:"))?;
    if report.config_changes.is_empty() {
        writeln!(writer, "  no config changes")?;
    } else {
        write_diff_changes(writer, &report.config_changes, 25)?;
        if report.config_changes.len() > 25 {
            writeln!(
                writer,
                "  ... {} more config changes; use --format json for full detail",
                report.config_changes.len() - 25
            )?;
        }
    }
    for note in &report.notes {
        writeln!(writer, "note: {note}")?;
    }
    Ok(())
}

fn write_diff_changes(
    writer: &mut impl Write,
    changes: &[JobDiffChange],
    limit: usize,
) -> io::Result<()> {
    for change in changes.iter().take(limit) {
        writeln!(
            writer,
            "  {}: {} -> {}",
            change.path,
            change.left.as_deref().unwrap_or("<missing>"),
            change.right.as_deref().unwrap_or("<missing>")
        )?;
    }
    Ok(())
}

fn write_status_snapshot(writer: &mut impl Write, snapshot: &StatusSnapshot) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_label("job id", &snapshot.record.job_id)
    )?;
    writeln!(writer, "{}", term::styled_section_header("Scheduler:"))?;
    writeln!(
        writer,
        "  {}: {} ({})",
        term::styled_bold("state"),
        term::styled_scheduler_state(&snapshot.scheduler.state),
        scheduler_source_label(snapshot.scheduler.source)
    )?;
    if let Some(detail) = &snapshot.scheduler.detail {
        writeln!(writer, "  {}: {detail}", term::styled_bold("note"))?;
    }
    if let Some(queue) = &snapshot.queue_diagnostics {
        if let Some(reason) = &queue.pending_reason {
            writeln!(
                writer,
                "  {}: {reason}",
                term::styled_bold("pending reason")
            )?;
        }
        if let Some(eligible_time) = &queue.eligible_time {
            writeln!(
                writer,
                "  {}: {eligible_time}",
                term::styled_bold("eligible time")
            )?;
        }
        if let Some(start_time) = &queue.start_time {
            writeln!(
                writer,
                "  {}: {start_time}",
                term::styled_bold("start time")
            )?;
        }
    }
    writeln!(writer, "{}", term::styled_section_header("Runtime:"))?;
    writeln!(
        writer,
        "  {}",
        term::styled_label(
            "compose file",
            &snapshot.record.compose_file.display().to_string()
        )
    )?;
    writeln!(
        writer,
        "  {}",
        term::styled_label(
            "script path",
            &snapshot.record.script_path.display().to_string()
        )
    )?;
    writeln!(
        writer,
        "  {}",
        term::styled_label(
            "cache dir",
            &snapshot.record.cache_dir.display().to_string()
        )
    )?;
    writeln!(
        writer,
        "  {}",
        term::styled_label("log dir", &snapshot.log_dir.display().to_string())
    )?;
    if let Some(attempt) = snapshot.attempt {
        writeln!(writer, "  attempt: {attempt}")?;
    }
    if let Some(is_resume) = snapshot.is_resume {
        writeln!(writer, "  is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &snapshot.resume_dir {
        writeln!(writer, "  resume dir: {}", resume_dir.display())?;
    }
    writeln!(
        writer,
        "  batch log: {} (present: {}, updated: {})",
        snapshot.batch_log.path.display(),
        yes_no(snapshot.batch_log.present),
        match snapshot.batch_log.updated_age_seconds {
            Some(seconds) => format_age_seconds(seconds),
            None => "unknown".to_string(),
        }
    )?;
    if snapshot.scheduler.terminal {
        write_service_outcome_summary(writer, snapshot)?;
    }
    for service in &snapshot.services {
        let age = match service.updated_age_seconds {
            Some(seconds) => format_age_seconds(seconds),
            None => "unknown".to_string(),
        };
        writeln!(
            writer,
            "  log  service '{}': {} (present: {}, updated: {})",
            service.service_name,
            service.path.display(),
            yes_no(service.present),
            age
        )?;
        if service.step_name.is_some()
            || service.launcher_pid.is_some()
            || service.healthy.is_some()
            || service.completed_successfully.is_some()
            || service.status.is_some()
        {
            let step_name = service.step_name.as_deref().unwrap_or("unknown");
            let pid = service
                .launcher_pid
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let ready = service.healthy.map(yes_no).unwrap_or("unknown");
            let completed = service
                .completed_successfully
                .map(yes_no)
                .unwrap_or("unknown");
            let status = service.status.as_deref().unwrap_or("unknown");
            writeln!(
                writer,
                "    {}: {} {}: {} {}: {} {}: {} {}: {}",
                term::styled_bold("step"),
                step_name,
                term::styled_bold("pid"),
                pid,
                term::styled_bold("ready"),
                ready,
                term::styled_bold("completed"),
                completed,
                term::styled_bold("status"),
                term::styled_service_status(status)
            )?;
        }
        if service.failure_policy_mode.is_some()
            || service.restart_count.is_some()
            || service.max_restarts.is_some()
            || service.window_seconds.is_some()
            || service.max_restarts_in_window.is_some()
            || service.restart_failures_in_window.is_some()
            || service.last_exit_code.is_some()
        {
            let mode = service.failure_policy_mode.as_deref().unwrap_or("unknown");
            let restart_count = service
                .restart_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let max_restarts = service
                .max_restarts
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let window_seconds = service
                .window_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let max_restarts_in_window = service
                .max_restarts_in_window
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let restart_failures_in_window = service
                .restart_failures_in_window
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let last_exit = service
                .last_exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let completed = service
                .completed_successfully
                .map(yes_no)
                .unwrap_or("unknown");
            let window_state = if mode == "restart_on_failure"
                && service.window_seconds.is_some()
                && service.max_restarts_in_window.is_some()
                && service.restart_failures_in_window.is_some()
            {
                format!(
                    " window={}/{}@{}s",
                    restart_failures_in_window, max_restarts_in_window, window_seconds
                )
            } else {
                String::new()
            };
            writeln!(
                writer,
                "  state service '{}': failure_policy={} restarts={}/{}{} last_exit={} completed={}",
                service.service_name,
                mode,
                restart_count,
                max_restarts,
                window_state,
                last_exit,
                completed
            )?;
        }
        if let Some(assertions) = &service.assertions
            && (assertions.configured || assertions.status.as_deref() != Some("none"))
        {
            let status = assertions.status.as_deref().unwrap_or("unknown");
            let expected_exit = assertions
                .expected_exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let artifact_pattern = assertions.artifacts_contain.as_deref().unwrap_or("-");
            let max_duration = assertions
                .max_duration_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let duration = assertions
                .duration_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            writeln!(
                writer,
                "  assert service '{}': status={} exit_code={} artifacts_contain={} duration={}/{}s",
                service.service_name,
                term::styled_service_status(status),
                expected_exit,
                artifact_pattern,
                duration,
                max_duration
            )?;
            for failure in &assertions.failures {
                writeln!(writer, "    assertion: {failure}")?;
            }
        }
        if service.placement_mode.is_some()
            || service.nodes.is_some()
            || service.ntasks.is_some()
            || service.ntasks_per_node.is_some()
            || service.nodelist.is_some()
        {
            writeln!(
                writer,
                "  placement service '{}': mode={} nodes={} ntasks={} ntasks_per_node={} nodelist={}",
                service.service_name,
                service.placement_mode.as_deref().unwrap_or("unknown"),
                service
                    .nodes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                service
                    .ntasks
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                service
                    .ntasks_per_node
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                service.nodelist.as_deref().unwrap_or("unknown"),
            )?;
        }
    }
    if let Some(array) = &snapshot.array {
        write_array_status(writer, array)?;
    }
    Ok(())
}

fn write_service_outcome_summary(
    writer: &mut impl Write,
    snapshot: &StatusSnapshot,
) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_section_header("Service outcomes:")
    )?;
    if snapshot.services.is_empty() {
        writeln!(writer, "  none")?;
        return Ok(());
    }
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "service",
        "readiness",
        "status",
        "exit",
        "restarts",
        "duration",
    ]);
    for service in &snapshot.services {
        let readiness = readiness_outcome_label(service, snapshot.scheduler.terminal);
        let status = service.status.as_deref().unwrap_or("unknown");
        let exit = service
            .last_exit_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let restarts = service
            .restart_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let duration = service
            .duration_seconds
            .map(format_compact_elapsed)
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            service.service_name.clone(),
            readiness.to_string(),
            term::styled_service_status(status),
            exit,
            restarts,
            duration,
        ]);
    }
    write!(writer, "{table}")?;
    Ok(())
}

fn readiness_outcome_label(
    service: &hpc_compose::job::PsServiceRow,
    terminal: bool,
) -> &'static str {
    if service.readiness_configured == Some(false) {
        return "n/a";
    }
    match service.healthy {
        Some(true) => "passed",
        Some(false) if terminal => "failed",
        _ if service.readiness_configured == Some(true) && terminal => "failed",
        _ => "n/a",
    }
}

fn write_array_status(
    writer: &mut impl Write,
    array: &hpc_compose::job::ArrayStatusSnapshot,
) -> io::Result<()> {
    writeln!(writer, "{}", term::styled_section_header("Array tasks:"))?;
    writeln!(
        writer,
        "  parent job id: {}{}",
        array.parent_job_id,
        array
            .filtered_task_id
            .map(|task| format!(" task={task}"))
            .unwrap_or_default()
    )?;
    if let Some(reason) = &array.reason {
        writeln!(writer, "  note: {reason}")?;
    }
    if array.state_counts.is_empty() {
        writeln!(writer, "  counts: none")?;
    } else {
        let counts = array
            .state_counts
            .iter()
            .map(|(state, count)| format!("{state}={count}"))
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(writer, "  counts: {counts}")?;
    }
    if array.tasks.is_empty() {
        writeln!(writer, "  tasks: none")?;
        return Ok(());
    }
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL_CONDENSED);
    table.set_header(vec!["task", "state", "source", "exit", "elapsed", "reason"]);
    for task in &array.tasks {
        table.add_row(vec![
            task.task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| task.job_id_raw.clone()),
            term::styled_scheduler_state(&task.state),
            scheduler_source_label(task.source).to_string(),
            task.exit_code.clone().unwrap_or_else(|| "-".to_string()),
            task.elapsed.clone().unwrap_or_else(|| "-".to_string()),
            task.reason.clone().unwrap_or_else(|| "-".to_string()),
        ]);
    }
    write!(writer, "{table}")?;
    Ok(())
}

fn write_ps_snapshot(writer: &mut impl Write, snapshot: &PsSnapshot) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_label("job id", &snapshot.record.job_id)
    )?;
    writeln!(
        writer,
        "{}: {} ({})",
        term::styled_bold("scheduler"),
        term::styled_scheduler_state(&snapshot.scheduler.state),
        scheduler_source_label(snapshot.scheduler.source)
    )?;
    if let Some(queue) = &snapshot.queue_diagnostics
        && let Some(reason) = &queue.pending_reason
    {
        writeln!(writer, "{}: {reason}", term::styled_bold("pending reason"))?;
    }
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "service",
        "step",
        "pid",
        "ready",
        "status",
        "restarts",
        "last_exit",
        "log",
    ]);
    for service in &snapshot.services {
        let step = service.step_name.as_deref().unwrap_or("-");
        let pid = service
            .launcher_pid
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let ready = service.healthy.map(yes_no).unwrap_or("unknown");
        let status = service.status.as_deref().unwrap_or("unknown");
        let restarts = service
            .restart_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let last_exit = service
            .last_exit_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            service.service_name.clone(),
            step.to_string(),
            pid,
            ready.to_string(),
            term::styled_service_status(status),
            restarts,
            last_exit,
            term::styled_dim(&service.path.display().to_string()),
        ]);
    }
    write!(writer, "{table}")?;
    Ok(())
}

fn write_stats_snapshot(writer: &mut impl Write, snapshot: &StatsSnapshot) -> io::Result<()> {
    writeln!(writer, "{}", term::styled_label("job id", &snapshot.job_id))?;
    writeln!(
        writer,
        "{}: {} ({})",
        term::styled_bold("scheduler state"),
        term::styled_scheduler_state(&snapshot.scheduler.state),
        scheduler_source_label(snapshot.scheduler.source)
    )?;
    if let Some(detail) = &snapshot.scheduler.detail {
        writeln!(writer, "scheduler {}: {detail}", term::styled_bold("note"))?;
    }
    writeln!(
        writer,
        "{}",
        term::styled_label("stats source", &snapshot.source)
    )?;
    if let Some(metrics_dir) = &snapshot.metrics_dir {
        writeln!(writer, "metrics dir: {}", metrics_dir.display())?;
    }
    if let Some(attempt) = snapshot.attempt {
        writeln!(writer, "attempt: {attempt}")?;
    }
    if let Some(is_resume) = snapshot.is_resume {
        writeln!(writer, "is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &snapshot.resume_dir {
        writeln!(writer, "resume dir: {}", resume_dir.display())?;
    }
    if let Some(reason) = &snapshot.reason {
        writeln!(writer, "stats reason: {reason}")?;
    }
    for note in &snapshot.notes {
        writeln!(writer, "note: {note}")?;
    }
    if let Some(accounting) = &snapshot.accounting {
        writeln!(writer)?;
        writeln!(writer, "accounting source: {}", accounting.source)?;
        if !accounting.available {
            writeln!(
                writer,
                "accounting reason: {}",
                accounting.reason.as_deref().unwrap_or("unavailable")
            )?;
        }
        if let Some(summary) = &accounting.summary {
            writeln!(
                writer,
                "allocated cpu hours: {}",
                display_optional_f64(summary.allocated_cpu_hours)
            )?;
            writeln!(
                writer,
                "total cpu hours: {}",
                display_optional_f64(summary.total_cpu_hours)
            )?;
            writeln!(
                writer,
                "allocated gpu hours: {}",
                display_optional_f64(summary.allocated_gpu_hours)
            )?;
            writeln!(
                writer,
                "allocated memory byte-seconds: {} ({})",
                display_optional_f64(summary.allocated_memory_byte_seconds),
                summary.memory_basis
            )?;
            writeln!(
                writer,
                "max rss bytes: {}",
                summary
                    .max_rss_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )?;
        }
    }
    if let Some(failure) = &snapshot.first_failure {
        writeln!(
            writer,
            "first failure: service={} exit={} at={} node={} rank={}",
            failure.service,
            failure.exit_code,
            failure
                .at_unix
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            failure.node.as_deref().unwrap_or("unknown"),
            failure.rank.as_deref().unwrap_or("unknown"),
        )?;
    }
    if let Some(sampler) = &snapshot.sampler {
        for collector in &sampler.collectors {
            if !collector.enabled {
                continue;
            }
            writeln!(
                writer,
                "collector '{}': {} (last sampled: {})",
                collector.name,
                if collector.available {
                    "available"
                } else {
                    "unavailable"
                },
                collector.last_sampled_at.as_deref().unwrap_or("never")
            )?;
        }
        if let Some(gpu) = &sampler.gpu {
            writeln!(writer)?;
            writeln!(writer, "gpu snapshot: {}", gpu.sampled_at)?;
            for node in &gpu.nodes {
                writeln!(
                    writer,
                    "gpu node {}: count={}, avg util={}, mem={} / {}",
                    display_optional_stats_value(node.node.as_deref()),
                    node.gpu_count,
                    node.avg_utilization_gpu
                        .map(|value| format!("{value:.1}"))
                        .unwrap_or_else(|| "-".to_string()),
                    node.memory_used_mib
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    node.memory_total_mib
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                )?;
            }
            for device in &gpu.gpus {
                writeln!(
                    writer,
                    "gpu {} on {}: name={}, util={}, mem util={}, mem={} / {}, temp={}, power={} / {}",
                    display_optional_stats_value(device.index.as_deref()),
                    display_optional_stats_value(device.node.as_deref()),
                    display_optional_stats_value(device.name.as_deref()),
                    display_optional_stats_value(device.utilization_gpu.as_deref()),
                    display_optional_stats_value(device.utilization_memory.as_deref()),
                    display_optional_stats_value(device.memory_used_mib.as_deref()),
                    display_optional_stats_value(device.memory_total_mib.as_deref()),
                    display_optional_stats_value(device.temperature_c.as_deref()),
                    display_optional_stats_value(device.power_draw_w.as_deref()),
                    display_optional_stats_value(device.power_limit_w.as_deref()),
                )?;
            }
            for process in &gpu.processes {
                writeln!(
                    writer,
                    "gpu process: pid={}, name={}, gpu_uuid={}, mem={}",
                    display_optional_stats_value(process.pid.as_deref()),
                    display_optional_stats_value(process.process_name.as_deref()),
                    display_optional_stats_value(process.gpu_uuid.as_deref()),
                    display_optional_stats_value(process.used_memory_mib.as_deref()),
                )?;
            }
        }
    }
    if !snapshot.available {
        return Ok(());
    }
    for step in &snapshot.steps {
        writeln!(writer)?;
        writeln!(writer, "step: {}", step.step_id)?;
        writeln!(writer, "ntasks: {}", display_stats_value(&step.ntasks))?;
        writeln!(writer, "ave cpu: {}", display_stats_value(&step.ave_cpu))?;
        writeln!(writer, "ave rss: {}", display_stats_value(&step.ave_rss))?;
        writeln!(writer, "max rss: {}", display_stats_value(&step.max_rss))?;
        writeln!(
            writer,
            "alloc tres: {}",
            display_stats_value(&step.alloc_tres)
        )?;
        writeln!(
            writer,
            "tres usage in ave: {}",
            display_stats_value(&step.tres_usage_in_ave)
        )?;
        if let Some(gpu_count) = &step.gpu_count {
            writeln!(writer, "gpu count: {gpu_count}")?;
        }
        if let Some(gpu_util) = &step.gpu_util {
            writeln!(writer, "gpu util: {gpu_util}")?;
        }
        if let Some(gpu_mem) = &step.gpu_mem {
            writeln!(writer, "gpu mem: {gpu_mem}")?;
        }
    }
    Ok(())
}

fn write_rightsize_report(writer: &mut impl Write, report: &RightsizeReport) -> io::Result<()> {
    writeln!(writer, "{}", term::styled_label("job id", &report.job_id))?;
    writeln!(
        writer,
        "{}: {} ({})",
        term::styled_bold("scheduler state"),
        term::styled_scheduler_state(&report.scheduler_state),
        report.scheduler_source
    )?;
    writeln!(
        writer,
        "{}: {}",
        term::styled_bold("rightsize status"),
        if report.complete {
            "complete"
        } else {
            "provisional"
        }
    )?;
    if !report.sources.is_empty() {
        writeln!(writer, "sources: {}", report.sources.join(", "))?;
    }
    for note in &report.notes {
        writeln!(writer, "note: {note}")?;
    }

    if !report.observations.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "{}", term::styled_bold("observations"))?;
        for observation in &report.observations {
            let requested = observation.requested.as_deref().unwrap_or("-");
            let observed = observation.observed.as_deref().unwrap_or("-");
            let utilization = observation
                .utilization
                .map(|value| format!(" ({:.1}%)", value * 100.0))
                .unwrap_or_default();
            writeln!(
                writer,
                "- {} [{}]: observed {} of {}{} via {} ({})",
                observation.resource,
                observation.scope,
                observed,
                requested,
                utilization,
                observation.source,
                confidence_label(observation.confidence),
            )?;
            if let Some(note) = &observation.note {
                writeln!(writer, "  note: {note}")?;
            }
        }
    }

    writeln!(writer)?;
    writeln!(writer, "{}", term::styled_bold("recommendations"))?;
    if report.recommendations.is_empty() {
        writeln!(
            writer,
            "No concrete right-sizing changes suggested from the available evidence."
        )?;
        return Ok(());
    }
    for recommendation in &report.recommendations {
        writeln!(
            writer,
            "- {}: consider {}: {} (was {}, observed {}; confidence: {})",
            recommendation.scope,
            recommendation.target_path,
            recommendation.suggested,
            recommendation.current,
            recommendation.observed,
            confidence_label(recommendation.confidence),
        )?;
        writeln!(writer, "  reason: {}", recommendation.reason)?;
    }
    Ok(())
}

fn write_efficiency_score_report(
    writer: &mut impl Write,
    report: &EfficiencyScoreReport,
) -> io::Result<()> {
    const INNER_WIDTH: usize = 46;
    writeln!(writer, "+{}+", "-".repeat(INNER_WIDTH + 2))?;
    write_score_card_line(
        writer,
        INNER_WIDTH,
        &format!("EFFICIENCY SCORE: {}/100 ({})", report.score, report.grade),
    )?;
    write_score_card_line(writer, INNER_WIDTH, "")?;
    write_score_metric_line(writer, INNER_WIDTH, report, "gpu_utilization", "GPU Util")?;
    write_score_metric_line(writer, INNER_WIDTH, report, "memory_utilization", "Memory")?;
    write_score_metric_line(
        writer,
        INNER_WIDTH,
        report,
        "compute_time_utilization",
        "Walltime",
    )?;
    let energy = report
        .energy_kwh
        .map(|value| format!("~{value:.2} kWh"))
        .unwrap_or_else(|| "n/a".to_string());
    write_score_card_line(writer, INNER_WIDTH, &format!("Energy:     {energy}"))?;
    write_score_card_line(writer, INNER_WIDTH, "")?;
    let tip = report
        .tips
        .first()
        .cloned()
        .unwrap_or_else(|| "No concrete right-sizing tip from available evidence.".to_string());
    for line in wrap_score_card_text(&format!("Tip: {tip}"), INNER_WIDTH) {
        write_score_card_line(writer, INNER_WIDTH, &line)?;
    }
    writeln!(writer, "+{}+", "-".repeat(INNER_WIDTH + 2))?;
    writeln!(
        writer,
        "{}: {} ({})",
        term::styled_bold("scheduler state"),
        term::styled_scheduler_state(&report.scheduler_state),
        report.scheduler_source
    )?;
    writeln!(
        writer,
        "{}: {}",
        term::styled_bold("score status"),
        if report.complete {
            "complete"
        } else {
            "provisional"
        }
    )?;
    writeln!(
        writer,
        "confidence: {}",
        score_confidence_label(report.confidence)
    )?;
    if !report.sources.is_empty() {
        writeln!(writer, "sources: {}", report.sources.join(", "))?;
    }
    for note in &report.notes {
        writeln!(writer, "note: {note}")?;
    }
    Ok(())
}

fn write_score_metric_line(
    writer: &mut impl Write,
    width: usize,
    report: &EfficiencyScoreReport,
    name: &str,
    label: &str,
) -> io::Result<()> {
    let component = report
        .components
        .iter()
        .find(|component| component.name == name);
    let value = component
        .and_then(|component| component.utilization)
        .map(|utilization| format!("{:>3.0}%", (utilization * 100.0).clamp(0.0, 999.0)))
        .unwrap_or_else(|| "n/a".to_string());
    let bar = component
        .and_then(|component| component.utilization)
        .map(score_bar)
        .unwrap_or_else(|| "..........".to_string());
    write_score_card_line(writer, width, &format!("{label:<11} {value:>5} {bar}"))
}

fn write_score_card_line(writer: &mut impl Write, width: usize, content: &str) -> io::Result<()> {
    let clipped = clip_ascii(content, width);
    writeln!(writer, "| {clipped:<width$} |")
}

fn score_bar(utilization: f64) -> String {
    let filled = ((utilization.clamp(0.0, 1.0) * 10.0).round() as usize).min(10);
    format!("{}{}", "#".repeat(filled), ".".repeat(10 - filled))
}

fn wrap_score_card_text(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let next_len = if current.is_empty() {
            word.len()
        } else {
            current.len() + 1 + word.len()
        };
        if next_len > width && !current.is_empty() {
            lines.push(current);
            current = word.to_string();
        } else if current.is_empty() {
            current = word.to_string();
        } else {
            current.push(' ');
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn clip_ascii(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    format!("{}...", &value[..width - 3])
}

fn score_confidence_label(confidence: hpc_compose::job::EfficiencyScoreConfidence) -> &'static str {
    match confidence {
        hpc_compose::job::EfficiencyScoreConfidence::High => "high",
        hpc_compose::job::EfficiencyScoreConfidence::Medium => "medium",
        hpc_compose::job::EfficiencyScoreConfidence::Low => "low",
    }
}

fn confidence_label(confidence: RightsizeConfidence) -> &'static str {
    match confidence {
        RightsizeConfidence::High => "high",
        RightsizeConfidence::Medium => "medium",
        RightsizeConfidence::Low => "low",
    }
}

pub(crate) fn write_stats_snapshot_csv(
    writer: &mut impl Write,
    snapshot: &StatsSnapshot,
) -> io::Result<()> {
    if snapshot.accounting.is_some() {
        return write_accounting_snapshot_csv(writer, snapshot);
    }
    writeln!(
        writer,
        "job_id,scheduler_state,scheduler_source,stats_source,step_id,ntasks,ave_cpu,ave_rss,max_rss,alloc_tres,tres_usage_in_ave,gpu_count,gpu_util,gpu_mem,alloc_tres_map,usage_tres_in_ave_map"
    )?;
    for step in &snapshot.steps {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&snapshot.job_id),
            csv_field(&snapshot.scheduler.state),
            csv_field(scheduler_source_label(snapshot.scheduler.source)),
            csv_field(&snapshot.source),
            csv_field(&step.step_id),
            csv_field(&step.ntasks),
            csv_field(&step.ave_cpu),
            csv_field(&step.ave_rss),
            csv_field(&step.max_rss),
            csv_field(&step.alloc_tres),
            csv_field(&step.tres_usage_in_ave),
            csv_field(step.gpu_count.as_deref().unwrap_or("")),
            csv_field(step.gpu_util.as_deref().unwrap_or("")),
            csv_field(step.gpu_mem.as_deref().unwrap_or("")),
            csv_field(&format_tres_map(&step.alloc_tres_map)),
            csv_field(&format_tres_map(&step.usage_tres_in_ave_map)),
        )?;
    }
    Ok(())
}

pub(crate) fn write_stats_snapshot_jsonl(
    writer: &mut impl Write,
    snapshot: &StatsSnapshot,
) -> io::Result<()> {
    write_jsonl_record(
        writer,
        &serde_json::json!({
            "record_type": "summary",
            "job_id": snapshot.job_id,
            "scheduler_state": snapshot.scheduler.state,
            "scheduler_source": scheduler_source_label(snapshot.scheduler.source),
            "stats_source": snapshot.source,
            "available": snapshot.available,
            "reason": snapshot.reason,
            "metrics_dir": snapshot.metrics_dir,
            "attempt": snapshot.attempt,
            "is_resume": snapshot.is_resume,
            "resume_dir": snapshot.resume_dir,
            "first_failure": snapshot.first_failure,
            "accounting": snapshot.accounting,
        }),
    )?;
    if let Some(accounting) = &snapshot.accounting {
        write_jsonl_record(
            writer,
            &serde_json::json!({
                "record_type": "accounting_summary",
                "job_id": snapshot.job_id,
                "available": accounting.available,
                "reason": accounting.reason,
                "source": accounting.source,
                "summary": accounting.summary,
            }),
        )?;
        for row in &accounting.rows {
            write_jsonl_record(
                writer,
                &serde_json::json!({
                    "record_type": "accounting_row",
                    "job_id": snapshot.job_id,
                    "row": row,
                }),
            )?;
        }
    }
    for note in &snapshot.notes {
        write_jsonl_record(
            writer,
            &serde_json::json!({
                "record_type": "note",
                "job_id": snapshot.job_id,
                "message": note,
            }),
        )?;
    }
    if let Some(sampler) = &snapshot.sampler {
        for collector in &sampler.collectors {
            write_jsonl_record(
                writer,
                &serde_json::json!({
                    "record_type": "collector",
                    "job_id": snapshot.job_id,
                    "name": collector.name,
                    "enabled": collector.enabled,
                    "available": collector.available,
                    "note": collector.note,
                    "last_sampled_at": collector.last_sampled_at,
                }),
            )?;
        }
        if let Some(gpu) = &sampler.gpu {
            for device in &gpu.gpus {
                write_jsonl_record(
                    writer,
                    &serde_json::json!({
                        "record_type": "gpu_device",
                        "job_id": snapshot.job_id,
                        "sampled_at": gpu.sampled_at,
                        "device": device,
                    }),
                )?;
            }
            for process in &gpu.processes {
                write_jsonl_record(
                    writer,
                    &serde_json::json!({
                        "record_type": "gpu_process",
                        "job_id": snapshot.job_id,
                        "sampled_at": gpu.sampled_at,
                        "process": process,
                    }),
                )?;
            }
        }
    }
    for step in &snapshot.steps {
        write_jsonl_record(
            writer,
            &serde_json::json!({
                "record_type": "step",
                "job_id": snapshot.job_id,
                "scheduler_state": snapshot.scheduler.state,
                "scheduler_source": scheduler_source_label(snapshot.scheduler.source),
                "stats_source": snapshot.source,
                "step": step,
            }),
        )?;
    }
    Ok(())
}

fn write_accounting_snapshot_csv(
    writer: &mut impl Write,
    snapshot: &StatsSnapshot,
) -> io::Result<()> {
    writeln!(
        writer,
        "job_id,accounting_available,accounting_reason,allocated_cpu_hours,total_cpu_hours,allocated_gpu_hours,allocated_memory_byte_seconds,memory_basis,max_rss_bytes"
    )?;
    let accounting = snapshot.accounting.as_ref();
    let summary = accounting.and_then(|accounting| accounting.summary.as_ref());
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{}",
        csv_field(&snapshot.job_id),
        csv_field(if accounting.is_some_and(|item| item.available) {
            "true"
        } else {
            "false"
        }),
        csv_field(
            accounting
                .and_then(|item| item.reason.as_deref())
                .unwrap_or("")
        ),
        csv_field(
            &format_optional_f64(summary.and_then(|summary| summary.allocated_cpu_hours))
                .unwrap_or_default()
        ),
        csv_field(
            &format_optional_f64(summary.and_then(|summary| summary.total_cpu_hours))
                .unwrap_or_default()
        ),
        csv_field(
            &format_optional_f64(summary.and_then(|summary| summary.allocated_gpu_hours))
                .unwrap_or_default()
        ),
        csv_field(
            &format_optional_f64(
                summary.and_then(|summary| { summary.allocated_memory_byte_seconds })
            )
            .unwrap_or_default()
        ),
        csv_field(
            summary
                .map(|summary| summary.memory_basis.as_str())
                .unwrap_or("")
        ),
        csv_field(
            &summary
                .and_then(|summary| summary.max_rss_bytes)
                .map(|value| value.to_string())
                .unwrap_or_default()
        ),
    )
}

fn write_jsonl_record(writer: &mut impl Write, value: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(io::Error::other)?;
    writeln!(writer)
}

fn csv_field(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn format_tres_map(values: &std::collections::BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn write_artifact_export_report(
    writer: &mut impl Write,
    report: &ArtifactExportReport,
) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_label("job id", &report.record.job_id)
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("manifest", &report.manifest_path.display().to_string())
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("payload dir", &report.payload_dir.display().to_string())
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("export dir", &report.export_dir.display().to_string())
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("collect policy", &report.manifest.collect_policy)
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("job outcome", &report.manifest.job_outcome)
    )?;
    if let Some(attempt) = report.manifest.attempt {
        writeln!(writer, "attempt: {attempt}")?;
    }
    if let Some(is_resume) = report.manifest.is_resume {
        writeln!(writer, "is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &report.manifest.resume_dir {
        writeln!(writer, "resume dir: {}", resume_dir.display())?;
    }
    writeln!(
        writer,
        "declared patterns: {}",
        report.manifest.declared_source_patterns.len()
    )?;
    writeln!(
        writer,
        "matched source paths: {}",
        report.manifest.matched_source_paths.len()
    )?;
    writeln!(
        writer,
        "selected bundles: {}",
        report.selected_bundles.join(",")
    )?;
    writeln!(writer, "bundle reports: {}", report.bundles.len())?;
    writeln!(writer, "exported paths: {}", report.exported_paths.len())?;
    for warning in &report.warnings {
        writeln!(writer, "warning: {warning}")?;
    }
    for bundle in &report.bundles {
        writeln!(
            writer,
            "bundle '{}': exported={} provenance={}{}",
            bundle.name,
            bundle.exported_paths.len(),
            bundle.provenance_path.display(),
            match &bundle.tarball_path {
                Some(path) => format!(" tarball={}", path.display()),
                None => String::new(),
            }
        )?;
        for warning in &bundle.warnings {
            writeln!(writer, "bundle warning '{}': {warning}", bundle.name)?;
        }
    }
    for path in &report.exported_paths {
        writeln!(writer, "exported: {}", path.display())?;
    }
    Ok(())
}

fn write_plan_inspect_tree(
    writer: &mut impl Write,
    plan: &Plan,
    runtime_plan: &RuntimePlan,
) -> io::Result<()> {
    let services: Vec<&hpc_compose::planner::PlannedService> =
        plan.ordered_services.iter().collect();
    let name_to_index: std::collections::HashMap<&str, usize> = services
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    let mut children: Vec<Vec<usize>> = vec![Vec::new(); services.len()];
    let mut has_parent = vec![false; services.len()];
    for (idx, svc) in services.iter().enumerate() {
        for dep in &svc.depends_on {
            if let Some(&dep_idx) = name_to_index.get(dep.name.as_str()) {
                children[dep_idx].push(idx);
                has_parent[idx] = true;
            }
        }
    }

    let roots: Vec<usize> = (0..services.len()).filter(|&i| !has_parent[i]).collect();

    for (root_i, &root_idx) in roots.iter().enumerate() {
        let is_last_root = root_i == roots.len() - 1;
        write_tree_node(
            writer,
            root_idx,
            &services,
            &children,
            runtime_plan,
            "",
            is_last_root,
        )?;
        if !is_last_root {
            writeln!(writer)?;
        }
    }

    Ok(())
}

fn write_tree_node(
    writer: &mut impl Write,
    idx: usize,
    services: &[&hpc_compose::planner::PlannedService],
    children: &[Vec<usize>],
    runtime_plan: &RuntimePlan,
    prefix: &str,
    is_last: bool,
) -> io::Result<()> {
    let connector = if is_last {
        "\u{2514}\u{2500}\u{2500} "
    } else {
        "\u{251c}\u{2500}\u{2500} "
    };
    let svc = services[idx];
    let state = runtime_plan
        .ordered_services
        .iter()
        .find(|rs| rs.name == svc.name)
        .map(|rs| runtime_cache_state(rs))
        .unwrap_or("unknown");

    let state_colored = match state {
        "cache hit" | "local image present" => term::styled_success_raw(state),
        "rebuild on prepare" | "cache miss" | "local image missing" => {
            term::styled_warning_raw(state)
        }
        _ => state.to_string(),
    };

    if prefix.is_empty() {
        writeln!(writer, "{} {}", term::styled_bold(&svc.name), state_colored,)?;
    } else {
        writeln!(
            writer,
            "{}{}{} {}",
            prefix,
            connector,
            term::styled_bold(&svc.name),
            state_colored,
        )?;
    }

    let child_prefix = if prefix.is_empty() {
        "    ".to_string()
    } else if is_last {
        format!("{prefix}    ")
    } else {
        format!("{prefix}\u{2502}   ")
    };

    let deps: Vec<String> = svc
        .depends_on
        .iter()
        .map(|d| {
            let cond = match d.condition {
                DependencyCondition::ServiceStarted => "service_started",
                DependencyCondition::ServiceHealthy => "service_healthy",
                DependencyCondition::ServiceCompletedSuccessfully => {
                    "service_completed_successfully"
                }
            };
            format!(
                "{} [{}]",
                term::styled_dim(d.name.as_str()),
                term::styled_dim(cond)
            )
        })
        .collect();

    let mut details = Vec::new();
    if !deps.is_empty() {
        details.push(format!("depends_on: {}", deps.join(", ")));
    }
    if let Some(ref readiness) = svc.readiness {
        details.push(format!(
            "readiness: {}",
            readiness_description(Some(readiness))
        ));
    }
    if !svc.volumes.is_empty() {
        details.push(format!("mounts: {}", svc.volumes.len()));
    }

    let detail_count = details.len() + children[idx].len();
    for (di, detail) in details.iter().enumerate() {
        let detail_is_last = di == detail_count - 1;
        let detail_connector = if detail_is_last {
            "\u{2514}\u{2500}\u{2500} "
        } else {
            "\u{251c}\u{2500}\u{2500} "
        };
        writeln!(writer, "{}{}{}", child_prefix, detail_connector, detail)?;
    }

    for (ci, &child_idx) in children[idx].iter().enumerate() {
        let child_is_last = ci == children[idx].len() - 1;
        write_tree_node(
            writer,
            child_idx,
            services,
            children,
            runtime_plan,
            &child_prefix,
            child_is_last,
        )?;
    }

    Ok(())
}

pub(crate) fn print_plan_inspect_tree(plan: &Plan, runtime_plan: &RuntimePlan) -> Result<()> {
    let mut writer = io::stdout();
    write_plan_inspect_tree(&mut writer, plan, runtime_plan).context("failed to write tree output")
}

pub(crate) fn build_dependency_graph(
    plan: &Plan,
    runtime_plan: &RuntimePlan,
) -> DependencyGraphOutput {
    let runtime_by_name = runtime_plan
        .ordered_services
        .iter()
        .map(|service| (service.name.as_str(), service))
        .collect::<std::collections::HashMap<_, _>>();
    let mut incoming = std::collections::BTreeMap::<String, usize>::new();
    let mut outgoing = std::collections::BTreeMap::<String, usize>::new();
    let mut nodes = Vec::with_capacity(plan.ordered_services.len());
    let mut edges = Vec::new();

    for planned in &plan.ordered_services {
        let runtime = runtime_by_name.get(planned.name.as_str());
        let readiness = runtime
            .map(|service| readiness_description(service.readiness.as_ref()))
            .unwrap_or_else(|| readiness_description(planned.readiness.as_ref()));
        let readiness_type = runtime
            .and_then(|service| service.readiness.as_ref())
            .or(planned.readiness.as_ref())
            .map(readiness_type)
            .unwrap_or("none")
            .to_string();
        incoming.entry(planned.name.clone()).or_insert(0);
        outgoing.entry(planned.name.clone()).or_insert(0);
        nodes.push(DependencyGraphNode {
            service: planned.name.clone(),
            readiness,
            readiness_type,
        });
    }

    let node_readiness = nodes
        .iter()
        .map(|node| {
            (
                node.service.as_str(),
                (node.readiness.as_str(), node.readiness_type.as_str()),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    for planned in &plan.ordered_services {
        for dependency in &planned.depends_on {
            *incoming.entry(planned.name.clone()).or_insert(0) += 1;
            *outgoing.entry(dependency.name.clone()).or_insert(0) += 1;
            let (readiness, readiness_type) = node_readiness
                .get(dependency.name.as_str())
                .copied()
                .unwrap_or(("none", "none"));
            edges.push(DependencyGraphEdge {
                from: dependency.name.clone(),
                to: planned.name.clone(),
                condition: dependency_condition_label(dependency.condition).to_string(),
                readiness: readiness.to_string(),
                readiness_type: readiness_type.to_string(),
            });
        }
    }

    let roots = nodes
        .iter()
        .filter(|node| incoming.get(&node.service).copied().unwrap_or(0) == 0)
        .map(|node| node.service.clone())
        .collect();
    let leaves = nodes
        .iter()
        .filter(|node| outgoing.get(&node.service).copied().unwrap_or(0) == 0)
        .map(|node| node.service.clone())
        .collect();

    DependencyGraphOutput {
        nodes,
        edges,
        roots,
        leaves,
    }
}

pub(crate) fn print_dependency_graph_text(graph: &DependencyGraphOutput) -> io::Result<()> {
    write_dependency_graph_text(&mut io::stdout(), graph)
}

pub(crate) fn print_dependency_graph_dot(graph: &DependencyGraphOutput) -> io::Result<()> {
    write_dependency_graph_dot(&mut io::stdout(), graph)
}

fn write_dependency_graph_text(
    writer: &mut impl Write,
    graph: &DependencyGraphOutput,
) -> io::Result<()> {
    writeln!(writer, "dependency graph:")?;
    writeln!(
        writer,
        "  roots: {}",
        if graph.roots.is_empty() {
            "none".to_string()
        } else {
            graph.roots.join(", ")
        }
    )?;
    writeln!(
        writer,
        "  leaves: {}",
        if graph.leaves.is_empty() {
            "none".to_string()
        } else {
            graph.leaves.join(", ")
        }
    )?;
    writeln!(writer)?;
    writeln!(writer, "services:")?;
    for node in &graph.nodes {
        writeln!(
            writer,
            "  {} readiness={} ({})",
            node.service, node.readiness_type, node.readiness
        )?;
    }
    writeln!(writer)?;
    writeln!(writer, "edges:")?;
    if graph.edges.is_empty() {
        writeln!(writer, "  none")?;
    } else {
        for edge in &graph.edges {
            writeln!(
                writer,
                "  {} -> {} condition={} readiness={} ({})",
                edge.from, edge.to, edge.condition, edge.readiness_type, edge.readiness
            )?;
        }
    }
    Ok(())
}

fn write_dependency_graph_dot(
    writer: &mut impl Write,
    graph: &DependencyGraphOutput,
) -> io::Result<()> {
    writeln!(writer, "digraph hpc_compose_dependencies {{")?;
    writeln!(writer, "  rankdir=LR;")?;
    for node in &graph.nodes {
        writeln!(
            writer,
            "  \"{}\" [label=\"{}\\nreadiness: {}\"];",
            dot_escape(&node.service),
            dot_escape(&node.service),
            dot_escape(&node.readiness_type)
        )?;
    }
    for edge in &graph.edges {
        writeln!(
            writer,
            "  \"{}\" -> \"{}\" [label=\"{} / {}\"];",
            dot_escape(&edge.from),
            dot_escape(&edge.to),
            dot_escape(&edge.condition),
            dot_escape(&edge.readiness_type)
        )?;
    }
    writeln!(writer, "}}")
}

fn dependency_condition_label(condition: DependencyCondition) -> &'static str {
    match condition {
        DependencyCondition::ServiceStarted => "service_started",
        DependencyCondition::ServiceHealthy => "service_healthy",
        DependencyCondition::ServiceCompletedSuccessfully => "service_completed_successfully",
    }
}

fn readiness_type(readiness: &hpc_compose::spec::ReadinessSpec) -> &'static str {
    match readiness {
        hpc_compose::spec::ReadinessSpec::Sleep { .. } => "sleep",
        hpc_compose::spec::ReadinessSpec::Tcp { .. } => "tcp",
        hpc_compose::spec::ReadinessSpec::Log { .. } => "log",
        hpc_compose::spec::ReadinessSpec::Http { .. } => "http",
    }
}

fn dot_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn write_plan_inspect_verbose(
    writer: &mut impl Write,
    plan: &Plan,
    runtime_plan: &RuntimePlan,
    cluster_profile: Option<&ClusterProfile>,
) -> io::Result<()> {
    write_plan_inspect(writer, runtime_plan)?;
    writeln!(writer)?;
    writeln!(writer, "compose file: {}", plan.spec_path.display())?;
    writeln!(writer, "project dir: {}", plan.project_dir.display())?;

    for (planned, runtime) in plan
        .ordered_services
        .iter()
        .zip(runtime_plan.ordered_services.iter())
    {
        writeln!(writer)?;
        writeln!(writer, "details for service '{}':", runtime.name)?;
        writeln!(
            writer,
            "execution form: {}",
            execution_form_label(&runtime.execution)
        )?;
        writeln!(
            writer,
            "resolved argv: {}",
            execution_argv(&runtime.execution, runtime.working_dir.as_deref()).join(" ")
        )?;
        writeln!(
            writer,
            "working dir: {}",
            runtime.working_dir.as_deref().unwrap_or("<image default>")
        )?;
        writeln!(writer, "{}", format_mount_block(runtime))?;
        writeln!(
            writer,
            "{}",
            format_environment_block(runtime, cluster_profile)
        )?;
        writeln!(
            writer,
            "depends_on: {}",
            if planned.depends_on.is_empty() {
                "0".to_string()
            } else {
                format_dependencies(&planned.depends_on)
            }
        )?;
        writeln!(
            writer,
            "readiness: {}",
            readiness_description(runtime.readiness.as_ref())
        )?;
        writeln!(
            writer,
            "{}",
            format_command_block(
                "effective srun args",
                &display_srun_command_for_backend(runtime, runtime_plan.runtime.backend),
            )
        )?;
        writeln!(
            writer,
            "step geometry: {}",
            format_service_step_geometry(runtime)
        )?;
        if let Some(reason) = rebuild_reason(runtime) {
            writeln!(writer, "rebuild reason: {reason}")?;
        }
    }
    Ok(())
}

fn format_mount_block(runtime: &RuntimeService) -> String {
    let mut mounts = Vec::with_capacity(runtime.volumes.len() + 1);
    mounts.push("${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}:/hpc-compose/job".into());
    mounts.extend(runtime.volumes.iter().cloned());
    format_debug_block("mounts", &mounts)
}

fn format_environment_block(
    runtime: &RuntimeService,
    cluster_profile: Option<&ClusterProfile>,
) -> String {
    let values = runtime
        .environment
        .iter()
        .map(|(name, _)| name.clone())
        .chain(distributed_environment_names_for_service(
            runtime,
            cluster_profile,
        ))
        .collect::<Vec<_>>();
    format_debug_block("environment", &values)
}

fn format_debug_block(label: &str, values: &[String]) -> String {
    if values.is_empty() {
        return format!("{label}: <none>");
    }

    let mut lines = vec![format!("{label}:")];
    for value in values {
        lines.push(format!("  - {value}"));
    }
    lines.join("\n")
}

fn format_command_block(label: &str, argv: &[String]) -> String {
    if argv.is_empty() {
        return format!("{label}: <none>");
    }
    if argv.len() == 1 {
        return format!("{label}: {}", argv[0]);
    }
    let mut lines = vec![format!("  {}", shell_quote_arg(&argv[0]))];
    for arg in &argv[1..] {
        lines.push(format!("    {}", shell_quote_arg(arg)));
    }
    format!("{label}:\n{}", lines.join(" \\\n"))
}

fn shell_quote_arg(value: &str) -> String {
    if value.chars().all(|ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '=' | ',')
    }) {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn format_allocation_geometry(plan: &RuntimePlan) -> String {
    format!(
        "nodes={} ntasks={} ntasks_per_node={}",
        plan.slurm.allocation_nodes(),
        format_optional_u32(plan.slurm.ntasks),
        format_optional_u32(plan.slurm.ntasks_per_node)
    )
}

fn inspect_time_limit_warnings(plan: &RuntimePlan) -> Vec<String> {
    let allocation_limit = plan
        .slurm
        .time
        .as_deref()
        .and_then(|value| parse_slurm_time_limit(value).ok());
    let service_limits = plan
        .ordered_services
        .iter()
        .filter_map(|service| {
            service
                .slurm
                .time_limit
                .as_deref()
                .and_then(|value| parse_slurm_time_limit(value).ok())
                .map(|seconds| (service.name.as_str(), seconds))
        })
        .collect::<BTreeMap<_, _>>();
    let mut warnings = std::collections::BTreeSet::new();

    for service in &plan.ordered_services {
        let Some(raw_limit) = service.slurm.time_limit.as_deref() else {
            continue;
        };
        let Some(service_limit) = parse_slurm_time_limit(raw_limit).ok() else {
            continue;
        };
        if let Some(allocation_limit) = allocation_limit
            && service_limit > allocation_limit
        {
            warnings.insert(format!(
                "service '{}' advisory time_limit {} exceeds allocation time {}",
                service.name,
                raw_limit,
                plan.slurm.time.as_deref().unwrap_or("unknown"),
            ));
        }
        for dependency in &service.depends_on {
            let Some(dependency_limit) = service_limits.get(dependency.name.as_str()) else {
                continue;
            };
            let Some(dependency_raw) = plan
                .ordered_services
                .iter()
                .find(|candidate| candidate.name == dependency.name)
                .and_then(|candidate| candidate.slurm.time_limit.as_deref())
            else {
                continue;
            };
            if *dependency_limit < service_limit {
                warnings.insert(format!(
                    "dependency '{}' advisory time_limit {} is shorter than dependent service '{}' ({})",
                    dependency.name, dependency_raw, service.name, raw_limit
                ));
            }
        }
    }

    if let Some(distributed) = plan
        .ordered_services
        .iter()
        .find(|service| service.placement.mode == ServicePlacementMode::Distributed)
        && let Some(distributed_raw) = distributed.slurm.time_limit.as_deref()
        && let Ok(distributed_limit) = parse_slurm_time_limit(distributed_raw)
    {
        for helper in plan
            .ordered_services
            .iter()
            .filter(|service| service.name != distributed.name)
        {
            let Some(helper_raw) = helper.slurm.time_limit.as_deref() else {
                continue;
            };
            let Ok(helper_limit) = parse_slurm_time_limit(helper_raw) else {
                continue;
            };
            if helper_limit > distributed_limit {
                warnings.insert(format!(
                    "helper service '{}' advisory time_limit {} outlives distributed service '{}' ({})",
                    helper.name, helper_raw, distributed.name, distributed_raw
                ));
            } else if helper_limit < distributed_limit {
                warnings.insert(format!(
                    "helper service '{}' advisory time_limit {} may exit before distributed service '{}' ({})",
                    helper.name, helper_raw, distributed.name, distributed_raw
                ));
            }
        }
    }

    warnings.into_iter().collect()
}

fn format_service_step_geometry(service: &RuntimeService) -> String {
    let mut parts = vec![
        format!("mode={}", placement_mode_label(service.placement.mode)),
        format!("nodes={}", service.placement.nodes),
        format!("ntasks={}", format_optional_u32(service.placement.ntasks)),
        format!(
            "ntasks_per_node={}",
            format_optional_u32(service.placement.ntasks_per_node)
        ),
    ];
    if service.placement.pin_to_primary_node {
        parts.push("nodelist=$HPC_COMPOSE_PRIMARY_NODE".to_string());
    }
    if let Some(indices) = &service.placement.node_indices {
        parts.push(format!(
            "node_indices={}",
            indices
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if !service.placement.exclude_indices.is_empty() {
        parts.push(format!(
            "exclude_indices={}",
            service
                .placement
                .exclude_indices
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    parts.join(" ")
}

fn format_optional_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "auto".to_string())
}

fn placement_mode_label(mode: ServicePlacementMode) -> &'static str {
    match mode {
        ServicePlacementMode::PrimaryNode => "primary_node",
        ServicePlacementMode::Partitioned => "partitioned",
        ServicePlacementMode::Distributed => "distributed",
    }
}

fn write_plan_inspect(writer: &mut impl Write, plan: &RuntimePlan) -> io::Result<()> {
    writeln!(writer, "{}", term::styled_label("name", &plan.name))?;
    writeln!(writer, "runtime mode: pyxis")?;
    writeln!(
        writer,
        "{}",
        term::styled_label("cache dir", &plan.cache_dir.display().to_string())
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("allocation geometry", &format_allocation_geometry(plan))
    )?;
    writeln!(
        writer,
        "{}",
        term::styled_label("service order", &service_names(plan).join(" -> "))
    )?;
    let warnings = inspect_time_limit_warnings(plan);
    if !warnings.is_empty() {
        writeln!(writer, "warnings:")?;
        for warning in warnings {
            writeln!(writer, "  - {warning}")?;
        }
    }

    for service in &plan.ordered_services {
        writeln!(writer)?;
        writeln!(writer, "service: {}", service.name)?;
        writeln!(
            writer,
            "source image: {}",
            source_image_display(&service.source)
        )?;
        if let ImageSource::Remote(_) = &service.source {
            let base_path =
                base_image_path_for_backend(&plan.cache_dir, service, plan.runtime.backend);
            writeln!(writer, "base cache artifact: {}", base_path.display())?;
            writeln!(
                writer,
                "base cache state: {}",
                hit_or_miss(base_path.exists())
            )?;
        }
        writeln!(writer, "runtime image: {}", service.runtime_image.display())?;
        writeln!(
            writer,
            "runtime image state: {}",
            runtime_cache_state(service)
        )?;
        writeln!(
            writer,
            "step geometry: {}",
            format_service_step_geometry(service)
        )?;
        if let Some(mpi) = &service.slurm.mpi {
            writeln!(writer, "mpi: {}", mpi.mpi_type.as_srun_value())?;
            if let Some(profile) = mpi.profile {
                writeln!(writer, "mpi profile: {}", profile.as_str())?;
            }
            if let Some(implementation) = mpi.resolved_implementation() {
                writeln!(writer, "mpi implementation: {}", implementation.as_str())?;
            }
        }
        if let Some(prepare) = &service.prepare {
            writeln!(
                writer,
                "prepare commands: {}",
                if prepare.commands.is_empty() {
                    "0".to_string()
                } else {
                    prepare.commands.len().to_string()
                }
            )?;
            if prepare.force_rebuild {
                writeln!(
                    writer,
                    "reuse policy: rebuild on prepare because prepare.mounts are present"
                )?;
            } else {
                writeln!(
                    writer,
                    "reuse policy: reuse prepared image when the cached artifact exists"
                )?;
            }
        } else if matches!(service.source, ImageSource::LocalSqsh(_)) {
            writeln!(writer, "reuse policy: uses local .sqsh directly")?;
        } else {
            writeln!(
                writer,
                "reuse policy: reuse imported base image when the cached artifact exists"
            )?;
        }
    }
    Ok(())
}

pub(crate) fn build_cache_inspect_report(
    plan: &RuntimePlan,
    filter: Option<&str>,
) -> Result<CacheInspectReport> {
    let mut services = Vec::new();
    for service in &plan.ordered_services {
        if let Some(filter_name) = filter
            && service.name != filter_name
        {
            continue;
        }

        let base_artifact = if let ImageSource::Remote(_) = &service.source {
            let base_path =
                base_image_path_for_backend(&plan.cache_dir, service, plan.runtime.backend);
            Some(CacheArtifactInspect {
                path: base_path.clone(),
                artifact_present: base_path.exists(),
                manifest_path: hpc_compose::cache::manifest_path_for(&base_path),
                manifest: load_manifest_if_exists(&base_path)?,
            })
        } else {
            None
        };

        services.push(CacheInspectService {
            service_name: service.name.clone(),
            source_image: source_image_display(&service.source),
            base_registry: match &service.source {
                ImageSource::Remote(remote) => Some(registry_host_for_remote(remote)),
                ImageSource::LocalSqsh(_) | ImageSource::LocalSif(_) | ImageSource::Host => None,
            },
            base_artifact,
            runtime_artifact: build_cache_artifact_inspect(&service.runtime_image)?,
            current_reuse_expectation: runtime_cache_state(service).to_string(),
            note: service.prepare.as_ref().and_then(|prepare| {
                if prepare.force_rebuild {
                    Some(
                        "this service rebuilds on prepare because prepare.mounts are present"
                            .into(),
                    )
                } else {
                    None
                }
            }),
        });
    }

    Ok(CacheInspectReport {
        cache_dir: plan.cache_dir.clone(),
        services,
    })
}

fn build_cache_artifact_inspect(path: &Path) -> Result<CacheArtifactInspect> {
    Ok(CacheArtifactInspect {
        path: path.to_path_buf(),
        artifact_present: path.exists(),
        manifest_path: hpc_compose::cache::manifest_path_for(path),
        manifest: load_manifest_if_exists(path)?,
    })
}

fn write_cache_inspect(writer: &mut impl Write, report: &CacheInspectReport) -> Result<()> {
    for service in &report.services {
        writeln!(writer, "service: {}", service.service_name)?;
        writeln!(writer, "source image: {}", service.source_image)?;

        if let Some(base_artifact) = &service.base_artifact {
            writeln!(writer, "base artifact: {}", base_artifact.path.display())?;
            if let Some(base_registry) = &service.base_registry {
                writeln!(writer, "base registry: {base_registry}")?;
            }
            write_cache_artifact_block(writer, base_artifact)?;
        }

        writeln!(
            writer,
            "runtime artifact: {}",
            service.runtime_artifact.path.display()
        )?;
        write_cache_artifact_block(writer, &service.runtime_artifact)?;
        writeln!(
            writer,
            "current reuse expectation: {}",
            service.current_reuse_expectation
        )?;
        if let Some(note) = &service.note {
            writeln!(writer, "note: {note}")?;
        }
        writeln!(writer)?;
    }
    Ok(())
}

#[cfg(test)]
fn print_manifest_block(path: &Path) -> Result<()> {
    let artifact = build_cache_artifact_inspect(path)?;
    write_cache_artifact_block(&mut io::stdout(), &artifact)
}

fn write_cache_artifact_block(
    writer: &mut impl Write,
    artifact: &CacheArtifactInspect,
) -> Result<()> {
    writeln!(
        writer,
        "artifact present: {}",
        yes_no(artifact.artifact_present)
    )?;
    writeln!(
        writer,
        "manifest path: {}",
        artifact.manifest_path.display()
    )?;
    if let Some(manifest) = &artifact.manifest {
        let kind = match manifest.kind {
            CacheEntryKind::Base => "base",
            CacheEntryKind::Prepared => "prepared",
        };
        writeln!(writer, "manifest kind: {kind}")?;
        writeln!(writer, "manifest cache key: {}", manifest.cache_key)?;
        writeln!(writer, "manifest source: {}", manifest.source_image)?;
        writeln!(
            writer,
            "manifest services: {}",
            manifest.service_names.join(",")
        )?;
        writeln!(writer, "manifest created_at: {}", manifest.created_at)?;
        writeln!(writer, "manifest last_used_at: {}", manifest.last_used_at)?;
        if manifest.kind == CacheEntryKind::Prepared {
            writeln!(
                writer,
                "prepare root: {}",
                manifest.prepare_root.unwrap_or(true)
            )?;
            writeln!(
                writer,
                "prepare commands: {}",
                if manifest.prepare_commands.is_empty() {
                    "0".to_string()
                } else {
                    manifest.prepare_commands.join(" | ")
                }
            )?;
            writeln!(
                writer,
                "force rebuild due to mounts: {}",
                yes_no(manifest.force_rebuild_due_to_mounts)
            )?;
        }
    } else {
        writeln!(writer, "manifest present: no")?;
    }
    Ok(())
}

fn runtime_cache_state(service: &hpc_compose::prepare::RuntimeService) -> &'static str {
    if let Some(prepare) = &service.prepare {
        if prepare.force_rebuild {
            "rebuild on prepare"
        } else if service.runtime_image.exists() {
            "cache hit"
        } else {
            "cache miss"
        }
    } else {
        match &service.source {
            ImageSource::LocalSqsh(path) => {
                if path.exists() {
                    "local image present"
                } else {
                    "local image missing"
                }
            }
            ImageSource::LocalSif(path) => {
                if path.exists() {
                    "local image present"
                } else {
                    "local image missing"
                }
            }
            ImageSource::Remote(_) => {
                if service.runtime_image.exists() {
                    "cache hit"
                } else {
                    "cache miss"
                }
            }
            ImageSource::Host => "host runtime",
        }
    }
}

fn source_image_display(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path.display().to_string(),
        ImageSource::LocalSif(path) => path.display().to_string(),
        ImageSource::Remote(remote) => remote.clone(),
        ImageSource::Host => "host".to_string(),
    }
}

fn hit_or_miss(exists: bool) -> &'static str {
    if exists { "cache hit" } else { "cache miss" }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn display_stats_value(value: &str) -> &str {
    if value.is_empty() { "unknown" } else { value }
}

fn display_optional_stats_value(value: Option<&str>) -> &str {
    match value {
        Some(value) if !value.is_empty() => value,
        _ => "unknown",
    }
}

fn display_optional_f64(value: Option<f64>) -> String {
    format_optional_f64(value).unwrap_or_else(|| "-".to_string())
}

fn format_optional_f64(value: Option<f64>) -> Option<String> {
    value.map(|value| format!("{value:.6}"))
}

fn runtime_presence_label(runtime_present: bool, legacy_present: bool) -> &'static str {
    match (runtime_present, legacy_present) {
        (true, true) => "runtime+legacy",
        (true, false) => "runtime",
        (false, true) => "legacy",
        (false, false) => "missing",
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }
    if unit_index == 0 {
        format!("{bytes} {}", UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
    }
}

fn service_names(plan: &RuntimePlan) -> Vec<&str> {
    plan.ordered_services
        .iter()
        .map(|service| service.name.as_str())
        .collect()
}

pub(crate) fn template_infos() -> Vec<TemplateInfoOutput> {
    templates()
        .iter()
        .map(|template| TemplateInfoOutput {
            name: template.name.to_string(),
            category: template_category(template.name).to_string(),
            description: template.description.to_string(),
        })
        .collect()
}

pub(crate) fn print_template_list() {
    for category in ["basics", "llm", "training", "distributed", "workflow"] {
        let grouped = templates()
            .iter()
            .filter(|template| template_category(template.name) == category)
            .collect::<Vec<_>>();
        if grouped.is_empty() {
            continue;
        }
        println!("{}:", term::styled_section_header(category));
        for template in grouped {
            println!(
                "  {}\t{}",
                term::styled_bold(template.name),
                term::styled_dim(template.description)
            );
        }
        println!();
    }
}

pub(crate) fn build_template_description(template_name: &str) -> Result<TemplateDescriptionOutput> {
    let template = resolve_template(template_name)?;
    Ok(TemplateDescriptionOutput {
        template: TemplateInfoOutput {
            name: template.name.to_string(),
            category: template_category(template.name).to_string(),
            description: template.description.to_string(),
        },
        cache_dir_required: false,
        cache_dir_placeholder: init_cache_dir_placeholder().to_string(),
        command: format!(
            "hpc-compose new --template {} --name my-app --output compose.yaml",
            template.name
        ),
    })
}

pub(crate) fn print_template_description(template_name: &str) -> Result<()> {
    let description = build_template_description(template_name)?;
    println!(
        "{}",
        term::styled_label("template", &description.template.name)
    );
    println!(
        "{}",
        term::styled_label("description", &description.template.description)
    );
    println!(
        "{}",
        term::styled_label(
            "cache dir",
            "optional; omit to use settings/default cache resolution"
        )
    );
    println!(
        "{}",
        term::styled_label("placeholder", &description.cache_dir_placeholder)
    );
    println!("{}:", term::styled_bold("command"));
    println!("{}", term::styled_dim(&description.command));
    Ok(())
}

pub(crate) fn resolve_init_answers(
    template: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    prompt_for_answers: impl FnOnce() -> Result<hpc_compose::init::InitAnswers>,
) -> Result<hpc_compose::init::InitAnswers> {
    if let Some(template_name) = template {
        let template = resolve_template(&template_name)?;
        let cache_dir = match cache_dir {
            Some(cache_dir) if !cache_dir.trim().is_empty() => Some(cache_dir),
            Some(_) => bail!(
                "--cache-dir cannot be empty; choose a path visible from both the login node and the compute nodes"
            ),
            None => None,
        };
        Ok(hpc_compose::init::InitAnswers {
            template_name: template.name.to_string(),
            app_name: match name {
                Some(name) => name,
                None => template.name.to_string(),
            },
            cache_dir,
        })
    } else {
        let mut answers = prompt_for_answers()?;
        if let Some(name) = name {
            answers.app_name = name;
        }
        if let Some(cache_dir) = cache_dir {
            if cache_dir.trim().is_empty() {
                bail!(
                    "--cache-dir cannot be empty; choose a path visible from both the login node and the compute nodes"
                );
            }
            answers.cache_dir = Some(cache_dir);
        }
        Ok(answers)
    }
}

pub(crate) fn print_submit_details(
    plan: &RuntimePlan,
    script_path: &Path,
    sbatch_stdout: &str,
) -> Result<()> {
    println!("rendered script: {}", script_path.display());
    println!("cache dir: {}", plan.cache_dir.display());

    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    if let Some(job_id) = extract_job_id(sbatch_stdout) {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}",
                service.name,
                submit_dir
                    .join(".hpc-compose")
                    .join(job_id)
                    .join("logs")
                    .join(log_file_name_for_service(&service.name))
                    .display()
            );
        }
    } else {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}/.hpc-compose/<job-id>/logs/{}.log",
                service.name,
                submit_dir.display(),
                log_file_name_for_service(&service.name)
            );
        }
    }
    Ok(())
}

pub(crate) fn print_submit_summary_box(
    plan: &RuntimePlan,
    job_id: &str,
    script_path: &Path,
    tracked_metadata_path: Option<&Path>,
) {
    let separator = "\u{2500}".repeat(50);
    println!("{separator}");
    println!(
        " {} Job {} submitted",
        term::styled_success_raw("\u{2713}"),
        term::styled_bold(job_id)
    );
    println!(
        " {} {}",
        term::styled_bold("script:"),
        term::styled_dim(&script_path.display().to_string())
    );
    println!(
        " {} {}",
        term::styled_bold("cache:"),
        term::styled_dim(&plan.cache_dir.display().to_string())
    );
    println!(
        " {} {}",
        term::styled_bold("services:"),
        plan.ordered_services.len()
    );
    if let Some(path) = tracked_metadata_path {
        println!(
            " {} {}",
            term::styled_bold("track:"),
            term::styled_dim(&path.display().to_string())
        );
    }
    println!("{separator}");
}

use hpc_compose::context::ValueSource;

#[derive(Debug, serde::Serialize)]
pub(crate) struct InterpolationVarsOutput {
    pub variables: BTreeMap<String, String>,
    pub sources: BTreeMap<String, String>,
}

pub(crate) fn print_interpolation_vars(
    vars: &BTreeMap<String, String>,
    sources: &BTreeMap<String, ValueSource>,
) {
    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL_CONDENSED);
    table.set_header(vec![
        term::styled_bold("VARIABLE").to_string(),
        term::styled_bold("VALUE").to_string(),
        term::styled_bold("SOURCE").to_string(),
    ]);
    let mut sorted_keys: Vec<&String> = vars.keys().collect();
    sorted_keys.sort();
    for key in sorted_keys {
        let value = vars.get(key).map(|s| s.as_str()).unwrap_or("");
        let source = sources
            .get(key)
            .map(|s| format!("{s:?}").to_lowercase())
            .unwrap_or_else(|| "unknown".to_string());
        table.add_row(vec![
            key.clone(),
            term::styled_dim(value).to_string(),
            source,
        ]);
    }
    println!("{table}");
}

pub(crate) fn extract_job_id(text: &str) -> Option<&str> {
    text.split_whitespace()
        .rev()
        .find(|token| token.chars().all(|ch| ch.is_ascii_digit()))
}

pub(crate) fn print_prune_result(cache_dir: &Path, removed: &[PathBuf]) {
    println!(
        "{}",
        term::styled_label("cache dir", &cache_dir.display().to_string())
    );
    if removed.is_empty() {
        println!("removed: 0");
        return;
    }
    println!(
        "{}",
        term::styled_label("removed", &removed.len().to_string())
    );
    for path in removed {
        println!(
            "{}: {}",
            term::styled_warning("pruned"),
            term::styled_dim(&path.display().to_string())
        );
    }
}

pub(crate) fn cancel_job(job_id: &str, scancel_bin: &str) -> Result<()> {
    let output = Command::new(scancel_bin)
        .arg(job_id)
        .output()
        .context(format!("failed to execute '{scancel_bin}'"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("scancel failed for job {job_id}");
        }
        bail!("scancel failed for job {job_id}: {detail}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        println!("{stdout}");
    }
    println!("cancelled job: {job_id}");
    Ok(())
}

pub(crate) fn finish_watch(
    record: &hpc_compose::job::SubmissionRecord,
    outcome: WatchOutcome,
) -> Result<()> {
    print_watch_final_summary(record, &outcome);
    match outcome {
        WatchOutcome::Completed(_) => Ok(()),
        WatchOutcome::Interrupted(_) => Ok(()),
        WatchOutcome::Unknown(status) => {
            if let Some(detail) = status.detail {
                bail!(
                    "job {} could not be tracked to a terminal scheduler state ({}): {}",
                    record.job_id,
                    status.state,
                    detail
                );
            }
            bail!(
                "job {} could not be tracked to a terminal scheduler state ({})",
                record.job_id,
                status.state
            );
        }
        WatchOutcome::Failed(status) => {
            bail!(
                "job {} finished in scheduler state {}",
                record.job_id,
                status.state
            )
        }
    }
}

fn print_watch_final_summary(record: &hpc_compose::job::SubmissionRecord, outcome: &WatchOutcome) {
    let (label, state) = match outcome {
        WatchOutcome::Completed(status)
        | WatchOutcome::Failed(status)
        | WatchOutcome::Unknown(status)
        | WatchOutcome::Interrupted(status) => {
            (watch_outcome_label(outcome), status.state.as_str())
        }
    };
    println!();
    println!("{}", term::styled_section_header("Watch summary:"));
    println!("  job id: {}", record.job_id);
    println!("  final state: {state} ({label})");
    if let Some(service) = failed_service_hint(record) {
        println!("  failed service: {service}");
    }
    println!(
        "  debug: hpc-compose debug -f {} --job-id {}",
        shell_quote(&record.compose_file.display().to_string()),
        shell_quote(&record.job_id)
    );
    println!(
        "  logs:  hpc-compose logs -f {} --job-id {} --lines 200",
        shell_quote(&record.compose_file.display().to_string()),
        shell_quote(&record.job_id)
    );
    println!(
        "  stats: hpc-compose stats -f {} --job-id {}",
        shell_quote(&record.compose_file.display().to_string()),
        shell_quote(&record.job_id)
    );
}

fn watch_outcome_label(outcome: &WatchOutcome) -> &'static str {
    match outcome {
        WatchOutcome::Completed(_) => "completed",
        WatchOutcome::Failed(_) => "failed",
        WatchOutcome::Unknown(_) => "unknown",
        WatchOutcome::Interrupted(_) => "interrupted",
    }
}

fn failed_service_hint(record: &hpc_compose::job::SubmissionRecord) -> Option<String> {
    let state_path = hpc_compose::job::state_path_for_record(record);
    let raw = fs::read_to_string(state_path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let services = value.get("services")?.as_array()?;
    for service in services {
        let name = service.get("service_name")?.as_str()?;
        let failed_status = service
            .get("status")
            .and_then(|value| value.as_str())
            .is_some_and(|status| status == "failed");
        let failed_exit = service
            .get("last_exit_code")
            .and_then(|value| value.as_i64())
            .is_some_and(|code| code != 0);
        if failed_status || failed_exit {
            return Some(name.to_string());
        }
    }
    None
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn execution_form_label(execution: &ExecutionSpec) -> &'static str {
    match execution {
        ExecutionSpec::ImageDefault => "image-default",
        ExecutionSpec::Shell(_) => "shell",
        ExecutionSpec::Exec(_) => "exec",
    }
}

fn readiness_description(readiness: Option<&hpc_compose::spec::ReadinessSpec>) -> String {
    match readiness {
        None => "none".to_string(),
        Some(hpc_compose::spec::ReadinessSpec::Sleep { seconds }) => {
            format!("sleep {}s", seconds)
        }
        Some(hpc_compose::spec::ReadinessSpec::Tcp {
            host,
            port,
            timeout_seconds,
        }) => format!(
            "tcp {}:{} (timeout {}s)",
            host.as_deref().unwrap_or("127.0.0.1"),
            port,
            timeout_seconds.unwrap_or(60)
        ),
        Some(hpc_compose::spec::ReadinessSpec::Log {
            pattern,
            timeout_seconds,
        }) => format!(
            "log '{}' (timeout {}s)",
            pattern,
            timeout_seconds.unwrap_or(60)
        ),
        Some(hpc_compose::spec::ReadinessSpec::Http {
            url,
            status_code,
            timeout_seconds,
        }) => format!(
            "http {} (status {} timeout {}s)",
            url,
            status_code,
            timeout_seconds.unwrap_or(60)
        ),
    }
}

fn rebuild_reason(service: &hpc_compose::prepare::RuntimeService) -> Option<&'static str> {
    let prepare = service.prepare.as_ref()?;
    if prepare.force_rebuild {
        Some("prepare.mounts are present")
    } else if !service.runtime_image.exists() {
        Some("runtime cache artifact is missing")
    } else {
        None
    }
}

fn format_dependencies(dependencies: &[ServiceDependency]) -> String {
    let mut formatted = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let condition = match dependency.condition {
            DependencyCondition::ServiceStarted => "service_started",
            DependencyCondition::ServiceHealthy => "service_healthy",
            DependencyCondition::ServiceCompletedSuccessfully => "service_completed_successfully",
        };
        formatted.push(format!("{}({condition})", dependency.name));
    }
    formatted.join(",")
}

fn format_age_seconds(seconds: u64) -> String {
    match seconds {
        0..=59 => format!("{seconds}s ago"),
        60..=3599 => format!("{}m ago", seconds / 60),
        3600..=86_399 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

fn format_compact_elapsed(seconds: u64) -> String {
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m{}s", seconds / 60, seconds % 60),
        3600..=86_399 => format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60),
        _ => format!("{}d{}h", seconds / 86_400, (seconds % 86_400) / 3600),
    }
}

#[cfg(test)]
mod tests;
