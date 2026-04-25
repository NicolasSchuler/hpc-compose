use std::collections::BTreeMap;
use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use hpc_compose::cache::{CacheEntryKind, load_manifest_if_exists};
use hpc_compose::cli::{OutputFormat, StatsOutputFormat};
use hpc_compose::cluster::ClusterProfile;
use hpc_compose::init::{
    cache_dir_placeholder as init_cache_dir_placeholder, resolve_template, template_category,
    templates,
};
use hpc_compose::job::{
    ArtifactExportReport, CleanupReport, JobInventoryScan, PsSnapshot, StatsSnapshot,
    StatusSnapshot, SubmissionBackend, WatchOutcome, scheduler_source_label,
};
use hpc_compose::planner::{
    ExecutionSpec, ImageSource, Plan, ServicePlacementMode, build_plan, registry_host_for_remote,
};
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
    pub(crate) cache_dir: String,
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
    build_plan(path, spec)
}

pub(crate) fn load_plan_with_interpolation_vars(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<Plan> {
    let spec = ComposeSpec::load_with_interpolation_vars(path, vars)?;
    build_plan(path, spec)
}

#[cfg(test)]
pub(crate) fn load_runtime_plan(path: &Path) -> Result<RuntimePlan> {
    let plan = load_plan(path)?;
    Ok(build_runtime_plan(&plan))
}

pub(crate) fn load_runtime_plan_with_interpolation_vars(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<RuntimePlan> {
    let plan = load_plan_with_interpolation_vars(path, vars)?;
    Ok(build_runtime_plan(&plan))
}

pub(crate) fn load_effective_config_with_interpolation_vars(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<EffectiveComposeConfig> {
    let spec = ComposeSpec::load_with_interpolation_vars(path, vars)?;
    let plan = build_plan(path, spec.clone())?;
    let normalized_policies = plan
        .ordered_services
        .iter()
        .map(|service| (service.name.clone(), service.failure_policy.clone()))
        .collect::<BTreeMap<_, _>>();
    spec.effective_config(&plan.cache_dir, &normalized_policies)
}

pub(crate) fn effective_config_yaml(config: &EffectiveComposeConfig) -> Result<String> {
    serde_norway::to_string(config).context("failed to serialize effective config as yaml")
}

pub(crate) fn load_plan_and_runtime_with_interpolation_vars(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<(Plan, RuntimePlan)> {
    let plan = load_plan_with_interpolation_vars(path, vars)?;
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

pub(crate) fn write_stats_snapshot_csv(
    writer: &mut impl Write,
    snapshot: &StatsSnapshot,
) -> io::Result<()> {
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
        }),
    )?;
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
            "effective srun args: {}",
            display_srun_command_for_backend(runtime, runtime_plan.runtime.backend).join(" ")
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
        cache_dir_required: true,
        cache_dir_placeholder: init_cache_dir_placeholder().to_string(),
        command: format!(
            "hpc-compose new --template {} --name my-app --cache-dir '{}' --output compose.yaml",
            template.name,
            init_cache_dir_placeholder()
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
            "required; choose a path visible from both the login node and the compute nodes"
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
            Some(cache_dir) if !cache_dir.trim().is_empty() => cache_dir,
            Some(_) => bail!(
                "--cache-dir cannot be empty; choose a path visible from both the login node and the compute nodes"
            ),
            None => bail!(
                "--cache-dir is required when using --template; choose a path visible from both the login node and the compute nodes"
            ),
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
            answers.cache_dir = cache_dir;
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

pub(crate) fn finish_watch(job_id: &str, outcome: WatchOutcome) -> Result<()> {
    match outcome {
        WatchOutcome::Completed(_) => Ok(()),
        WatchOutcome::Interrupted(_) => Ok(()),
        WatchOutcome::Unknown(status) => {
            if let Some(detail) = status.detail {
                bail!(
                    "job {} could not be tracked to a terminal scheduler state ({}): {}",
                    job_id,
                    status.state,
                    detail
                );
            }
            bail!(
                "job {} could not be tracked to a terminal scheduler state ({})",
                job_id,
                status.state
            );
        }
        WatchOutcome::Failed(status) => {
            bail!(
                "job {} finished in scheduler state {}",
                job_id,
                status.state
            )
        }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use super::*;
    use crate::commands::run_command;
    use hpc_compose::cache::{CacheEntryKind, CacheEntryManifest};
    use hpc_compose::cli::{CacheCommands, Commands, WatchMode};
    use hpc_compose::job::{
        ArtifactExportReport, ArtifactManifest, BatchLogStatus, CleanupJobReport, CleanupReport,
        CollectorStatus, GpuDeviceSample, GpuProcessSample, GpuSnapshot, JobInventoryEntry,
        JobInventoryScan, QueueDiagnostics, SamplerSnapshot, SchedulerSource, SchedulerStatus,
        ServiceLogStatus, StatsSnapshot, StatusSnapshot, StepStats, SubmissionKind,
        SubmissionRecord,
    };
    use hpc_compose::planner::{ExecutionSpec, ImageSource, PreparedImageSpec, ServicePlacement};
    use hpc_compose::spec::{
        DependencyCondition, ReadinessSpec, ServiceDependency, ServiceFailurePolicy,
        ServiceSlurmConfig, SlurmConfig,
    };

    fn runtime_service(
        source: ImageSource,
        runtime_image: PathBuf,
        prepare: Option<PreparedImageSpec>,
    ) -> hpc_compose::prepare::RuntimeService {
        hpc_compose::prepare::RuntimeService {
            name: "svc/name".into(),
            runtime_image,
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare,
            source,
        }
    }

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).expect("write script");
        let mut perms = fs::metadata(path).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("fake-enroot.sh");
        write_script(
            &path,
            r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name) name="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start) exit 0 ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove) exit 0 ;;
esac
"#,
        );
        path
    }

    fn write_fake_sbatch(tmpdir: &Path, success: bool) -> PathBuf {
        let path = tmpdir.join(if success { "sbatch-ok" } else { "sbatch-fail" });
        let body = if success {
            "#!/bin/bash\nset -euo pipefail\necho 'Submitted batch job 54321'\n"
        } else {
            "#!/bin/bash\nset -euo pipefail\necho 'boom' >&2\nexit 2\n"
        };
        write_script(&path, body);
        path
    }

    fn write_fake_srun(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("srun");
        write_script(
            &path,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage --container-image'; fi\n",
        );
        path
    }

    fn write_compose(tmpdir: &Path, body: &str) -> PathBuf {
        let path = tmpdir.join("compose.yaml");
        fs::write(&path, body).expect("compose");
        path
    }

    fn safe_cache_dir() -> tempfile::TempDir {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
        fs::create_dir_all(&root).expect("cache root");
        tempfile::Builder::new()
            .prefix("case-")
            .tempdir_in(root)
            .expect("cache tempdir")
    }

    fn write_valid_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
        fs::create_dir_all(tmpdir.join("app")).expect("app");
        fs::write(tmpdir.join("app/main.py"), "print('hi')\n").expect("main.py");
        write_compose(
            tmpdir,
            &format!(
                r#"
name: demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install click
"#,
                cache_dir.display()
            ),
        )
    }

    fn submission_record(tmpdir: &Path, plan: &RuntimePlan, job_id: &str) -> SubmissionRecord {
        hpc_compose::job::build_submission_record(
            &tmpdir.join("compose.yaml"),
            tmpdir,
            &tmpdir.join("job.sbatch"),
            plan,
            job_id,
        )
        .expect("record")
    }

    fn sample_step() -> StepStats {
        let mut alloc_tres_map = BTreeMap::new();
        alloc_tres_map.insert("gres/gpu".into(), "1".into());
        let mut usage_tres_map = BTreeMap::new();
        usage_tres_map.insert("gres/gpuutil".into(), "87".into());
        usage_tres_map.insert("gres/gpumem".into(), "4096M".into());
        StepStats {
            step_id: "12345.0".into(),
            ntasks: "1".into(),
            ave_cpu: "00:00:03".into(),
            ave_rss: "128M".into(),
            max_rss: "256M".into(),
            alloc_tres: "cpu=1,gres/gpu=1".into(),
            tres_usage_in_ave: "cpu=00:00:03,gres/gpuutil=87,gres/gpumem=4096M".into(),
            alloc_tres_map,
            usage_tres_in_ave_map: usage_tres_map,
            gpu_count: Some("1".into()),
            gpu_util: Some("87".into()),
            gpu_mem: Some("4096M".into()),
        }
    }

    fn sample_service_status(path: PathBuf) -> ServiceLogStatus {
        ServiceLogStatus {
            service_name: "svc/name".into(),
            path,
            present: false,
            updated_at: None,
            updated_age_seconds: None,
            log_path: None,
            step_name: None,
            launch_index: None,
            launcher_pid: None,
            healthy: None,
            completed_successfully: None,
            readiness_configured: None,
            status: None,
            failure_policy_mode: None,
            restart_count: None,
            max_restarts: None,
            window_seconds: None,
            max_restarts_in_window: None,
            restart_failures_in_window: None,
            last_exit_code: None,
            placement_mode: None,
            nodes: None,
            ntasks: None,
            ntasks_per_node: None,
            nodelist: None,
        }
    }

    #[test]
    fn action_and_label_helpers_cover_all_variants() {
        assert_eq!(action_label(ArtifactAction::Present), "OK");
        assert_eq!(action_label(ArtifactAction::Reused), "REUSE");
        assert_eq!(action_label(ArtifactAction::Built), "BUILD");
        assert_eq!(artifact_role_label("base"), "cache artifact");
        assert_eq!(artifact_role_label("runtime"), "artifact");
        assert_eq!(artifact_role_label("other"), "artifact");
        assert_eq!(hit_or_miss(true), "cache hit");
        assert_eq!(hit_or_miss(false), "cache miss");
        assert_eq!(yes_no(true), "yes");
        assert_eq!(yes_no(false), "no");
    }

    #[test]
    fn sanitize_and_extract_job_id_work() {
        assert_eq!(
            log_file_name_for_service("svc/name.with spaces"),
            "svc_x2f_name_x2e_with_x20_spaces.log"
        );
        assert_eq!(extract_job_id("Submitted batch job 12345"), Some("12345"));
        assert_eq!(extract_job_id("no job id here"), None);
    }

    #[test]
    fn finish_watch_requires_a_terminal_scheduler_result() {
        finish_watch(
            "12345",
            WatchOutcome::Completed(hpc_compose::job::SchedulerStatus {
                state: "COMPLETED".into(),
                source: hpc_compose::job::SchedulerSource::Sacct,
                terminal: true,
                failed: false,
                detail: None,
            }),
        )
        .expect("completed watch");

        let err = finish_watch(
            "12345",
            WatchOutcome::Unknown(hpc_compose::job::SchedulerStatus {
                state: "unknown".into(),
                source: hpc_compose::job::SchedulerSource::LocalOnly,
                terminal: false,
                failed: false,
                detail: Some("scheduler tools were unavailable".into()),
            }),
        )
        .expect_err("unknown watch should fail");
        assert!(err.to_string().contains("could not be tracked"));
        assert!(err.to_string().contains("scheduler tools were unavailable"));
    }

    #[test]
    fn runtime_cache_state_covers_prepare_and_local_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        let remote_sqsh = tmpdir.path().join("remote.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        std::fs::write(&remote_sqsh, "x").expect("remote");

        let with_forced_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        assert_eq!(
            runtime_cache_state(&with_forced_prepare),
            "rebuild on prepare"
        );

        let with_cached_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&with_cached_prepare), "cache hit");

        let missing_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("prepared-missing.sqsh"),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&missing_prepare), "cache miss");

        let local_present = runtime_service(
            ImageSource::LocalSqsh(local_sqsh.clone()),
            local_sqsh.clone(),
            None,
        );
        assert_eq!(runtime_cache_state(&local_present), "local image present");

        let local_missing = runtime_service(
            ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
            tmpdir.path().join("missing.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&local_missing), "local image missing");

        let remote_missing = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("missing-remote.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&remote_missing), "cache miss");
    }

    #[test]
    fn service_names_collect_in_order() {
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![
                runtime_service(
                    ImageSource::Remote("docker://redis:7".into()),
                    PathBuf::from("/cache/a.sqsh"),
                    None,
                ),
                hpc_compose::prepare::RuntimeService {
                    name: "worker".into(),
                    ..runtime_service(
                        ImageSource::Remote("docker://python:3.11-slim".into()),
                        PathBuf::from("/cache/b.sqsh"),
                        None,
                    )
                },
            ],
        };
        assert_eq!(service_names(&plan), vec!["svc/name", "worker"]);
    }

    #[test]
    fn path_helpers_return_expected_locations() {
        let path = PathBuf::from("/tmp/project/compose.yaml");
        assert_eq!(
            default_script_path(&path),
            PathBuf::from("/tmp/project/hpc-compose.sbatch")
        );
        assert_eq!(
            default_script_path(Path::new("compose.yaml")),
            PathBuf::from("hpc-compose.sbatch")
        );
        assert!(default_cache_dir().ends_with(".cache/hpc-compose"));
        let err =
            render_from_path(Path::new("/definitely/missing/compose.yaml")).expect_err("missing");
        assert!(err.to_string().contains("/definitely/missing/compose.yaml"));
    }

    #[test]
    fn print_helpers_cover_manifest_and_summary_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("prepared.sqsh");
        std::fs::write(&runtime_image, "x").expect("runtime");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        let manifest = CacheEntryManifest {
            kind: CacheEntryKind::Prepared,
            artifact_path: runtime_image.display().to_string(),
            service_names: vec!["svc/name".into()],
            cache_key: "key".into(),
            source_image: "docker://redis:7".into(),
            registry: Some("registry-1.docker.io".into()),
            prepare_commands: Vec::new(),
            prepare_env: Vec::new(),
            prepare_root: Some(true),
            prepare_mounts: Vec::new(),
            force_rebuild_due_to_mounts: false,
            created_at: 1,
            last_used_at: 1,
            tool_version: "0.1.0".into(),
        };
        let manifest_path = hpc_compose::cache::manifest_path_for(&runtime_image);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");

        let service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service.clone()],
        };
        let local_plan = RuntimePlan {
            name: "local-demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![runtime_service(
                ImageSource::LocalSqsh(local_sqsh.clone()),
                local_sqsh,
                None,
            )],
        };

        print_report(&Report { items: Vec::new() }, false);
        print_report(
            &Report {
                items: vec![hpc_compose::preflight::Item {
                    level: hpc_compose::preflight::Level::Warn,
                    message: "warn".into(),
                    remediation: None,
                }],
            },
            false,
        );
        print_prepare_summary(&PrepareSummary {
            services: vec![hpc_compose::prepare::ServicePrepareResult {
                service_name: service.name.clone(),
                base_image: Some(hpc_compose::prepare::ArtifactStatus {
                    path: tmpdir.path().join("base.sqsh"),
                    action: ArtifactAction::Built,
                    note: None,
                }),
                runtime_image: hpc_compose::prepare::ArtifactStatus {
                    path: runtime_image.clone(),
                    action: ArtifactAction::Reused,
                    note: Some("cached".into()),
                },
            }],
        });
        print_plan_inspect(&plan).expect("print plan inspect");
        print_plan_inspect(&local_plan).expect("print local plan inspect");
        print_cache_inspect(&build_cache_inspect_report(&plan, None).expect("inspect report"))
            .expect("inspect");
        print_cache_inspect(
            &build_cache_inspect_report(&plan, Some("other")).expect("inspect filtered report"),
        )
        .expect("inspect filtered");
        print_manifest_block(&runtime_image).expect("manifest block");
        print_manifest_block(&tmpdir.path().join("missing.sqsh")).expect("missing manifest block");
        print_prune_result(tmpdir.path(), &[]);
        print_prune_result(tmpdir.path(), std::slice::from_ref(&runtime_image));
        print_submit_details(&plan, Path::new("/tmp/job.sbatch"), "no job id")
            .expect("submit details");
        print_submit_details(
            &plan,
            Path::new("/tmp/job.sbatch"),
            "Submitted batch job 99999",
        )
        .expect("submit details with job id");
        assert_eq!(source_image_display(&service.source), "docker://redis:7");
        assert_eq!(
            source_image_display(&ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh"))),
            "/tmp/local.sqsh"
        );
    }

    #[test]
    fn writer_helpers_cover_status_stats_artifacts_and_verbose_inspect() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("prepared.sqsh");
        fs::write(&runtime_image, "x").expect("runtime");
        let mut service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image,
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        service.environment = vec![("TOKEN".into(), "secret".into())];
        service.volumes = vec!["./app:/workspace".into()];
        service.working_dir = Some("/workspace".into());
        service.readiness = Some(ReadinessSpec::Http {
            url: "http://127.0.0.1:8000/health".into(),
            status_code: 200,
            timeout_seconds: Some(30),
        });
        service.depends_on = vec![ServiceDependency {
            name: "db".into(),
            condition: DependencyCondition::ServiceHealthy,
        }];

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service.clone()],
        };
        let record = submission_record(tmpdir.path(), &plan, "12345");
        let status = StatusSnapshot {
            record: record.clone(),
            scheduler: SchedulerStatus {
                state: "COMPLETED".into(),
                source: SchedulerSource::Sacct,
                terminal: true,
                failed: false,
                detail: Some("finished".into()),
            },
            queue_diagnostics: Some(QueueDiagnostics {
                pending_reason: None,
                eligible_time: Some("2026-04-06T10:00:00".into()),
                start_time: Some("2026-04-06T10:05:00".into()),
            }),
            log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
            batch_log: BatchLogStatus {
                path: tmpdir.path().join("slurm-12345.out"),
                present: true,
                updated_at: Some(1),
                updated_age_seconds: Some(70),
            },
            services: vec![ServiceLogStatus {
                failure_policy_mode: Some("restart_on_failure".into()),
                restart_count: Some(1),
                max_restarts: Some(3),
                window_seconds: Some(60),
                max_restarts_in_window: Some(3),
                restart_failures_in_window: Some(1),
                last_exit_code: Some(0),
                placement_mode: Some("distributed".into()),
                nodes: Some(2),
                ntasks: Some(4),
                ntasks_per_node: Some(2),
                nodelist: Some("node01 node02".into()),
                step_name: Some("hpc-compose:svc_name".into()),
                launcher_pid: Some(4242),
                healthy: Some(true),
                completed_successfully: Some(false),
                readiness_configured: Some(true),
                status: Some("ready".into()),
                ..sample_service_status(tmpdir.path().join(".hpc-compose/12345/logs/svc.log"))
            }],
            attempt: Some(1),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/runs/demo")),
        };
        let mut status_out = Vec::new();
        write_status_snapshot(&mut status_out, &status).expect("status");
        let status_text = String::from_utf8(status_out).expect("utf8");
        assert!(status_text.contains("Scheduler:"));
        assert!(status_text.contains("  state: COMPLETED (sacct)"));
        assert!(status_text.contains("  note: finished"));
        assert!(status_text.contains("  eligible time: 2026-04-06T10:00:00"));
        assert!(status_text.contains("  start time: 2026-04-06T10:05:00"));
        assert!(status_text.contains("Runtime:"));
        assert!(status_text.contains("  attempt: 1"));
        assert!(status_text.contains("  is resume: yes"));
        assert!(status_text.contains("  resume dir: /shared/runs/demo"));
        assert!(status_text.contains("updated: 1m ago"));
        assert!(status_text.contains("updated: unknown"));
        assert!(status_text.contains(
            "  state service 'svc/name': failure_policy=restart_on_failure restarts=1/3 window=1/3@60s last_exit=0"
        ));
        assert!(status_text.contains(
            "  placement service 'svc/name': mode=distributed nodes=2 ntasks=4 ntasks_per_node=2 nodelist=node01 node02"
        ));

        let waiting = StatusSnapshot {
            record: record.clone(),
            scheduler: SchedulerStatus {
                state: "WAITING_FOR_ACCOUNTING".into(),
                source: SchedulerSource::LocalOnly,
                terminal: false,
                failed: false,
                detail: Some(
                    "job just disappeared from squeue and has not appeared in sacct yet".into(),
                ),
            },
            queue_diagnostics: None,
            log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
            batch_log: BatchLogStatus {
                path: tmpdir.path().join("slurm-12345.out"),
                present: false,
                updated_at: None,
                updated_age_seconds: None,
            },
            services: Vec::new(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
        };
        let mut waiting_out = Vec::new();
        write_status_snapshot(&mut waiting_out, &waiting).expect("waiting");
        let waiting_text = String::from_utf8(waiting_out).expect("utf8");
        assert!(waiting_text.contains("  state: WAITING_FOR_ACCOUNTING (local-only)"));
        assert!(waiting_text.contains(
            "  note: job just disappeared from squeue and has not appeared in sacct yet"
        ));
        assert!(!waiting_text.contains("pending reason:"));
        assert!(!waiting_text.contains("eligible time:"));
        assert!(!waiting_text.contains("start time:"));

        let stats = StatsSnapshot {
            job_id: "12345".into(),
            record: Some(record.clone()),
            metrics_dir: Some(tmpdir.path().join(".hpc-compose/12345/metrics")),
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: Some("visible".into()),
            },
            available: true,
            reason: Some("ignored once available".into()),
            source: "sampler+sstat".into(),
            notes: vec!["note one".into()],
            sampler: Some(SamplerSnapshot {
                interval_seconds: 5,
                collectors: vec![
                    CollectorStatus {
                        name: "gpu".into(),
                        enabled: true,
                        available: true,
                        note: None,
                        last_sampled_at: Some("2026-04-05T10:00:10Z".into()),
                    },
                    CollectorStatus {
                        name: "slurm".into(),
                        enabled: false,
                        available: false,
                        note: None,
                        last_sampled_at: None,
                    },
                ],
                gpu: Some(GpuSnapshot {
                    sampled_at: "2026-04-05T10:00:10Z".into(),
                    nodes: vec![hpc_compose::job::GpuNodeSummary {
                        node: Some("node01".into()),
                        gpu_count: 1,
                        avg_utilization_gpu: Some(87.0),
                        memory_used_mib: Some(4096),
                        memory_total_mib: Some(8192),
                    }],
                    gpus: vec![GpuDeviceSample {
                        node: Some("node01".into()),
                        rank: None,
                        local_rank: None,
                        service: None,
                        collector: Some("nvidia-smi".into()),
                        index: Some("0".into()),
                        uuid: Some("GPU-0".into()),
                        name: Some("A100".into()),
                        utilization_gpu: Some("87".into()),
                        utilization_memory: Some("73".into()),
                        memory_used_mib: Some("4096".into()),
                        memory_total_mib: Some("8192".into()),
                        temperature_c: Some("55".into()),
                        power_draw_w: Some("220".into()),
                        power_limit_w: Some("300".into()),
                    }],
                    processes: vec![GpuProcessSample {
                        node: Some("node01".into()),
                        rank: None,
                        local_rank: None,
                        service: None,
                        collector: Some("nvidia-smi".into()),
                        gpu_uuid: Some("GPU-0".into()),
                        pid: Some("4242".into()),
                        process_name: Some("python".into()),
                        used_memory_mib: Some("2048".into()),
                    }],
                }),
                slurm: None,
            }),
            steps: vec![sample_step()],
            first_failure: Some(hpc_compose::job::FirstFailure {
                service: "trainer".into(),
                exit_code: 42,
                at_unix: Some(1_774_000_000),
                node: Some("node01".into()),
                rank: None,
            }),
            attempt: Some(1),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/runs/demo")),
        };
        let mut stats_out = Vec::new();
        write_stats_snapshot(&mut stats_out, &stats).expect("stats");
        let stats_text = String::from_utf8(stats_out).expect("utf8");
        assert!(stats_text.contains("collector 'gpu': available"));
        assert!(stats_text.contains("attempt: 1"));
        assert!(stats_text.contains("is resume: yes"));
        assert!(stats_text.contains("resume dir: /shared/runs/demo"));
        assert!(!stats_text.contains("collector 'slurm'"));
        assert!(stats_text.contains("gpu snapshot: 2026-04-05T10:00:10Z"));
        assert!(stats_text.contains("gpu node node01"));
        assert!(stats_text.contains("first failure: service=trainer"));
        assert!(stats_text.contains("gpu process: pid=4242"));
        assert!(stats_text.contains("gpu count: 1"));

        let mut csv_out = Vec::new();
        write_stats_snapshot_csv(&mut csv_out, &stats).expect("csv");
        let csv_text = String::from_utf8(csv_out).expect("utf8");
        assert!(csv_text.contains("job_id,scheduler_state,scheduler_source,stats_source"));
        assert!(csv_text.contains("\"12345\",\"RUNNING\",\"squeue\",\"sampler+sstat\""));
        assert!(csv_text.contains("\"12345.0\""));

        let mut jsonl_out = Vec::new();
        write_stats_snapshot_jsonl(&mut jsonl_out, &stats).expect("jsonl");
        let jsonl_text = String::from_utf8(jsonl_out).expect("utf8");
        assert!(jsonl_text.contains("\"record_type\":\"summary\""));
        assert!(jsonl_text.contains("\"record_type\":\"collector\""));
        assert!(jsonl_text.contains("\"record_type\":\"gpu_device\""));
        assert!(jsonl_text.contains("\"record_type\":\"gpu_process\""));
        assert!(jsonl_text.contains("\"record_type\":\"step\""));
        assert!(jsonl_text.contains("\"attempt\":1"));
        assert!(jsonl_text.contains("\"is_resume\":true"));

        let unavailable_stats = StatsSnapshot {
            available: false,
            sampler: None,
            steps: Vec::new(),
            first_failure: None,
            source: "sstat".into(),
            notes: Vec::new(),
            reason: Some("job is pending".into()),
            metrics_dir: None,
            record: None,
            job_id: "12345".into(),
            scheduler: SchedulerStatus {
                state: "PENDING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: None,
            },
            attempt: None,
            is_resume: None,
            resume_dir: None,
        };
        let mut unavailable_out = Vec::new();
        write_stats_snapshot(&mut unavailable_out, &unavailable_stats).expect("stats");
        let unavailable_text = String::from_utf8(unavailable_out).expect("utf8");
        assert!(unavailable_text.contains("stats reason: job is pending"));
        assert!(!unavailable_text.contains("step: "));

        let mut unavailable_csv = Vec::new();
        write_stats_snapshot_csv(&mut unavailable_csv, &unavailable_stats).expect("csv");
        assert_eq!(
            String::from_utf8(unavailable_csv).expect("utf8"),
            "job_id,scheduler_state,scheduler_source,stats_source,step_id,ntasks,ave_cpu,ave_rss,max_rss,alloc_tres,tres_usage_in_ave,gpu_count,gpu_util,gpu_mem,alloc_tres_map,usage_tres_in_ave_map\n"
        );

        let report = ArtifactExportReport {
            record: record.clone(),
            manifest_path: tmpdir.path().join("manifest.json"),
            payload_dir: tmpdir.path().join("payload"),
            export_dir: tmpdir.path().join("results"),
            manifest: ArtifactManifest {
                schema_version: 2,
                job_id: "12345".into(),
                collect_policy: "always".into(),
                collected_at: "2026-04-05T10:00:00Z".into(),
                job_outcome: "success".into(),
                attempt: Some(1),
                is_resume: Some(true),
                resume_dir: Some(PathBuf::from("/shared/runs/demo")),
                declared_source_patterns: vec!["/x/**".into()],
                matched_source_paths: vec!["/x/a".into()],
                copied_relative_paths: vec!["a".into()],
                warnings: Vec::new(),
                bundles: BTreeMap::from([(
                    "default".into(),
                    hpc_compose::job::ArtifactBundleManifest {
                        declared_source_patterns: vec!["/x/**".into()],
                        matched_source_paths: vec!["/x/a".into()],
                        copied_relative_paths: vec!["a".into()],
                        warnings: Vec::new(),
                    },
                )]),
            },
            selected_bundles: vec!["default".into()],
            bundles: Vec::new(),
            exported_paths: vec![tmpdir.path().join("results/a")],
            tarball_paths: Vec::new(),
            warnings: vec!["missing optional path".into()],
        };
        let mut report_out = Vec::new();
        write_artifact_export_report(&mut report_out, &report).expect("artifacts");
        let report_text = String::from_utf8(report_out).expect("utf8");
        assert!(report_text.contains("collect policy: always"));
        assert!(report_text.contains("attempt: 1"));
        assert!(report_text.contains("is resume: yes"));
        assert!(report_text.contains("resume dir: /shared/runs/demo"));
        assert!(report_text.contains("warning: missing optional path"));
        assert!(report_text.contains("exported: "));

        let plan_model = hpc_compose::planner::Plan {
            spec_path: tmpdir.path().join("compose.yaml"),
            project_dir: tmpdir.path().to_path_buf(),
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: hpc_compose::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![hpc_compose::planner::PlannedService {
                name: service.name.clone(),
                image: service.source.clone(),
                execution: service.execution.clone(),
                environment: service.environment.clone(),
                volumes: service.volumes.clone(),
                working_dir: service.working_dir.clone(),
                depends_on: service.depends_on.clone(),
                readiness: service.readiness.clone(),
                failure_policy: service.failure_policy.clone(),
                placement: service.placement.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
            }],
        };
        let mut inspect_out = Vec::new();
        write_plan_inspect_verbose(&mut inspect_out, &plan_model, &plan, None).expect("inspect");
        let inspect_text = String::from_utf8(inspect_out).expect("utf8");
        assert!(inspect_text.contains("execution form: shell"));
        assert!(inspect_text.contains("depends_on: db(service_healthy)"));
        assert!(
            inspect_text
                .contains("readiness: http http://127.0.0.1:8000/health (status 200 timeout 30s)")
        );
        assert!(inspect_text.contains("rebuild reason: prepare.mounts are present"));
    }

    #[test]
    fn helper_functions_cover_remaining_formatting_paths() {
        assert_eq!(display_stats_value(""), "unknown");
        assert_eq!(display_stats_value("5"), "5");
        assert_eq!(display_optional_stats_value(None), "unknown");
        assert_eq!(display_optional_stats_value(Some("")), "unknown");
        assert_eq!(display_optional_stats_value(Some("x")), "x");
        assert_eq!(
            execution_form_label(&ExecutionSpec::ImageDefault),
            "image-default"
        );
        assert_eq!(
            execution_form_label(&ExecutionSpec::Shell("echo".into())),
            "shell"
        );
        assert_eq!(
            execution_form_label(&ExecutionSpec::Exec(vec!["echo".into()])),
            "exec"
        );
        assert_eq!(readiness_description(None), "none");
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Sleep { seconds: 5 })),
            "sleep 5s"
        );
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Tcp {
                host: None,
                port: 5432,
                timeout_seconds: None,
            })),
            "tcp 127.0.0.1:5432 (timeout 60s)"
        );
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: Some(9),
            })),
            "log 'ready' (timeout 9s)"
        );
        assert_eq!(format_age_seconds(59), "59s ago");
        assert_eq!(format_age_seconds(61), "1m ago");
        assert_eq!(format_age_seconds(7_200), "2h ago");
        assert_eq!(format_age_seconds(172_800), "2d ago");
        assert_eq!(
            format_dependencies(&[
                ServiceDependency {
                    name: "db".into(),
                    condition: DependencyCondition::ServiceStarted,
                },
                ServiceDependency {
                    name: "cache".into(),
                    condition: DependencyCondition::ServiceHealthy,
                },
            ]),
            "db(service_started),cache(service_healthy)"
        );

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("runtime.sqsh");
        let service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(
            rebuild_reason(&service),
            Some("runtime cache artifact is missing")
        );
        fs::write(&runtime_image, "x").expect("runtime");
        assert_eq!(rebuild_reason(&service), None);
    }

    #[test]
    fn resolve_init_answers_and_cancel_job_cover_remaining_paths() {
        let err = resolve_init_answers(Some("dev-python-app".into()), None, None, || {
            unreachable!("template path should not prompt")
        })
        .expect_err("missing required cache dir");
        assert!(err.to_string().contains("--cache-dir is required"));

        let answers = resolve_init_answers(
            Some("dev-python-app".into()),
            None,
            Some("/cache".into()),
            || unreachable!("template path should not prompt"),
        )
        .expect("template answers");
        assert_eq!(answers.app_name, "dev-python-app");
        assert_eq!(answers.cache_dir, "/cache");

        let err = resolve_init_answers(
            Some("dev-python-app".into()),
            None,
            Some("   ".into()),
            || unreachable!("template path should not prompt"),
        )
        .expect_err("blank cache dir");
        assert!(err.to_string().contains("--cache-dir cannot be empty"));

        let prompted =
            resolve_init_answers(None, Some("override".into()), Some("/cache".into()), || {
                Ok(hpc_compose::init::InitAnswers {
                    template_name: "app-redis-worker".into(),
                    app_name: "prompted".into(),
                    cache_dir: "/default".into(),
                })
            })
            .expect("prompted");
        assert_eq!(prompted.app_name, "override");
        assert_eq!(prompted.cache_dir, "/cache");

        let err = resolve_init_answers(None, None, Some("   ".into()), || {
            Ok(hpc_compose::init::InitAnswers {
                template_name: "app-redis-worker".into(),
                app_name: "prompted".into(),
                cache_dir: "/default".into(),
            })
        })
        .expect_err("blank prompted override");
        assert!(err.to_string().contains("--cache-dir cannot be empty"));

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let empty_fail = tmpdir.path().join("scancel-empty");
        write_script(&empty_fail, "#!/bin/bash\nset -euo pipefail\nexit 1\n");
        let err = cancel_job("42", empty_fail.to_str().expect("path")).expect_err("empty fail");
        assert_eq!(err.to_string(), "scancel failed for job 42");

        let stderr_fail = tmpdir.path().join("scancel-stderr");
        write_script(
            &stderr_fail,
            "#!/bin/bash\nset -euo pipefail\necho boom >&2\nexit 1\n",
        );
        let err = cancel_job("42", stderr_fail.to_str().expect("path")).expect_err("stderr fail");
        assert!(err.to_string().contains("scancel failed for job 42: boom"));

        let err = cancel_job(
            "42",
            tmpdir.path().join("missing-bin").to_str().expect("path"),
        )
        .expect_err("missing binary");
        assert!(err.to_string().contains("failed to execute"));
    }

    #[test]
    fn stdout_entrypoints_cover_public_output_wrappers() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache_root = safe_cache_dir();
        let cache_dir = cache_root.path().to_path_buf();
        let compose = write_valid_compose(tmpdir.path(), &cache_dir);
        let plan = load_plan(&compose).expect("plan");
        let runtime = build_runtime_plan(&plan);
        let record = submission_record(tmpdir.path(), &runtime, "12345");

        let status = StatusSnapshot {
            record: record.clone(),
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: Some("visible".into()),
            },
            queue_diagnostics: Some(QueueDiagnostics {
                pending_reason: None,
                eligible_time: Some("2026-04-06T10:00:00".into()),
                start_time: Some("2026-04-06T10:05:00".into()),
            }),
            log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
            batch_log: BatchLogStatus {
                path: tmpdir.path().join("slurm-12345.out"),
                present: false,
                updated_at: None,
                updated_age_seconds: None,
            },
            services: Vec::new(),
            attempt: Some(1),
            is_resume: Some(false),
            resume_dir: None,
        };

        let stats = StatsSnapshot {
            job_id: "12345".into(),
            record: Some(record.clone()),
            metrics_dir: Some(tmpdir.path().join(".hpc-compose/12345/metrics")),
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: None,
            },
            available: true,
            reason: None,
            source: "sstat".into(),
            notes: Vec::new(),
            sampler: None,
            steps: vec![sample_step()],
            first_failure: None,
            attempt: Some(1),
            is_resume: Some(false),
            resume_dir: None,
        };

        let artifact_report = ArtifactExportReport {
            record: record.clone(),
            manifest_path: tmpdir.path().join("manifest.json"),
            payload_dir: tmpdir.path().join("payload"),
            export_dir: tmpdir.path().join("results"),
            manifest: ArtifactManifest {
                schema_version: 2,
                job_id: "12345".into(),
                collect_policy: "always".into(),
                collected_at: "2026-04-05T10:00:00Z".into(),
                job_outcome: "success".into(),
                attempt: Some(1),
                is_resume: Some(false),
                resume_dir: None,
                declared_source_patterns: vec!["/x/**".into()],
                matched_source_paths: vec!["/x/a".into()],
                copied_relative_paths: vec!["a".into()],
                warnings: Vec::new(),
                bundles: BTreeMap::new(),
            },
            selected_bundles: vec!["default".into()],
            bundles: Vec::new(),
            exported_paths: vec![tmpdir.path().join("results/a")],
            tarball_paths: Vec::new(),
            warnings: Vec::new(),
        };

        let inventory = JobInventoryEntry {
            compose_file: compose.clone(),
            compose_metadata_root: tmpdir.path().join(".hpc-compose"),
            job_id: "12345".into(),
            kind: SubmissionKind::Main,
            is_latest: true,
            submitted_at: 1_775_807_600,
            age_seconds: 42,
            submit_dir: tmpdir.path().to_path_buf(),
            record_path: tmpdir.path().join(".hpc-compose/jobs/12345.json"),
            runtime_job_root: tmpdir.path().join(".hpc-compose/12345"),
            runtime_job_root_present: true,
            legacy_runtime_job_root: tmpdir.path().join(".hpc-compose/legacy/12345"),
            legacy_runtime_job_root_present: false,
            disk_usage_bytes: Some(2_048),
        };
        let scan = JobInventoryScan {
            scan_root: tmpdir.path().to_path_buf(),
            jobs: vec![inventory.clone()],
        };
        let cleanup = CleanupReport {
            compose_file: compose,
            mode: "age".into(),
            dry_run: true,
            removed_job_ids: vec!["12345".into()],
            kept_job_ids: vec!["67890".into()],
            latest_pointer_job_id_before: Some("12345".into()),
            latest_job_id_before: Some("12345".into()),
            latest_job_id_after: Some("67890".into()),
            total_bytes_reclaimed: Some(2_048),
            jobs: vec![CleanupJobReport {
                inventory,
                selected: true,
                bytes_reclaimed: Some(2_048),
                removable_paths: vec![tmpdir.path().join(".hpc-compose/jobs/12345.json")],
            }],
        };

        assert_eq!(
            resolve_stats_output_format(None, false),
            StatsOutputFormat::Text
        );
        assert_eq!(
            resolve_stats_output_format(Some(StatsOutputFormat::Csv), false),
            StatsOutputFormat::Csv
        );
        assert_eq!(
            resolve_stats_output_format(Some(StatsOutputFormat::Text), true),
            StatsOutputFormat::Json
        );

        print_status_snapshot(&status).expect("print status snapshot");
        print_stats_snapshot(&stats).expect("print stats snapshot");
        print_artifact_export_report(&artifact_report).expect("print artifact export report");
        print_plan_inspect_verbose(&plan, &runtime).expect("print verbose inspect");
        print_job_inventory_scan(&scan, true).expect("print job inventory");
        print_cleanup_report(&cleanup, true).expect("print cleanup report");
        print_template_list();
        print_template_description("dev-python-app").expect("template description");
    }

    #[test]
    fn run_command_covers_success_and_error_arms() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache_root = safe_cache_dir();
        let cache_dir = cache_root.path().to_path_buf();
        let compose = write_valid_compose(tmpdir.path(), &cache_dir);
        let enroot = write_fake_enroot(tmpdir.path());
        let srun = write_fake_srun(tmpdir.path());
        let sbatch_ok = write_fake_sbatch(tmpdir.path(), true);
        let sbatch_fail = write_fake_sbatch(tmpdir.path(), false);
        let empty_cache = tmpdir.path().join("empty-cache");
        fs::create_dir_all(&empty_cache).expect("empty cache");
        let no_id_sbatch = tmpdir.path().join("sbatch-no-id");
        write_script(
            &no_id_sbatch,
            "#!/bin/bash\nset -euo pipefail\necho 'submitted without id'\n",
        );
        let scancel_ok = tmpdir.path().join("scancel-ok");
        write_script(
            &scancel_ok,
            "#!/bin/bash\nset -euo pipefail\necho 'cancel ok'\n",
        );
        let scancel_fail = tmpdir.path().join("scancel-fail");
        write_script(
            &scancel_fail,
            "#!/bin/bash\nset -euo pipefail\necho 'denied' >&2\nexit 1\n",
        );

        run_command(Commands::Validate {
            file: Some(compose.clone()),
            strict_env: false,
            format: None,
        })
        .expect("validate");
        run_command(Commands::Render {
            file: Some(compose.clone()),
            output: None,
            format: None,
        })
        .expect("render stdout");
        let rendered = tmpdir.path().join("rendered.sbatch");
        run_command(Commands::Render {
            file: Some(compose.clone()),
            output: Some(rendered.clone()),
            format: None,
        })
        .expect("render file");
        assert!(rendered.exists());
        let render_err = run_command(Commands::Render {
            file: Some(compose.clone()),
            output: Some(tmpdir.path().join("missing-parent/rendered.sbatch")),
            format: None,
        })
        .expect_err("render write failure");
        assert!(
            render_err
                .to_string()
                .contains("failed to write rendered script")
        );

        run_command(Commands::Prepare {
            file: Some(compose.clone()),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            keep_failed_prep: false,
            force: true,
            format: None,
        })
        .expect("prepare");

        let err = run_command(Commands::Preflight {
            file: Some(compose.clone()),
            strict: true,
            verbose: false,
            format: None,
            json: false,
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
            scontrol_bin: "scontrol".into(),
        })
        .expect_err("strict warnings");
        assert!(err.to_string().contains("preflight reported warnings"));
        run_command(Commands::Preflight {
            file: Some(compose.clone()),
            strict: false,
            verbose: false,
            format: None,
            json: false,
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
            scontrol_bin: "scontrol".into(),
        })
        .expect("non-strict preflight");

        run_command(Commands::Inspect {
            file: Some(compose.clone()),
            verbose: false,
            tree: false,
            format: None,
            json: false,
        })
        .expect("inspect");

        let err = run_command(Commands::Up {
            file: Some(compose.clone()),
            script_out: None,
            sbatch_bin: sbatch_fail.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
            local: false,
            allow_resume_changes: false,
            resume_diff_only: false,
            dry_run: false,
            detach: true,
            watch_mode: WatchMode::Auto,
            no_tui: false,
            format: None,
        })
        .expect_err("sbatch fail");
        assert!(err.to_string().contains("sbatch failed"));

        run_command(Commands::Up {
            file: Some(compose.clone()),
            script_out: Some(tmpdir.path().join("submit.sbatch")),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: false,
            local: false,
            allow_resume_changes: false,
            resume_diff_only: false,
            dry_run: false,
            detach: true,
            watch_mode: WatchMode::Auto,
            no_tui: false,
            format: None,
        })
        .expect("submit");
        run_command(Commands::Up {
            file: Some(compose.clone()),
            script_out: Some(tmpdir.path().join("submit-no-id.sbatch")),
            sbatch_bin: no_id_sbatch.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
            local: false,
            allow_resume_changes: false,
            resume_diff_only: false,
            dry_run: false,
            detach: true,
            watch_mode: WatchMode::Auto,
            no_tui: false,
            format: None,
        })
        .expect("submit without id");

        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(cache_dir.clone()),
                format: None,
            },
        })
        .expect("cache list");
        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(empty_cache),
                format: None,
            },
        })
        .expect("cache list empty");
        run_command(Commands::Cache {
            command: CacheCommands::Inspect {
                file: Some(compose.clone()),
                service: Some("app".into()),
                format: None,
            },
        })
        .expect("cache inspect");
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir.clone()),
                age: None,
                all_unused: true,
                format: None,
            },
        })
        .expect_err("missing file");
        assert!(err.to_string().contains("--all-unused requires -f/--file"));
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(compose.clone()),
                cache_dir: Some(cache_dir.clone()),
                age: Some(7),
                all_unused: true,
                format: None,
            },
        })
        .expect_err("conflicting strategies");
        assert!(
            err.to_string()
                .contains("cache prune accepts only one strategy at a time")
        );
        run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir),
                age: Some(999),
                all_unused: false,
                format: None,
            },
        })
        .expect("prune age");
        run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(compose.clone()),
                cache_dir: None,
                age: None,
                all_unused: true,
                format: None,
            },
        })
        .expect("prune all unused");

        run_command(Commands::Cancel {
            file: Some(compose.clone()),
            job_id: Some("12345".into()),
            scancel_bin: scancel_ok.display().to_string(),
            purge_cache: false,
            format: None,
        })
        .expect("cancel ok");
        let cancel_err = run_command(Commands::Cancel {
            file: Some(compose.clone()),
            job_id: Some("12345".into()),
            scancel_bin: scancel_fail.display().to_string(),
            purge_cache: false,
            format: None,
        })
        .expect_err("cancel fail");
        assert!(
            cancel_err
                .to_string()
                .contains("scancel failed for job 12345")
        );

        let init_output = tmpdir.path().join("init-compose.yaml");
        run_command(Commands::New {
            template: Some("dev-python-app".into()),
            list_templates: false,
            describe_template: None,
            name: Some("custom-init".into()),
            cache_dir: Some("/tmp/custom-cache".into()),
            output: init_output.clone(),
            force: true,
            format: None,
        })
        .expect("init");
        assert!(init_output.exists());
    }

    #[test]
    fn write_status_snapshot_omits_window_for_non_restart_or_legacy_state() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![runtime_service(
                ImageSource::Remote("docker://redis:7".into()),
                tmpdir.path().join("prepared.sqsh"),
                None,
            )],
        };
        let record = submission_record(tmpdir.path(), &plan, "12345");
        let status = StatusSnapshot {
            record,
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: None,
            },
            queue_diagnostics: None,
            log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
            batch_log: BatchLogStatus {
                path: tmpdir.path().join("slurm-12345.out"),
                present: true,
                updated_at: Some(1),
                updated_age_seconds: Some(1),
            },
            services: vec![
                ServiceLogStatus {
                    service_name: "ignore".into(),
                    failure_policy_mode: Some("ignore".into()),
                    restart_count: Some(0),
                    max_restarts: Some(0),
                    window_seconds: Some(0),
                    max_restarts_in_window: Some(0),
                    restart_failures_in_window: Some(0),
                    last_exit_code: Some(42),
                    placement_mode: None,
                    nodes: None,
                    ntasks: None,
                    ntasks_per_node: None,
                    nodelist: None,
                    status: Some("failed".into()),
                    present: true,
                    updated_at: Some(1),
                    updated_age_seconds: Some(1),
                    ..sample_service_status(
                        tmpdir.path().join(".hpc-compose/12345/logs/ignore.log"),
                    )
                },
                ServiceLogStatus {
                    service_name: "legacy".into(),
                    failure_policy_mode: Some("restart_on_failure".into()),
                    restart_count: Some(1),
                    max_restarts: Some(3),
                    window_seconds: None,
                    max_restarts_in_window: None,
                    restart_failures_in_window: None,
                    last_exit_code: Some(17),
                    placement_mode: None,
                    nodes: None,
                    ntasks: None,
                    ntasks_per_node: None,
                    nodelist: None,
                    status: Some("failed".into()),
                    present: true,
                    updated_at: Some(1),
                    updated_age_seconds: Some(1),
                    path: tmpdir.path().join(".hpc-compose/12345/logs/legacy.log"),
                    log_path: None,
                    step_name: None,
                    launch_index: None,
                    launcher_pid: None,
                    healthy: None,
                    completed_successfully: None,
                    readiness_configured: None,
                },
            ],
            attempt: None,
            is_resume: None,
            resume_dir: None,
        };
        let mut status_out = Vec::new();
        write_status_snapshot(&mut status_out, &status).expect("status");
        let status_text = String::from_utf8(status_out).expect("utf8");
        assert!(
            status_text.contains(
                "  state service 'ignore': failure_policy=ignore restarts=0/0 last_exit=42"
            )
        );
        assert!(status_text.contains(
            "  state service 'legacy': failure_policy=restart_on_failure restarts=1/3 last_exit=17"
        ));
        assert!(!status_text.contains("window=0/0@0s"));
        assert!(!status_text.contains("window=unknown/unknown@unknowns"));
    }

    #[test]
    fn inspect_tree_preserves_indentation_for_root_descendants() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_compose(
            tmpdir.path(),
            r#"
name: demo
x-slurm:
  cache_dir: ./cache
services:
  root:
    image: redis:7
    command: /bin/true
  child:
    image: redis:7
    command: /bin/true
    depends_on:
      root:
        condition: service_started
  grandchild:
    image: redis:7
    command: /bin/true
    depends_on:
      child:
        condition: service_started
"#,
        );
        let plan = load_plan(&compose).expect("plan");
        let runtime_plan = build_runtime_plan(&plan);
        let mut out = Vec::new();
        write_plan_inspect_tree(&mut out, &plan, &runtime_plan).expect("tree");
        let text = String::from_utf8(out).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();

        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("    └── ") && line.contains("child")),
            "{text}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("        └── ") && line.contains("grandchild")),
            "{text}"
        );
    }
}
