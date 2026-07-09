use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::ResolvedContext;
use hpc_compose::job::{
    SchedulerOptions, StatsOptions, SubmissionKind, SubmissionRecordBuildOptions,
    build_rightsize_report, build_status_snapshot, parse_log_since_duration,
};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, prepare_runtime_plan_with_reporter};
use hpc_compose::render::{RenderOptions, render_script_with_options};
use hpc_compose::runtime_plan::RuntimePlan;
use hpc_compose::spec::{MetricsCollector, MetricsConfig, parse_slurm_time_limit};
use serde::Serialize;

use super::{
    PrepareFlags, PreparedSlurmSubmission, collect_submit_provenance,
    ensure_batch_submission_supported, latest_record_path, load_discovered_cluster_profile,
    requested_walltime, submit_prepared_slurm_submission, tracked_cached_artifacts,
};
use crate::commands::load;
use crate::output;
use crate::progress::{PrepareProgress, ProgressReporter};

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct GerminateOutput<'a> {
    pub(crate) schema_version: u32,
    compose_file: &'a Path,
    script_path: &'a Path,
    cache_dir: &'a Path,
    dry_run: bool,
    job_id: Option<&'a str>,
    tracked_metadata_path: Option<PathBuf>,
    yaml_patch: Option<String>,
    report: Option<&'a hpc_compose::job::RightsizeReport>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn germinate(
    context: ResolvedContext,
    script_out: Option<PathBuf>,
    canary_time: String,
    metrics_interval: u64,
    timeout: String,
    min_cpus: u32,
    min_mem: String,
    min_gpus: u32,
    flags: PrepareFlags,
    dry_run: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    if metrics_interval == 0 {
        bail!("germinate --metrics-interval must be at least 1");
    }
    if min_cpus == 0 {
        bail!("germinate --min-cpus must be at least 1");
    }
    if min_gpus == 0 {
        bail!("germinate --min-gpus must be at least 1");
    }
    if min_mem.trim().is_empty() {
        bail!("germinate --min-mem must not be empty");
    }
    parse_slurm_time_limit(&canary_time).context("germinate --canary-time is invalid")?;
    let pending_timeout_seconds =
        parse_log_since_duration(&timeout).context("germinate --timeout is invalid")?;

    let file = context.compose_file.value.clone();
    let output_format = output::resolve_output_format(format);
    let effective_config =
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml =
        output::effective_config_yaml(&effective_config, &context.secret_values())?;
    let original_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if original_plan.slurm.array.is_some() {
        bail!("germinate does not support x-slurm.array; submit one representative task instead");
    }
    ensure_batch_submission_supported(&original_plan, false, false)?;

    let canary_plan = minimized_canary_plan(
        &original_plan,
        &canary_time,
        metrics_interval,
        min_cpus,
        &min_mem,
        min_gpus,
    );
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let cluster_profile = load_discovered_cluster_profile(&context)?;

    if !dry_run && !no_preflight {
        let report = progress.run_checked_result(
            "Running canary preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &canary_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
                        fs_probes: false,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before submitting a canary");
        }
    }

    if !dry_run && !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&canary_plan, !quiet && output_format == OutputFormat::Text);
        let summary = prepare_progress.run("Preparing canary runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &canary_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering canary submission script", || {
        render_script_with_options(
            &canary_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                cluster_profile,
                runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                    &context.cwd,
                    canary_plan.slurm.runtime_root.as_deref(),
                )),
                annotate: false,
            },
        )
    })?;
    let script_path = script_out.unwrap_or_else(|| default_canary_script_path(&file));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered canary script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        match output_format {
            OutputFormat::Text => {
                println!("  script: {}", script_path.display());
                println!("  cache:  {}", canary_plan.cache_dir.display());
                println!("dry run: skipping sbatch submission");
            }
            OutputFormat::Json => {
                println!(
                    "{}",
                    crate::output::to_pretty_json(&GerminateOutput {
                        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                        compose_file: &file,
                        script_path: &script_path,
                        cache_dir: &canary_plan.cache_dir,
                        dry_run: true,
                        job_id: None,
                        tracked_metadata_path: None,
                        yaml_patch: None,
                        report: None,
                    })
                    .context("failed to serialize germinate output")?
                );
            }
        }
        return Ok(());
    }

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Canary,
        service_name: None,
        command_override: None,
        requested_walltime: requested_walltime(&canary_plan),
        slurm_array: None,
        sweep: None,
        config_snapshot_yaml: Some(effective_config_yaml),
        cached_artifacts: tracked_cached_artifacts(&canary_plan),
        provenance: collect_submit_provenance(&context.cwd, &canary_plan),
    };
    let prepared = PreparedSlurmSubmission {
        file: file.clone(),
        submit_dir,
        script_path: script_path.clone(),
        runtime_plan: canary_plan.clone(),
        record_options,
        output_format,
    };
    let outcome = submit_prepared_slurm_submission(&context, &prepared, &progress)?;
    let Some((record, persisted)) = outcome.tracked_submission.as_ref() else {
        bail!("sbatch output did not include a numeric Slurm job id; cannot analyze canary usage");
    };
    if !persisted {
        bail!("canary submitted but tracking metadata could not be written; cannot analyze usage");
    }

    wait_for_canary_terminal(
        &file,
        &record.job_id,
        pending_timeout_seconds,
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value.clone(),
            sacct_bin: context.binaries.sacct.value.clone(),
        },
    )?;

    let mut report = build_rightsize_report(
        &original_plan,
        record,
        &StatsOptions {
            sstat_bin: context.binaries.sstat.value.clone(),
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            accounting: true,
        },
    )?;
    suppress_canary_walltime_recommendations(&mut report);
    let yaml_patch = recommendation_yaml_patch(&report);

    match output_format {
        OutputFormat::Text => {
            println!("canary job: {}", record.job_id);
            println!("rendered script: {}", script_path.display());
            println!("tracked metadata: {}", latest_record_path(record).display());
            output::print_rightsize_report(&report)?;
            println!();
            println!("{}", hpc_compose::term::styled_bold("suggested YAML patch"));
            if yaml_patch.trim().is_empty() {
                println!(
                    "No concrete YAML resource changes suggested from the available evidence."
                );
            } else {
                println!("{yaml_patch}");
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&GerminateOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                    compose_file: &file,
                    script_path: &script_path,
                    cache_dir: &canary_plan.cache_dir,
                    dry_run: false,
                    job_id: Some(&record.job_id),
                    tracked_metadata_path: Some(latest_record_path(record)),
                    yaml_patch: Some(yaml_patch),
                    report: Some(&report),
                })
                .context("failed to serialize germinate output")?
            );
        }
    }
    Ok(())
}

fn default_canary_script_path(compose_file: &Path) -> PathBuf {
    let parent = compose_file.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose-canary.sbatch")
}

fn minimized_canary_plan(
    original: &RuntimePlan,
    canary_time: &str,
    metrics_interval: u64,
    min_cpus: u32,
    min_mem: &str,
    min_gpus: u32,
) -> RuntimePlan {
    let mut plan = original.clone();
    plan.name = format!("{}-canary", original.name);
    plan.slurm.time = Some(canary_time.to_string());
    plan.slurm.cpus_per_task = Some(min_cpus);
    plan.slurm.mem = Some(min_mem.to_string());
    if allocation_or_service_requests_gpus(original) {
        if plan.slurm.gpus.is_some() {
            plan.slurm.gpus = Some(min_gpus);
        }
        if plan.slurm.gpus_per_node.is_some() {
            plan.slurm.gpus_per_node = Some(min_gpus);
        }
        if plan.slurm.gpus_per_task.is_some() {
            plan.slurm.gpus_per_task = Some(min_gpus);
        }
        if let Some(gres) = &mut plan.slurm.gres {
            *gres = minimized_gpu_gres(gres, min_gpus);
        }
    }
    plan.slurm.metrics = Some(MetricsConfig {
        enabled: Some(true),
        interval_seconds: Some(metrics_interval),
        collectors: vec![
            MetricsCollector::Gpu,
            MetricsCollector::Slurm,
            MetricsCollector::Cpu,
        ],
    });
    for service in &mut plan.ordered_services {
        if service.slurm.cpus_per_task.is_some() {
            service.slurm.cpus_per_task = Some(min_cpus);
        }
        if service.slurm.gpus.is_some() {
            service.slurm.gpus = Some(min_gpus);
        }
        if service.slurm.gpus_per_node.is_some() {
            service.slurm.gpus_per_node = Some(min_gpus);
        }
        if service.slurm.gpus_per_task.is_some() {
            service.slurm.gpus_per_task = Some(min_gpus);
        }
        if let Some(gres) = &mut service.slurm.gres {
            *gres = minimized_gpu_gres(gres, min_gpus);
        }
    }
    plan
}

fn minimized_gpu_gres(gres: &str, min_gpus: u32) -> String {
    gres.split(',')
        .map(|part| minimized_gpu_gres_part(part.trim(), min_gpus))
        .collect::<Vec<_>>()
        .join(",")
}

fn minimized_gpu_gres_part(part: &str, min_gpus: u32) -> String {
    let mut fields = part.split(':').collect::<Vec<_>>();
    let Some(resource) = fields.first().copied() else {
        return part.to_string();
    };
    if resource != "gpu" && !resource.ends_with("/gpu") {
        return part.to_string();
    }
    if fields
        .last()
        .is_some_and(|last| last.parse::<u32>().is_ok())
    {
        fields.pop();
        fields.push("");
        let mut minimized = fields.join(":");
        minimized.push_str(&min_gpus.to_string());
        return minimized;
    }
    part.to_string()
}

fn allocation_or_service_requests_gpus(plan: &RuntimePlan) -> bool {
    plan.slurm.gpus.unwrap_or(0) > 0
        || plan.slurm.gpus_per_node.unwrap_or(0) > 0
        || plan.slurm.gpus_per_task.unwrap_or(0) > 0
        || plan
            .slurm
            .gres
            .as_deref()
            .is_some_and(|gres| gres.contains("gpu"))
        || plan.ordered_services.iter().any(|service| {
            service.slurm.gpus.unwrap_or(0) > 0
                || service.slurm.gpus_per_node.unwrap_or(0) > 0
                || service.slurm.gpus_per_task.unwrap_or(0) > 0
                || service
                    .slurm
                    .gres
                    .as_deref()
                    .is_some_and(|gres| gres.contains("gpu"))
        })
}

fn wait_for_canary_terminal(
    spec_path: &Path,
    job_id: &str,
    timeout_seconds: u64,
    scheduler: &SchedulerOptions,
) -> Result<()> {
    let started = SystemTime::now();
    loop {
        let snapshot = build_status_snapshot(spec_path, Some(job_id), scheduler)
            .with_context(|| format!("failed to inspect canary job {job_id}"))?;
        if snapshot.scheduler.terminal {
            return Ok(());
        }
        let elapsed = started
            .elapsed()
            .map(|duration| duration.as_secs())
            .unwrap_or(timeout_seconds);
        if elapsed >= timeout_seconds {
            bail!(
                "canary job {job_id} did not reach a terminal scheduler state within {timeout_seconds}s; inspect the queue with `hpc-compose status --job-id {job_id}`"
            );
        }
        thread::sleep(Duration::from_secs(5));
    }
}

fn suppress_canary_walltime_recommendations(report: &mut hpc_compose::job::RightsizeReport) {
    let before = report.recommendations.len();
    report
        .recommendations
        .retain(|recommendation| recommendation.resource != "time");
    if report.recommendations.len() != before {
        report.notes.push(
            "walltime is observed from the canary but not down-sized from a one-minute probe"
                .to_string(),
        );
    }
}

fn recommendation_yaml_patch(report: &hpc_compose::job::RightsizeReport) -> String {
    let mut top_level = BTreeMap::<String, String>::new();
    let mut services = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut unknown = Vec::new();
    for recommendation in &report.recommendations {
        if let Some(key) = recommendation.target_path.strip_prefix("x-slurm.") {
            top_level.insert(key.to_string(), recommendation.suggested.clone());
        } else if let Some(rest) = recommendation.target_path.strip_prefix("services.") {
            if let Some((service, key)) = rest.split_once(".x-slurm.") {
                services
                    .entry(service.to_string())
                    .or_default()
                    .insert(key.to_string(), recommendation.suggested.clone());
            } else {
                unknown.push(recommendation);
            }
        } else {
            unknown.push(recommendation);
        }
    }
    let mut out = String::new();
    if !top_level.is_empty() {
        out.push_str("x-slurm:\n");
        for (key, value) in top_level {
            out.push_str(&format!("  {key}: {value}\n"));
        }
    }
    if !services.is_empty() {
        out.push_str("services:\n");
        for (service, values) in services {
            out.push_str(&format!("  {service}:\n"));
            out.push_str("    x-slurm:\n");
            for (key, value) in values {
                out.push_str(&format!("      {key}: {value}\n"));
            }
        }
    }
    for recommendation in unknown {
        out.push_str(&format!(
            "# {}: {}\n",
            recommendation.target_path, recommendation.suggested
        ));
    }
    out
}
