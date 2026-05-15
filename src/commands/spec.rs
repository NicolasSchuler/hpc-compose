use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{DependencyOutputFormat, OutputFormat};
use hpc_compose::cluster::{discover_cluster_profile_path, load_cluster_profile};
use hpc_compose::context::{ResolvedContext, ResolvedValue, ValueSource};
use hpc_compose::job::{
    SchedulerOptions, StatsOptions, build_rightsize_report, jobs_dir_for, load_submission_record,
    metadata_root_for,
};
use hpc_compose::lint::{LintFinding, LintLevel};
use hpc_compose::planner::ImageSource;
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, RuntimePlan, build_runtime_plan, prepare_runtime_plan};
use hpc_compose::render::{RenderOptions, render_script_with_options};
use hpc_compose::spec::{missing_defaulted_variables, referenced_variables};
use hpc_compose::term;
use serde::Serialize;

use crate::output::{common as output_common, spec as output_spec};
use crate::progress::{PrepareProgress, ProgressReporter};

pub(crate) fn validate(
    context: ResolvedContext,
    strict_env: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let plan =
        output_common::load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let runtime_plan = build_runtime_plan(&plan);
    let cluster_warnings = load_discovered_cluster_profile(&context)?
        .map(|profile| {
            profile
                .validate_runtime_plan(&runtime_plan)
                .into_iter()
                .map(|warning| warning.message)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if strict_env {
        let missing =
            missing_defaulted_variables(&context.compose_file.value, &context.interpolation_vars)?;
        if !missing.is_empty() {
            bail!(
                "strict env validation failed; missing variables consumed default fallbacks: {}",
                missing.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
    }
    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("{}", term::styled_success("spec is valid"));
            for warning in &cluster_warnings {
                eprintln!("{} {warning}", term::styled_warning("WARN"));
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output_spec::build_validate_output(
                    &plan,
                    cluster_warnings
                ))
                .context("failed to serialize validate output")?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct LintOutput {
    passed: bool,
    compose_file: PathBuf,
    warning_count: usize,
    error_count: usize,
    findings: Vec<LintFinding>,
}

pub(crate) fn lint(
    context: ResolvedContext,
    strict_env: bool,
    allow_warnings: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (plan, runtime_plan) =
        output_common::load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if strict_env {
        let missing =
            missing_defaulted_variables(&context.compose_file.value, &context.interpolation_vars)?;
        if !missing.is_empty() {
            bail!(
                "strict env validation failed; missing variables consumed default fallbacks: {}",
                missing.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
    }
    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let findings = hpc_compose::lint::lint_plan(&plan, &runtime_plan, cluster_profile.as_ref());
    let warning_count = findings
        .iter()
        .filter(|finding| finding.level == LintLevel::Warning)
        .count();
    let error_count = findings
        .iter()
        .filter(|finding| finding.level == LintLevel::Error)
        .count();
    let passed = error_count == 0 && (allow_warnings || warning_count == 0);

    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => print_lint_findings(&findings, passed),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&LintOutput {
                    passed,
                    compose_file: plan.spec_path,
                    warning_count,
                    error_count,
                    findings,
                })
                .context("failed to serialize lint output")?
            );
        }
    }

    if !passed {
        bail!(
            "lint found {warning_count} warning(s) and {error_count} error(s); pass --allow-warnings to allow warnings"
        );
    }
    Ok(())
}

fn print_lint_findings(findings: &[LintFinding], passed: bool) {
    if findings.is_empty() {
        println!("{}", term::styled_success("spec passed lint"));
        return;
    }
    for finding in findings {
        let level = match finding.level {
            LintLevel::Warning => term::styled_warning("WARN"),
            LintLevel::Error => term::styled_error("ERROR"),
        };
        if let Some(service) = finding.service.as_deref() {
            println!(
                "{} {} service={}: {}",
                level, finding.code, service, finding.message
            );
        } else {
            println!("{} {}: {}", level, finding.code, finding.message);
        }
        if let Some(field) = finding.field.as_deref() {
            println!("  field: {field}");
        }
        if let Some(recommendation) = finding.recommendation.as_deref() {
            println!("  recommendation: {recommendation}");
        }
    }
    if passed {
        println!(
            "{}",
            term::styled_success("lint passed with allowed warnings")
        );
    }
}

pub(crate) fn render(
    context: ResolvedContext,
    output_path: Option<PathBuf>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let plan =
        output_common::load_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let runtime_plan = build_runtime_plan(&plan);
    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let script = render_script_with_options(
        &runtime_plan,
        &RenderOptions {
            apptainer_bin: context.binaries.apptainer.value.clone(),
            singularity_bin: context.binaries.singularity.value.clone(),
            cluster_profile,
        },
    )?;
    if let Some(path) = output_path.as_ref() {
        fs::write(path, &script)
            .with_context(|| format!("failed to write rendered script to {}", path.display()))?;
    }
    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            if let Some(path) = output_path {
                println!("{}", path.display());
            } else {
                print!("{script}");
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output_spec::RenderOutput {
                    compose_file: plan.spec_path,
                    output_path,
                    script,
                })
                .context("failed to serialize render output")?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct PlanOutput {
    valid: bool,
    compose_file: PathBuf,
    runtime_plan: hpc_compose::prepare::RuntimePlan,
    cluster_warnings: Vec<String>,
    explanations: Vec<PlanHint>,
    script: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PlanHint {
    level: &'static str,
    message: String,
}

pub(crate) fn plan(
    context: ResolvedContext,
    strict_env: bool,
    verbose: bool,
    tree: bool,
    show_script: bool,
    explain: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (plan, runtime_plan) =
        output_common::load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if strict_env {
        let missing =
            missing_defaulted_variables(&context.compose_file.value, &context.interpolation_vars)?;
        if !missing.is_empty() {
            bail!(
                "strict env validation failed; missing variables consumed default fallbacks: {}",
                missing.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
    }

    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let cluster_warnings = cluster_profile
        .as_ref()
        .map(|profile| {
            profile
                .validate_runtime_plan(&runtime_plan)
                .into_iter()
                .map(|warning| warning.message)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let script = if show_script {
        Some(render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                cluster_profile: cluster_profile.clone(),
            },
        )?)
    } else {
        None
    };
    let explanations = build_plan_hints(&runtime_plan, &cluster_warnings);

    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("{}", term::styled_success("spec is valid"));
            for warning in &cluster_warnings {
                eprintln!("{} {warning}", term::styled_warning("WARN"));
            }
            if tree {
                output_spec::print_plan_inspect_tree(&plan, &runtime_plan)
                    .context("failed to write tree output")?;
            } else if verbose {
                output_spec::print_plan_inspect_verbose_with_profile(
                    &plan,
                    &runtime_plan,
                    cluster_profile.as_ref(),
                )
                .context("failed to write plan output")?;
            } else {
                output_spec::print_plan_inspect(&runtime_plan)
                    .context("failed to write plan output")?;
            }
            if let Some(script) = script.as_deref() {
                println!();
                println!("{}", term::styled_section_header("Rendered script:"));
                print!("{script}");
            }
            if explain {
                print_plan_hints(&explanations);
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&PlanOutput {
                    valid: true,
                    compose_file: plan.spec_path,
                    runtime_plan,
                    cluster_warnings,
                    explanations,
                    script,
                })
                .context("failed to serialize plan output")?
            );
        }
    }
    Ok(())
}

fn build_plan_hints(runtime_plan: &RuntimePlan, cluster_warnings: &[String]) -> Vec<PlanHint> {
    let mut hints = Vec::new();
    for warning in cluster_warnings {
        hints.push(PlanHint {
            level: "warn",
            message: format!("cluster profile warning: {warning}"),
        });
    }
    if cache_looks_home_local(&runtime_plan.cache_dir) {
        hints.push(PlanHint {
            level: "warn",
            message: format!(
                "cache directory '{}' appears to be under HOME; use shared storage if compute nodes cannot see this path",
                runtime_plan.cache_dir.display()
            ),
        });
    }
    if runtime_plan.slurm.resume_dir().is_some() {
        hints.push(PlanHint {
            level: "info",
            message: "resume is configured; hpc-compose will compare the effective config with the previous tracked submission".to_string(),
        });
    }
    if runtime_plan.slurm.artifacts.is_some() {
        hints.push(PlanHint {
            level: "info",
            message: "artifact collection is configured; use `hpc-compose artifacts` after the run to export bundles".to_string(),
        });
    }
    for service in &runtime_plan.ordered_services {
        if matches!(service.source, ImageSource::Remote(_)) && !service.runtime_image.exists() {
            hints.push(PlanHint {
                level: "info",
                message: format!(
                    "service '{}' will import or prepare a missing runtime artifact during prepare",
                    service.name
                ),
            });
        }
        if service
            .prepare
            .as_ref()
            .is_some_and(|prepare| prepare.force_rebuild)
        {
            hints.push(PlanHint {
                level: "info",
                message: format!(
                    "service '{}' rebuilds on prepare because prepare.mounts are present",
                    service.name
                ),
            });
        }
        if matches!(&service.source, ImageSource::Remote(image) if image.contains("docker.io") || !image.contains('/'))
        {
            hints.push(PlanHint {
                level: "info",
                message: format!(
                    "service '{}' pulls from Docker Hub; anonymous pulls may be rate-limited",
                    service.name
                ),
            });
        }
    }
    hints.push(PlanHint {
        level: "next",
        message: "next command: hpc-compose up -f <compose.yaml>".to_string(),
    });
    hints
}

fn cache_looks_home_local(path: &std::path::Path) -> bool {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .is_some_and(|home| path.starts_with(home))
}

fn print_plan_hints(hints: &[PlanHint]) {
    if hints.is_empty() {
        return;
    }
    println!();
    println!("{}", term::styled_section_header("Plan hints:"));
    for hint in hints {
        let label = match hint.level {
            "warn" => term::styled_warning("warn"),
            "next" => term::styled_success("next"),
            _ => term::styled_dim(hint.level),
        };
        println!("- {label}: {}", hint.message);
    }
}

pub(crate) fn prepare(
    context: ResolvedContext,
    keep_failed_prep: bool,
    force: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let output_format = output_common::resolve_output_format(format, false);
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        &context.compose_file.value,
        &context.interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    let prepare_progress =
        PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
    let summary = progress.run_result("Preparing runtime artifacts", || {
        prepare_runtime_plan(
            &runtime_plan,
            &PrepareOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                keep_failed_prep,
                force_rebuild: force,
            },
        )
    })?;
    prepare_progress.finish_from_summary(&summary);
    match output_format {
        OutputFormat::Text if !quiet => output_spec::print_prepare_summary(&summary),
        OutputFormat::Text => {}
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .context("failed to serialize prepare output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn preflight(
    context: ResolvedContext,
    strict: bool,
    verbose: bool,
    format: Option<OutputFormat>,
    json: bool,
    quiet: bool,
) -> Result<()> {
    let output_format = output_common::resolve_output_format(format, json);
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);
    let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        &context.compose_file.value,
        &context.interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let report = progress.run_checked_result(
        "Running preflight checks",
        || {
            Ok::<_, anyhow::Error>(run_preflight(
                &runtime_plan,
                &PreflightOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    sbatch_bin: context.binaries.sbatch.value.clone(),
                    srun_bin: context.binaries.srun.value.clone(),
                    scontrol_bin: context.binaries.scontrol.value.clone(),
                    require_submit_tools: true,
                    skip_prepare: false,
                    cluster_profile,
                },
            ))
        },
        |report| report.has_errors(),
    )?;
    match output_format {
        OutputFormat::Text
            if !quiet || report.has_errors() || (strict && report.has_warnings()) =>
        {
            output_spec::print_report(&report, verbose)
        }
        OutputFormat::Text => {}
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.grouped())
                    .context("failed to serialize preflight report")?
            );
        }
    }
    if report.has_errors() {
        bail!("preflight failed");
    }
    if strict && report.has_warnings() {
        bail!("preflight reported warnings");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn inspect(
    context: ResolvedContext,
    verbose: bool,
    tree: bool,
    rightsize: bool,
    dependencies: bool,
    dependencies_format: DependencyOutputFormat,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    json: bool,
) -> Result<()> {
    let (plan, runtime_plan) =
        output_common::load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if rightsize {
        let record = load_submission_record(&context.compose_file.value, job_id.as_deref())
            .with_context(|| {
                if let Some(job_id) = job_id.as_deref() {
                    format!(
                        "inspect --rightsize requires tracked submission metadata for job {job_id}; run hpc-compose up --detach -f {} first",
                        context.compose_file.value.display()
                    )
                } else {
                    format!(
                        "inspect --rightsize requires tracked submission metadata; run hpc-compose up --detach -f {} first",
                        context.compose_file.value.display()
                    )
                }
            })?;
        let report = build_rightsize_report(
            &runtime_plan,
            &record,
            &StatsOptions {
                scheduler: SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
                sstat_bin: context.binaries.sstat.value.clone(),
                accounting: false,
            },
        )?;
        match output_common::resolve_output_format(format, json) {
            OutputFormat::Text => output_spec::print_rightsize_report(&report)
                .context("failed to write rightsize output")?,
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .context("failed to serialize rightsize output")?
                );
            }
        }
        return Ok(());
    }
    if dependencies {
        let output_format = output_common::resolve_output_format(format, json);
        if output_format == OutputFormat::Json && dependencies_format == DependencyOutputFormat::Dot
        {
            bail!(
                "inspect --dependencies --format json cannot be combined with --dependencies-format dot"
            );
        }
        let graph = crate::output::build_dependency_graph(&plan, &runtime_plan);
        match output_format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&graph)
                        .context("failed to serialize dependency graph output")?
                );
            }
            OutputFormat::Text => match dependencies_format {
                DependencyOutputFormat::Text => crate::output::print_dependency_graph_text(&graph)
                    .context("failed to write dependency graph output")?,
                DependencyOutputFormat::Dot => crate::output::print_dependency_graph_dot(&graph)
                    .context("failed to write dependency graph DOT output")?,
            },
        }
        return Ok(());
    }
    match output_common::resolve_output_format(format, json) {
        OutputFormat::Text => {
            if tree {
                output_spec::print_plan_inspect_tree(&plan, &runtime_plan)
                    .context("failed to write tree output")?;
            } else if verbose {
                let cluster_profile = load_discovered_cluster_profile(&context)?;
                if cluster_profile.is_some() {
                    output_spec::print_plan_inspect_verbose_with_profile(
                        &plan,
                        &runtime_plan,
                        cluster_profile.as_ref(),
                    )
                    .context("failed to write inspect output")?;
                } else {
                    output_spec::print_plan_inspect_verbose(&plan, &runtime_plan)
                        .context("failed to write inspect output")?;
                }
            } else {
                output_spec::print_plan_inspect(&runtime_plan)
                    .context("failed to write inspect output")?;
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&runtime_plan)
                    .context("failed to serialize inspect output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn config(
    context: ResolvedContext,
    format: Option<OutputFormat>,
    variables: bool,
    show_values: bool,
) -> Result<()> {
    let config = output_common::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
        &context.compose_file.value,
        &context.interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    let output_format = output_common::resolve_output_format(format, false);
    if variables {
        let referenced =
            referenced_variables(&context.compose_file.value, &context.interpolation_vars)?;
        let (vars, sources) = scoped_interpolation_vars(
            &context.interpolation_vars,
            &context.interpolation_var_sources,
            &referenced,
            show_values,
        );
        match output_format {
            OutputFormat::Text => output_spec::print_interpolation_vars(&vars, &sources),
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output_spec::InterpolationVarsOutput {
                        variables: vars,
                        sources: sources
                            .iter()
                            .map(|(k, v)| (k.clone(), format!("{v:?}").to_lowercase()))
                            .collect(),
                    })
                    .context("failed to serialize variables output")?
                );
            }
        }
        return Ok(());
    }
    match output_format {
        OutputFormat::Text => {
            print!("{}", output_common::effective_config_yaml(&config)?);
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&config)
                    .context("failed to serialize config output")?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct ContextRuntimePaths {
    compose_dir: PathBuf,
    current_submit_dir: PathBuf,
    default_script_path: PathBuf,
    runtime_job_root_pattern: String,
    cache_dir: Option<ResolvedValue<PathBuf>>,
    resume_dir: Option<ResolvedValue<PathBuf>>,
    artifact_export_dir: Option<ResolvedValue<String>>,
    metadata_root: ResolvedValue<PathBuf>,
    jobs_dir: ResolvedValue<PathBuf>,
}

#[derive(Debug, Serialize)]
struct ContextOutput {
    cwd: PathBuf,
    settings_path: Option<PathBuf>,
    settings_base_dir: Option<PathBuf>,
    selected_profile: Option<String>,
    compose_file: ResolvedValue<PathBuf>,
    binaries: hpc_compose::context::ResolvedBinaries,
    interpolation_vars: std::collections::BTreeMap<String, String>,
    interpolation_var_sources: std::collections::BTreeMap<String, ValueSource>,
    compose_load_error: Option<String>,
    runtime_paths: ContextRuntimePaths,
}

pub(crate) fn context(
    context: ResolvedContext,
    format: Option<OutputFormat>,
    show_values: bool,
) -> Result<()> {
    let compose_dir = context
        .compose_file
        .value
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();
    let current_submit_dir = context.cwd.clone();
    let (cache_dir, resume_dir, artifact_export_dir, compose_load_error) =
        match output_common::load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        ) {
            Ok((plan, runtime_plan)) => (
                Some(ResolvedValue {
                    value: runtime_plan.cache_dir.clone(),
                    source: if plan.slurm.cache_dir.is_some() {
                        ValueSource::Compose
                    } else {
                        context.cache_dir.source
                    },
                }),
                runtime_plan.slurm.resume_dir().map(|value| ResolvedValue {
                    value: PathBuf::from(value),
                    source: ValueSource::Compose,
                }),
                runtime_plan
                    .slurm
                    .artifacts
                    .as_ref()
                    .and_then(|artifacts| artifacts.export_dir.clone())
                    .map(|value| ResolvedValue {
                        value,
                        source: ValueSource::Compose,
                    }),
                None,
            ),
            Err(err) => (None, None, None, Some(format!("{err:#}"))),
        };
    let runtime_paths = ContextRuntimePaths {
        compose_dir: compose_dir.clone(),
        current_submit_dir: current_submit_dir.clone(),
        default_script_path: output_common::default_script_path(&context.compose_file.value),
        runtime_job_root_pattern: current_submit_dir
            .join(".hpc-compose")
            .join("{job_id}")
            .display()
            .to_string(),
        cache_dir,
        resume_dir,
        artifact_export_dir,
        metadata_root: ResolvedValue {
            value: metadata_root_for(&context.compose_file.value),
            source: ValueSource::Builtin,
        },
        jobs_dir: ResolvedValue {
            value: jobs_dir_for(&context.compose_file.value),
            source: ValueSource::Builtin,
        },
    };
    let referenced = referenced_variables(&context.compose_file.value, &context.interpolation_vars)
        .unwrap_or_default();
    let (interpolation_vars, interpolation_var_sources) = scoped_interpolation_vars(
        &context.interpolation_vars,
        &context.interpolation_var_sources,
        &referenced,
        show_values,
    );
    let output = ContextOutput {
        cwd: context.cwd,
        settings_path: context.settings_path,
        settings_base_dir: context.settings_base_dir,
        selected_profile: context.selected_profile,
        compose_file: context.compose_file,
        binaries: context.binaries,
        interpolation_vars,
        interpolation_var_sources,
        compose_load_error,
        runtime_paths,
    };

    match output_common::resolve_output_format(format, false) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize context output")?
            );
        }
        OutputFormat::Text => {
            println!(
                "{}",
                term::styled_label("cwd", &output.cwd.display().to_string())
            );
            println!(
                "{}",
                term::styled_label(
                    "settings file",
                    &output
                        .settings_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "<none>".to_string())
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "settings base dir",
                    &output
                        .settings_base_dir
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "<none>".to_string())
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "profile",
                    output
                        .selected_profile
                        .as_deref()
                        .unwrap_or("<none (builtin defaults)>")
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "compose file",
                    &format!(
                        "{} ({:?})",
                        output.compose_file.value.display(),
                        output.compose_file.source
                    )
                )
            );
            if let Some(error) = output.compose_load_error.as_deref() {
                println!(
                    "{}",
                    term::styled_label("compose load error", &term::styled_error(error))
                );
            }
            println!(
                "{}",
                term::styled_label(
                    "compose dir",
                    &output.runtime_paths.compose_dir.display().to_string()
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "current submit dir",
                    &output
                        .runtime_paths
                        .current_submit_dir
                        .display()
                        .to_string()
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "default script path",
                    &output
                        .runtime_paths
                        .default_script_path
                        .display()
                        .to_string()
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "runtime job root pattern",
                    &output.runtime_paths.runtime_job_root_pattern
                )
            );
            println!(
                "{}",
                term::styled_label(
                    "binaries",
                    &format!(
                        "enroot={} ({:?}) sbatch={} ({:?}) srun={} ({:?}) squeue={} ({:?}) sacct={} ({:?}) sstat={} ({:?}) scancel={} ({:?}) sshare={} ({:?}) sprio={} ({:?})",
                        output.binaries.enroot.value,
                        output.binaries.enroot.source,
                        output.binaries.sbatch.value,
                        output.binaries.sbatch.source,
                        output.binaries.srun.value,
                        output.binaries.srun.source,
                        output.binaries.squeue.value,
                        output.binaries.squeue.source,
                        output.binaries.sacct.value,
                        output.binaries.sacct.source,
                        output.binaries.sstat.value,
                        output.binaries.sstat.source,
                        output.binaries.scancel.value,
                        output.binaries.scancel.source,
                        output.binaries.sshare.value,
                        output.binaries.sshare.source,
                        output.binaries.sprio.value,
                        output.binaries.sprio.source,
                    )
                )
            );
            println!("{}:", term::styled_section_header("runtime paths"));
            if let Some(cache_dir) = &output.runtime_paths.cache_dir {
                println!(
                    "  {}",
                    term::styled_label(
                        "cache dir",
                        &format!("{} ({:?})", cache_dir.value.display(), cache_dir.source)
                    )
                );
            } else {
                println!("  {}", term::styled_label("cache dir", "<unavailable>"));
            }
            if let Some(resume) = &output.runtime_paths.resume_dir {
                println!(
                    "  {}",
                    term::styled_label(
                        "resume dir",
                        &format!("{} ({:?})", resume.value.display(), resume.source)
                    )
                );
            }
            if let Some(export) = &output.runtime_paths.artifact_export_dir {
                println!(
                    "  {}",
                    term::styled_label(
                        "artifact export dir",
                        &format!("{} ({:?})", export.value, export.source)
                    )
                );
            }
            println!(
                "  {}",
                term::styled_label(
                    "metadata root",
                    &format!(
                        "{} ({:?})",
                        output.runtime_paths.metadata_root.value.display(),
                        output.runtime_paths.metadata_root.source
                    )
                )
            );
            println!(
                "  {}",
                term::styled_label(
                    "jobs dir",
                    &format!(
                        "{} ({:?})",
                        output.runtime_paths.jobs_dir.value.display(),
                        output.runtime_paths.jobs_dir.source
                    )
                )
            );
            println!("{}:", term::styled_section_header("interpolation vars"));
            for (key, value) in &output.interpolation_vars {
                let source = output
                    .interpolation_var_sources
                    .get(key)
                    .copied()
                    .unwrap_or(ValueSource::Builtin);
                println!(
                    "  {}={}",
                    term::styled_bold(key),
                    term::styled_dim(&format!("{value} ({source:?})"))
                );
            }
        }
    }
    Ok(())
}

fn scoped_interpolation_vars(
    vars: &std::collections::BTreeMap<String, String>,
    sources: &std::collections::BTreeMap<String, ValueSource>,
    referenced: &std::collections::BTreeSet<String>,
    show_values: bool,
) -> (
    std::collections::BTreeMap<String, String>,
    std::collections::BTreeMap<String, ValueSource>,
) {
    let mut scoped_vars = std::collections::BTreeMap::new();
    let mut scoped_sources = std::collections::BTreeMap::new();
    for key in referenced {
        let Some(value) = vars.get(key) else {
            continue;
        };
        let source = sources.get(key).copied().unwrap_or(ValueSource::Builtin);
        let value = if show_values || !looks_sensitive_env_name(key) {
            value.clone()
        } else {
            "<redacted>".to_string()
        };
        scoped_vars.insert(key.clone(), value);
        scoped_sources.insert(key.clone(), source);
    }
    (scoped_vars, scoped_sources)
}

fn looks_sensitive_env_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "PASSWD",
        "API_KEY",
        "ACCESS_KEY",
        "PRIVATE_KEY",
        "CREDENTIAL",
        "AUTH",
        "COOKIE",
        "SESSION",
        "BEARER",
    ]
    .iter()
    .any(|needle| upper.contains(needle))
}

fn load_discovered_cluster_profile(
    context: &ResolvedContext,
) -> Result<Option<hpc_compose::cluster::ClusterProfile>> {
    let start = context
        .compose_file
        .value
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let Some(path) = discover_cluster_profile_path(start) else {
        return Ok(None);
    };
    Ok(Some(load_cluster_profile(&path)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use hpc_compose::context::{ResolvedBinaries, ResolvedContext};

    fn write_script(path: &std::path::Path, body: &str) {
        fs::write(path, body).expect("script");
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn write_compose(root: &std::path::Path) -> PathBuf {
        let local_image = root.join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&cache_dir).expect("cache dir");
        let compose = root.join("compose.yaml");
        fs::write(
            &compose,
            format!(
                r#"
name: command-demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: /bin/true
"#,
                cache_dir.display(),
                local_image.display()
            ),
        )
        .expect("compose");
        compose
    }

    fn write_remote_compose(root: &std::path::Path) -> PathBuf {
        let cache_dir = root.join("cache-remote");
        fs::create_dir_all(&cache_dir).expect("remote cache dir");
        let compose = root.join("compose-remote.yaml");
        fs::write(
            &compose,
            format!(
                r#"
name: remote-demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: redis:7
    command: /bin/true
"#,
                cache_dir.display()
            ),
        )
        .expect("remote compose");
        compose
    }

    fn write_context_compose(root: &std::path::Path) -> PathBuf {
        let local_image = root.join("context.sqsh");
        fs::write(&local_image, "sqsh").expect("context image");
        let cache_dir = root.join("cache-context");
        fs::create_dir_all(&cache_dir).expect("context cache");
        let compose = root.join("compose-context.yaml");
        fs::write(
            &compose,
            format!(
                r#"
name: context-demo
x-slurm:
  cache_dir: {}
  resume:
    path: /shared/runs/demo
  artifacts:
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/logs/**
services:
  app:
    image: {}
    command: /bin/true
"#,
                cache_dir.display(),
                local_image.display()
            ),
        )
        .expect("context compose");
        compose
    }

    fn write_missing_image_compose(root: &std::path::Path) -> PathBuf {
        let cache_dir = root.join("cache-missing");
        fs::create_dir_all(&cache_dir).expect("missing cache");
        let compose = root.join("compose-missing.yaml");
        fs::write(
            &compose,
            format!(
                r#"
name: missing-demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: /bin/true
"#,
                cache_dir.display(),
                root.join("missing.sqsh").display()
            ),
        )
        .expect("missing compose");
        compose
    }

    fn binaries(root: &std::path::Path) -> ResolvedBinaries {
        let enroot = root.join("enroot");
        let sbatch = root.join("sbatch");
        let srun = root.join("srun");
        write_script(&enroot, "#!/bin/bash\nexit 0\n");
        write_script(&sbatch, "#!/bin/bash\nexit 0\n");
        write_script(
            &srun,
            "#!/bin/bash\nif [ \"$1\" = \"--help\" ]; then echo '--container-image'; fi\nexit 0\n",
        );
        let resolved = |value: &std::path::Path| ResolvedValue {
            value: value.display().to_string(),
            source: ValueSource::Cli,
        };
        ResolvedBinaries {
            enroot: resolved(&enroot),
            apptainer: ResolvedValue {
                value: "apptainer".to_string(),
                source: ValueSource::Builtin,
            },
            singularity: ResolvedValue {
                value: "singularity".to_string(),
                source: ValueSource::Builtin,
            },
            salloc: ResolvedValue {
                value: "salloc".to_string(),
                source: ValueSource::Builtin,
            },
            sbatch: resolved(&sbatch),
            srun: resolved(&srun),
            scontrol: ResolvedValue {
                value: "scontrol".to_string(),
                source: ValueSource::Builtin,
            },
            sinfo: ResolvedValue {
                value: "sinfo".to_string(),
                source: ValueSource::Builtin,
            },
            squeue: ResolvedValue {
                value: "squeue".to_string(),
                source: ValueSource::Builtin,
            },
            sacct: ResolvedValue {
                value: "sacct".to_string(),
                source: ValueSource::Builtin,
            },
            sstat: ResolvedValue {
                value: "sstat".to_string(),
                source: ValueSource::Builtin,
            },
            scancel: ResolvedValue {
                value: "scancel".to_string(),
                source: ValueSource::Builtin,
            },
            sshare: ResolvedValue {
                value: "sshare".to_string(),
                source: ValueSource::Builtin,
            },
            sprio: ResolvedValue {
                value: "sprio".to_string(),
                source: ValueSource::Builtin,
            },
        }
    }

    fn context_for(compose: &std::path::Path, root: &std::path::Path) -> ResolvedContext {
        ResolvedContext {
            cwd: root.to_path_buf(),
            settings_path: None,
            settings_base_dir: None,
            selected_profile: None,
            compose_file: ResolvedValue {
                value: compose.to_path_buf(),
                source: ValueSource::Cli,
            },
            cache_dir: ResolvedValue {
                value: root.join(".cache/hpc-compose"),
                source: ValueSource::Builtin,
            },
            resource_profiles: BTreeMap::new(),
            binaries: binaries(root),
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
        }
    }

    fn tempdir_in_repo() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("hpc-compose-spec-command-tests-")
            .tempdir_in(env::current_dir().expect("cwd"))
            .expect("tmpdir")
    }

    #[test]
    fn command_wrappers_cover_json_and_text_paths() {
        let tmpdir = tempdir_in_repo();
        let compose = write_compose(tmpdir.path());
        let resolved_context = context_for(&compose, tmpdir.path());

        validate(resolved_context.clone(), false, Some(OutputFormat::Json)).expect("validate json");
        render(resolved_context.clone(), None, Some(OutputFormat::Json)).expect("render json");
        render(
            resolved_context.clone(),
            Some(tmpdir.path().join("rendered.sbatch")),
            Some(OutputFormat::Json),
        )
        .expect("render file json");
        prepare(
            resolved_context.clone(),
            false,
            true,
            Some(OutputFormat::Json),
            false,
        )
        .expect("prepare json");
        preflight(
            resolved_context.clone(),
            false,
            true,
            Some(OutputFormat::Json),
            false,
            false,
        )
        .expect("preflight json");
        inspect(
            resolved_context.clone(),
            false,
            false,
            false,
            false,
            DependencyOutputFormat::Text,
            None,
            Some(OutputFormat::Json),
            false,
        )
        .expect("inspect json");
        context(resolved_context.clone(), Some(OutputFormat::Json), false).expect("context json");
        context(resolved_context, None, false).expect("context text");
    }

    #[test]
    fn context_succeeds_when_compose_cannot_be_loaded() {
        let tmpdir = tempdir_in_repo();
        let missing = tmpdir.path().join("missing.yaml");
        let resolved_context = context_for(&missing, tmpdir.path());
        context(resolved_context.clone(), Some(OutputFormat::Json), false).expect("context json");
        context(resolved_context, None, false).expect("context text");
    }

    #[test]
    fn render_preflight_and_context_cover_error_and_optional_text_paths() {
        let tmpdir = tempdir_in_repo();
        let compose = write_compose(tmpdir.path());
        let resolved_context = context_for(&compose, tmpdir.path());

        let render_err = render(
            resolved_context.clone(),
            Some(tmpdir.path().join("missing/output/rendered.sbatch")),
            None,
        )
        .expect_err("render should report write failures");
        assert!(
            render_err
                .to_string()
                .contains("failed to write rendered script")
        );

        let remote_compose = write_remote_compose(tmpdir.path());
        let strict_warning_context = context_for(&remote_compose, tmpdir.path());
        let strict_warning = preflight(strict_warning_context, true, false, None, false, false)
            .expect_err("warnings");
        assert!(
            strict_warning
                .to_string()
                .contains("preflight reported warnings")
        );

        let missing_compose = write_missing_image_compose(tmpdir.path());
        let missing_context = context_for(&missing_compose, tmpdir.path());
        let preflight_err = preflight(missing_context, false, false, None, false, false)
            .expect_err("missing image");
        assert!(preflight_err.to_string().contains("preflight failed"));

        let context_compose = write_context_compose(tmpdir.path());
        let mut context_with_vars = context_for(&context_compose, tmpdir.path());
        context_with_vars
            .interpolation_vars
            .insert("EXTRA_VAR".into(), "value".into());
        context_with_vars
            .interpolation_var_sources
            .insert("EXTRA_VAR".into(), ValueSource::Profile);
        context(context_with_vars, None, false).expect("context text with optional fields");
    }

    #[test]
    fn validate_render_and_inspect_cover_additional_text_and_strict_env_paths() {
        let tmpdir = tempdir_in_repo();
        let compose = write_compose(tmpdir.path());
        let resolved_context = context_for(&compose, tmpdir.path());

        validate(resolved_context.clone(), false, None).expect("validate text");
        render(
            resolved_context.clone(),
            Some(tmpdir.path().join("rendered-text.sbatch")),
            None,
        )
        .expect("render text");
        inspect(
            resolved_context,
            true,
            false,
            false,
            false,
            DependencyOutputFormat::Text,
            None,
            None,
            false,
        )
        .expect("inspect verbose text");

        let strict_compose = tmpdir.path().join("compose-strict.yaml");
        fs::create_dir_all(tmpdir.path().join("cache-strict")).expect("strict cache");
        fs::write(
            &strict_compose,
            format!(
                r#"
name: strict-demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: redis:7
    command: /bin/sh -lc "echo ${{NEEDS_DEFAULT:-fallback}}"
"#,
                tmpdir.path().join("cache-strict").display()
            ),
        )
        .expect("strict compose");
        let strict_context = context_for(&strict_compose, tmpdir.path());
        let strict_err =
            validate(strict_context, true, Some(OutputFormat::Json)).expect_err("strict env");
        assert!(
            strict_err
                .to_string()
                .contains("strict env validation failed")
        );
    }
}
