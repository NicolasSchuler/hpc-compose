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
use hpc_compose::lint_fix::{self, AppliedFix};
use hpc_compose::planner::ImageSource;
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{
    PrepareOptions, RuntimePlan, build_runtime_plan, prepare_runtime_plan_with_reporter,
};
use hpc_compose::render::{RenderOptions, render_script_with_options};
use hpc_compose::spec::{missing_defaulted_variables, referenced_variables};
use hpc_compose::term;
use serde::Serialize;

use crate::output::{self, common as output_common, spec as output_spec};
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
            output::print_next_steps(&output::validate_next_commands(Some(
                &context.compose_file.value,
            )));
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
    fixable_count: usize,
    findings: Vec<LintFinding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    applied_fixes: Vec<AppliedFix>,
}

pub(crate) fn lint(
    context: ResolvedContext,
    strict_env: bool,
    allow_warnings: bool,
    fix: bool,
    dry_run: bool,
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
    let fixable_count = findings.iter().filter(|f| f.fix.is_some()).count();

    let mut displayed_findings = findings;
    let mut applied_fixes: Vec<AppliedFix> = Vec::new();
    let mut dry_run_diff: Option<String> = None;

    if fix && fixable_count > 0 {
        let original_text = fs::read_to_string(&context.compose_file.value).with_context(|| {
            format!(
                "failed to read {} for --fix",
                context.compose_file.value.display()
            )
        })?;
        let fixable: Vec<_> = displayed_findings
            .iter()
            .filter_map(|finding| finding.fix.clone())
            .collect();
        let (new_text, applied) = lint_fix::apply_fixes(&original_text, &fixable)
            .context("lint --fix could not apply a fix safely")?;
        applied_fixes = applied;
        if dry_run {
            dry_run_diff = Some(lint_fix::unified_diff(&original_text, &new_text));
        } else {
            crate::secure_io::write_atomic_preserving_mode(
                &context.compose_file.value,
                new_text.as_bytes(),
                false,
            )
            .with_context(|| {
                format!(
                    "failed to write fixes to {}",
                    context.compose_file.value.display()
                )
            })?;
            // Safety gate: reload the spec to confirm the rewrite is still
            // valid and to refresh findings. Roll back on any failure.
            let reload = output_common::load_plan_and_runtime_with_interpolation_vars_cache_default_and_resource_profiles(
                &context.compose_file.value,
                &context.interpolation_vars,
                Some(&context.cache_dir.value),
                &context.resource_profiles,
            );
            match reload {
                Ok((plan2, runtime_plan2)) => {
                    let findings_after = hpc_compose::lint::lint_plan(
                        &plan2,
                        &runtime_plan2,
                        cluster_profile.as_ref(),
                    );
                    displayed_findings = findings_after;
                }
                Err(err) => {
                    crate::secure_io::write_atomic_preserving_mode(
                        &context.compose_file.value,
                        original_text.as_bytes(),
                        false,
                    )
                    .with_context(|| {
                        format!(
                            "failed to restore original compose file after --fix validation failure at {}",
                            context.compose_file.value.display()
                        )
                    })?;
                    bail!(
                        "lint --fix produced a spec that failed to reload; restored the original file. Error: {err:#}"
                    );
                }
            }
        }
    }

    let warning_count = displayed_findings
        .iter()
        .filter(|finding| finding.level == LintLevel::Warning)
        .count();
    let error_count = displayed_findings
        .iter()
        .filter(|finding| finding.level == LintLevel::Error)
        .count();
    let passed = error_count == 0 && (allow_warnings || warning_count == 0);

    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            print_lint_findings(&displayed_findings, passed);
            print_fix_summary(fix, dry_run, &applied_fixes, dry_run_diff.as_deref());
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&LintOutput {
                    passed,
                    compose_file: plan.spec_path,
                    warning_count,
                    error_count,
                    fixable_count,
                    findings: displayed_findings,
                    applied_fixes,
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

fn print_fix_summary(fix: bool, dry_run: bool, applied: &[AppliedFix], diff: Option<&str>) {
    if !fix {
        return;
    }
    if dry_run {
        if let Some(diff) = diff {
            println!();
            println!(
                "{}",
                term::styled_section_header("Proposed changes (--dry-run):")
            );
            print!("{diff}");
        } else {
            println!();
            println!(
                "{}",
                term::styled_success("lint --fix: no changes proposed")
            );
        }
        return;
    }
    if applied.is_empty() {
        return;
    }
    println!();
    println!(
        "{}",
        term::styled_success(&format!(
            "Applied {} fix(es) to the compose file:",
            applied.len()
        ))
    );
    for fix in applied {
        println!("- {} [{}]: {}", fix.service, fix.code, fix.description);
    }
    println!(
        "{}",
        term::styled_dim("re-run `hpc-compose lint -f <file>` to confirm remaining findings")
    );
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
            huggingface_cli_bin: context.huggingface_cli_bin.clone(),
            cluster_profile,
            runtime_root: None,
        },
    )?;
    if let Some(path) = output_path.as_ref() {
        // The rendered script can carry resolved `secrets:` values literally in
        // its launch_env, so it must be owner-only like every real-submission
        // path (secrets.md promises mode 0600). Plain fs::write would leave it
        // group/world-readable on a shared cluster filesystem.
        crate::secure_io::write(path, &script, true)
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
                huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                cluster_profile: cluster_profile.clone(),
                runtime_root: None,
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
    if crate::platform::is_macos() {
        hints.push(PlanHint {
            level: "next",
            message: "next: inspect with `hpc-compose plan --show-script -f <compose.yaml>`; \
                run `hpc-compose up` from a Linux Slurm login node (macOS is authoring-only)"
                .to_string(),
        });
    } else {
        hints.push(PlanHint {
            level: "next",
            message: "next command: hpc-compose up -f <compose.yaml>".to_string(),
        });
    }
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
    let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        &context.compose_file.value,
        &context.interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    let prepare_progress =
        PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
    let summary = prepare_progress.run("Preparing runtime artifacts", || {
        prepare_runtime_plan_with_reporter(
            &runtime_plan,
            &PrepareOptions {
                enroot_bin: context.binaries.enroot.value.clone(),
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                keep_failed_prep,
                force_rebuild: force,
                enroot_temp_dir: context.enroot_temp_dir.clone(),
            },
            &prepare_progress,
        )
    })?;
    prepare_progress.finish_from_summary(&summary);
    match output_format {
        OutputFormat::Text if !quiet => {
            output_spec::print_prepare_summary(&summary);
            output::print_next_steps(&output::ready_to_run_next_commands(Some(
                &context.compose_file.value,
            )));
        }
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
    // Reached only on a clean pass; point at the run.
    if output_format == OutputFormat::Text && !quiet {
        output::print_next_steps(&output::ready_to_run_next_commands(Some(
            &context.compose_file.value,
        )));
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
    let secret_values = crate::redaction::secret_value_set(
        &context.interpolation_vars,
        &context.interpolation_var_sources,
    );
    let redacted_runtime_plan =
        crate::redaction::redacted_runtime_plan(&runtime_plan, &secret_values, false);

    match output_common::resolve_output_format(format, json) {
        OutputFormat::Text => {
            if tree {
                output_spec::print_plan_inspect_tree(&plan, &redacted_runtime_plan)
                    .context("failed to write tree output")?;
            } else if verbose {
                let cluster_profile = load_discovered_cluster_profile(&context)?;
                if cluster_profile.is_some() {
                    output_spec::print_plan_inspect_verbose_with_profile(
                        &plan,
                        &redacted_runtime_plan,
                        cluster_profile.as_ref(),
                    )
                    .context("failed to write inspect output")?;
                } else {
                    output_spec::print_plan_inspect_verbose(&plan, &redacted_runtime_plan)
                        .context("failed to write inspect output")?;
                }
            } else {
                output_spec::print_plan_inspect(&redacted_runtime_plan)
                    .context("failed to write inspect output")?;
            }
        }
        OutputFormat::Json => {
            let runtime_plan =
                crate::redaction::redacted_json_value(&runtime_plan, &secret_values, false)
                    .context("failed to serialize inspect output")?;
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
    let mut config = output_common::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
        &context.compose_file.value,
        &context.interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    // Redact sensitive service env values before any output. Secrets declared
    // via the top-level `secrets:` block (ValueSource::Secret) and any value
    // matching a resolved secret are hidden unless --show-values is passed.
    let secret_values = crate::redaction::secret_value_set(
        &context.interpolation_vars,
        &context.interpolation_var_sources,
    );
    for service in config.services.values_mut() {
        service.environment =
            crate::redaction::redact_env_map(&service.environment, &secret_values, show_values);
    }
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
            let redacted_config =
                crate::redaction::redacted_yaml_value(&config, &secret_values, show_values)
                    .context("failed to serialize config output")?;
            print!(
                "{}",
                serde_norway::to_string(&redacted_config)
                    .context("failed to serialize config output")?
            );
        }
        OutputFormat::Json => {
            let redacted_config =
                crate::redaction::redacted_json_value(&config, &secret_values, show_values)
                    .context("failed to serialize config output")?;
            println!(
                "{}",
                serde_json::to_string_pretty(&redacted_config)
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
    /// Resolved enroot prepare-time temporary scratch directory
    /// (`ENROOT_TEMP_PATH`).
    enroot_temp_dir: ResolvedValue<PathBuf>,
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

/// Resolves the enroot prepare temp dir for display, tracking which layer won.
fn resolve_enroot_temp_display(
    spec_value: Option<&str>,
    settings_value: Option<&str>,
    cache_dir: &std::path::Path,
) -> ResolvedValue<PathBuf> {
    let env_value = std::env::var(hpc_compose::prepare::ENROOT_TEMP_DIR_ENV).ok();
    let env_ref = env_value.as_deref().filter(|raw| !raw.trim().is_empty());
    let spec_ref = spec_value.filter(|raw| !raw.trim().is_empty());
    let settings_ref = settings_value.filter(|raw| !raw.trim().is_empty());
    let value =
        hpc_compose::prepare::resolve_enroot_temp_dir(env_ref, spec_ref, settings_ref, cache_dir);
    let source = if env_ref.is_some() {
        ValueSource::ProcessEnv
    } else if spec_ref.is_some() {
        ValueSource::Compose
    } else if settings_ref.is_some() {
        ValueSource::Defaults
    } else {
        ValueSource::Builtin
    };
    ResolvedValue { value, source }
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
    let (cache_dir, spec_enroot_temp, plan_cache_dir, resume_dir, artifact_export_dir, compose_load_error) =
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
                runtime_plan.slurm.enroot_temp_dir.clone(),
                runtime_plan.cache_dir.clone(),
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
            Err(err) => (None, None, context.cache_dir.value.clone(), None, None, Some(format!("{err:#}"))),
        };
    let enroot_temp_dir = resolve_enroot_temp_display(
        spec_enroot_temp.as_deref(),
        context.enroot_temp_dir.as_deref(),
        &plan_cache_dir,
    );
    let runtime_paths = ContextRuntimePaths {
        compose_dir: compose_dir.clone(),
        current_submit_dir: current_submit_dir.clone(),
        default_script_path: output_common::default_script_path(&context.compose_file.value),
        runtime_job_root_pattern: current_submit_dir
            .join(crate::tracked_paths::METADATA_DIR_NAME)
            .join("{job_id}")
            .display()
            .to_string(),
        cache_dir,
        enroot_temp_dir,
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
            println!(
                "  {}",
                term::styled_label(
                    "enroot temp dir",
                    &format!(
                        "{} ({:?})",
                        output.runtime_paths.enroot_temp_dir.value.display(),
                        output.runtime_paths.enroot_temp_dir.source
                    )
                )
            );
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
    let secret_values = crate::redaction::secret_value_set(vars, sources);
    let mut scoped_vars = std::collections::BTreeMap::new();
    let mut scoped_sources = std::collections::BTreeMap::new();
    for key in referenced {
        let Some(value) = vars.get(key) else {
            continue;
        };
        let source = sources.get(key).copied().unwrap_or(ValueSource::Builtin);
        let redacted =
            crate::redaction::redact_value(key, value, Some(source), &secret_values, show_values);
        scoped_vars.insert(key.clone(), redacted);
        scoped_sources.insert(key.clone(), source);
    }
    (scoped_vars, scoped_sources)
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
            login_host: None,
            login_user: None,
            enroot_temp_dir: None,
            resource_profiles: BTreeMap::new(),
            binaries: binaries(root),
            huggingface_cli_bin: "huggingface-cli".to_string(),
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
            watch: Default::default(),
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
