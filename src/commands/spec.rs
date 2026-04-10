use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::{ResolvedContext, ResolvedValue, ValueSource};
use hpc_compose::job::{jobs_dir_for, metadata_root_for};
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, build_runtime_plan, prepare_runtime_plan};
use hpc_compose::render::render_script;
use hpc_compose::spec::missing_defaulted_variables;
use serde::Serialize;

use crate::output;

pub(crate) fn validate(
    context: ResolvedContext,
    strict_env: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let plan = output::load_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
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
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => println!("spec is valid"),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::build_validate_output(&plan))
                    .context("failed to serialize validate output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn render(
    context: ResolvedContext,
    output_path: Option<PathBuf>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let plan = output::load_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let runtime_plan = build_runtime_plan(&plan);
    let script = render_script(&runtime_plan)?;
    if let Some(path) = output_path.as_ref() {
        fs::write(path, &script)
            .with_context(|| format!("failed to write rendered script to {}", path.display()))?;
    }
    match output::resolve_output_format(format, false) {
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
                serde_json::to_string_pretty(&output::RenderOutput {
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

pub(crate) fn prepare(
    context: ResolvedContext,
    keep_failed_prep: bool,
    force: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let summary = prepare_runtime_plan(
        &runtime_plan,
        &PrepareOptions {
            enroot_bin: context.binaries.enroot.value,
            keep_failed_prep,
            force_rebuild: force,
        },
    )?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => output::print_prepare_summary(&summary),
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
) -> Result<()> {
    let runtime_plan = output::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let report = run_preflight(
        &runtime_plan,
        &PreflightOptions {
            enroot_bin: context.binaries.enroot.value,
            sbatch_bin: context.binaries.sbatch.value,
            srun_bin: context.binaries.srun.value,
            scontrol_bin: "scontrol".to_string(),
            require_submit_tools: true,
            skip_prepare: false,
        },
    );
    match output::resolve_output_format(format, json) {
        OutputFormat::Text => output::print_report(&report, verbose),
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

pub(crate) fn inspect(
    context: ResolvedContext,
    verbose: bool,
    format: Option<OutputFormat>,
    json: bool,
) -> Result<()> {
    let (plan, runtime_plan) = output::load_plan_and_runtime_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    match output::resolve_output_format(format, json) {
        OutputFormat::Text => {
            if verbose {
                output::print_plan_inspect_verbose(&plan, &runtime_plan);
            } else {
                output::print_plan_inspect(&runtime_plan);
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

pub(crate) fn context(context: ResolvedContext, format: Option<OutputFormat>) -> Result<()> {
    let compose_dir = context
        .compose_file
        .value
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();
    let current_submit_dir = context.cwd.clone();
    let (cache_dir, resume_dir, artifact_export_dir, compose_load_error) =
        match output::load_plan_and_runtime_with_interpolation_vars(
            &context.compose_file.value,
            &context.interpolation_vars,
        ) {
            Ok((plan, runtime_plan)) => (
                Some(ResolvedValue {
                    value: runtime_plan.cache_dir.clone(),
                    source: if plan.slurm.cache_dir.is_some() {
                        ValueSource::Compose
                    } else {
                        ValueSource::Builtin
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
        default_script_path: output::default_script_path(&context.compose_file.value),
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
    let output = ContextOutput {
        cwd: context.cwd,
        settings_path: context.settings_path,
        settings_base_dir: context.settings_base_dir,
        selected_profile: context.selected_profile,
        compose_file: context.compose_file,
        binaries: context.binaries,
        interpolation_vars: context.interpolation_vars,
        interpolation_var_sources: context.interpolation_var_sources,
        compose_load_error,
        runtime_paths,
    };

    match output::resolve_output_format(format, false) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize context output")?
            );
        }
        OutputFormat::Text => {
            println!("cwd: {}", output.cwd.display());
            println!(
                "settings file: {}",
                output
                    .settings_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            );
            println!(
                "settings base dir: {}",
                output
                    .settings_base_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            );
            println!(
                "profile: {}",
                output
                    .selected_profile
                    .as_deref()
                    .unwrap_or("<none (builtin defaults)>")
            );
            println!(
                "compose file: {} ({:?})",
                output.compose_file.value.display(),
                output.compose_file.source
            );
            if let Some(error) = output.compose_load_error.as_deref() {
                println!("compose load error: {error}");
            }
            println!(
                "compose dir: {}",
                output.runtime_paths.compose_dir.display()
            );
            println!(
                "current submit dir: {}",
                output.runtime_paths.current_submit_dir.display()
            );
            println!(
                "default script path: {}",
                output.runtime_paths.default_script_path.display()
            );
            println!(
                "runtime job root pattern: {}",
                output.runtime_paths.runtime_job_root_pattern
            );
            println!(
                "binaries: enroot={} ({:?}) sbatch={} ({:?}) srun={} ({:?}) squeue={} ({:?}) sacct={} ({:?}) sstat={} ({:?}) scancel={} ({:?})",
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
            );
            println!("runtime paths:");
            if let Some(cache_dir) = &output.runtime_paths.cache_dir {
                println!(
                    "  cache dir: {} ({:?})",
                    cache_dir.value.display(),
                    cache_dir.source
                );
            } else {
                println!("  cache dir: <unavailable>");
            }
            if let Some(resume) = &output.runtime_paths.resume_dir {
                println!(
                    "  resume dir: {} ({:?})",
                    resume.value.display(),
                    resume.source
                );
            }
            if let Some(export) = &output.runtime_paths.artifact_export_dir {
                println!(
                    "  artifact export dir: {} ({:?})",
                    export.value, export.source
                );
            }
            println!(
                "  metadata root: {} ({:?})",
                output.runtime_paths.metadata_root.value.display(),
                output.runtime_paths.metadata_root.source
            );
            println!(
                "  jobs dir: {} ({:?})",
                output.runtime_paths.jobs_dir.value.display(),
                output.runtime_paths.jobs_dir.source
            );
            println!("interpolation vars:");
            for (key, value) in &output.interpolation_vars {
                let source = output
                    .interpolation_var_sources
                    .get(key)
                    .copied()
                    .unwrap_or(ValueSource::Builtin);
                println!("  {key}={value} ({source:?})");
            }
        }
    }
    Ok(())
}
