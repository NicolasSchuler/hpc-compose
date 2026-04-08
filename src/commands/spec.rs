use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::preflight::{Options as PreflightOptions, run as run_preflight};
use hpc_compose::prepare::{PrepareOptions, build_runtime_plan, prepare_runtime_plan};
use hpc_compose::render::render_script;

use crate::output;

pub(crate) fn validate(file: PathBuf, format: Option<OutputFormat>) -> Result<()> {
    let plan = output::load_plan(&file)?;
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
    file: PathBuf,
    output_path: Option<PathBuf>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let plan = output::load_plan(&file)?;
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
    file: PathBuf,
    enroot_bin: String,
    keep_failed_prep: bool,
    force: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let runtime_plan = output::load_runtime_plan(&file)?;
    let summary = prepare_runtime_plan(
        &runtime_plan,
        &PrepareOptions {
            enroot_bin,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn preflight(
    file: PathBuf,
    strict: bool,
    verbose: bool,
    format: Option<OutputFormat>,
    json: bool,
    enroot_bin: String,
    sbatch_bin: String,
    srun_bin: String,
) -> Result<()> {
    let runtime_plan = output::load_runtime_plan(&file)?;
    let report = run_preflight(
        &runtime_plan,
        &PreflightOptions {
            enroot_bin,
            sbatch_bin,
            srun_bin,
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
    file: PathBuf,
    verbose: bool,
    format: Option<OutputFormat>,
    json: bool,
) -> Result<()> {
    let (plan, runtime_plan) = output::load_plan_and_runtime(&file)?;
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
