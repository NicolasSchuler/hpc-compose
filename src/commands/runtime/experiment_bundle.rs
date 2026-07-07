//! `hpc-compose experiment bundle` command orchestration.
//!
//! The command resolves a tracked record and collects the same read-only
//! aggregate as `experiment show`, then delegates all filesystem materialization
//! to `job::write_experiment_bundle`.

use super::*;

const DEFAULT_BUNDLE_PUE: f64 = 1.20;
const DEFAULT_BUNDLE_GPU_TDP_W: f64 = 300.0;
const DEFAULT_BUNDLE_CPU_WATTS_PER_CORE: f64 = 8.0;

pub(crate) fn experiment_bundle(
    context: ResolvedContext,
    job_id: Option<String>,
    into: PathBuf,
    tarball: bool,
    include_artifacts: bool,
    bundles: Vec<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let show = collect_experiment_show_output(
        &context,
        &record,
        DEFAULT_BUNDLE_PUE,
        DEFAULT_BUNDLE_GPU_TDP_W,
        DEFAULT_BUNDLE_CPU_WATTS_PER_CORE,
    )?;
    let checkpoint_history = hpc_compose::job::collect_checkpoint_history(&record);
    let manifest = hpc_compose::job::write_experiment_bundle(
        &record,
        &show,
        &checkpoint_history,
        &hpc_compose::job::ExperimentBundleOptions {
            into_dir: into,
            tarball,
            include_artifacts,
            selected_bundles: bundles,
        },
    )?;

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest)
                    .context("failed to serialize experiment bundle output")?
            );
        }
        OutputFormat::Text => print_experiment_bundle_output(&manifest),
    }
    Ok(())
}

fn print_experiment_bundle_output(manifest: &hpc_compose::job::ExperimentBundleManifest) {
    println!("{}", term::styled_section_header("Experiment Bundle"));
    println!("  job:    {}", manifest.job_id);
    println!("  root:   {}", manifest.bundle_root.display());
    println!(
        "  files:  {}",
        manifest
            .files
            .iter()
            .filter(|entry| entry.relative_path != "manifest.json")
            .count()
    );
    println!(
        "  payload: {}",
        if manifest.artifact_payload_included {
            if manifest.selected_bundles.is_empty() {
                "requested, none copied".to_string()
            } else {
                format!("included ({})", manifest.selected_bundles.join(", "))
            }
        } else {
            "metadata only".to_string()
        }
    );
    if let Some(tarball) = &manifest.tarball_path {
        println!("  tarball: {}", tarball.display());
    }
    for warning in &manifest.warnings {
        hpc_compose::diagnostics::warn(warning);
    }
}
