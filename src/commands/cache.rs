use std::path::PathBuf;

use anyhow::{Context, Result};
use hpc_compose::cache::{CacheEntryKind, prune_all_unused, prune_by_age, scan_cache};
use hpc_compose::cli::OutputFormat;

use crate::output;

pub(crate) fn list(cache_dir: Option<PathBuf>, format: Option<OutputFormat>) -> Result<()> {
    let cache_dir = cache_dir.unwrap_or_else(output::default_cache_dir);
    let manifests = scan_cache(&cache_dir)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            if manifests.is_empty() {
                println!("no cache entries found in {}", cache_dir.display());
            } else {
                println!("cache dir: {}", cache_dir.display());
                for manifest in manifests {
                    let kind = match manifest.kind {
                        CacheEntryKind::Base => "base",
                        CacheEntryKind::Prepared => "prepared",
                    };
                    println!(
                        "{kind}\t{}\tservices={}\tsource={}",
                        manifest.artifact_path,
                        manifest.service_names.join(","),
                        manifest.source_image
                    );
                }
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&manifests)
                    .context("failed to serialize cache list output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn inspect(
    file: PathBuf,
    service: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let runtime_plan = output::load_runtime_plan(&file)?;
    let report = output::build_cache_inspect_report(&runtime_plan, service.as_deref())?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => output::print_cache_inspect(&report)?,
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize cache inspect output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn prune(
    file: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    age: Option<u64>,
    all_unused: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let report = if let Some(days) = age {
        let target = cache_dir.unwrap_or_else(output::default_cache_dir);
        let result = prune_by_age(&target, days)?;
        output::CachePruneReport {
            cache_dir: target,
            mode: "age".to_string(),
            removed_count: result.removed.len(),
            removed_paths: result.removed,
        }
    } else {
        debug_assert!(all_unused);
        let file = file.context(
            "--all-unused requires -f/--file so the current plan can define which artifacts are still referenced",
        )?;
        let runtime_plan = output::load_runtime_plan(&file)?;
        let target = cache_dir.unwrap_or_else(|| runtime_plan.cache_dir.clone());
        let result = prune_all_unused(&target, &runtime_plan)?;
        output::CachePruneReport {
            cache_dir: target,
            mode: "all_unused".to_string(),
            removed_count: result.removed.len(),
            removed_paths: result.removed,
        }
    };
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => output::print_prune_result(&report.cache_dir, &report.removed_paths),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize cache prune output")?
            );
        }
    }
    Ok(())
}
