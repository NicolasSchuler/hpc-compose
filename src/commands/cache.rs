use std::path::PathBuf;

use anyhow::{Context, Result};
use hpc_compose::cache::{CacheEntryKind, prune_all_unused, prune_by_age, scan_cache};
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::{ResolvedContext, ValueSource};

use crate::output::{cache as output_cache, common as output_common};

pub(crate) fn list(cache_dir: Option<PathBuf>, format: Option<OutputFormat>) -> Result<()> {
    let cache_dir = cache_dir.unwrap_or_else(output_common::default_cache_dir);
    let manifests = scan_cache(&cache_dir)?;
    match output_common::resolve_output_format(format, false) {
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
    context: ResolvedContext,
    service: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    let report = output_cache::build_cache_inspect_report(&runtime_plan, service.as_deref())?;
    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => output_cache::print_cache_inspect(&report)?,
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
    context: ResolvedContext,
    cache_dir: Option<PathBuf>,
    age: Option<u64>,
    all_unused: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let report = if let Some(days) = age {
        let target = match cache_dir {
            Some(path) => path,
            None => active_cache_dir(&context)?,
        };
        let result = prune_by_age(&target, days)?;
        output_cache::CachePruneReport {
            cache_dir: target,
            mode: "age".to_string(),
            removed_count: result.removed.len(),
            removed_paths: result.removed,
        }
    } else {
        debug_assert!(all_unused);
        let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars(
            &context.compose_file.value,
            &context.interpolation_vars,
        )?;
        let target = cache_dir.unwrap_or_else(|| runtime_plan.cache_dir.clone());
        let result = prune_all_unused(&target, &runtime_plan)?;
        output_cache::CachePruneReport {
            cache_dir: target,
            mode: "all_unused".to_string(),
            removed_count: result.removed.len(),
            removed_paths: result.removed,
        }
    };
    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output_cache::print_prune_result(&report.cache_dir, &report.removed_paths)
        }
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

fn active_cache_dir(context: &ResolvedContext) -> Result<PathBuf> {
    if context.compose_file.source == ValueSource::Builtin && !context.compose_file.value.exists() {
        return Ok(output_common::default_cache_dir());
    }
    let runtime_plan = output_common::load_runtime_plan_with_interpolation_vars(
        &context.compose_file.value,
        &context.interpolation_vars,
    )?;
    Ok(runtime_plan.cache_dir)
}

pub(crate) fn prune_no_context(
    cache_dir: Option<PathBuf>,
    age: Option<u64>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let days = age.context("cache prune --age requires a day value")?;
    let target = cache_dir.unwrap_or_else(output_common::default_cache_dir);
    let result = prune_by_age(&target, days)?;
    let report = output_cache::CachePruneReport {
        cache_dir: target,
        mode: "age".to_string(),
        removed_count: result.removed.len(),
        removed_paths: result.removed,
    };
    match output_common::resolve_output_format(format, false) {
        OutputFormat::Text => {
            output_cache::print_prune_result(&report.cache_dir, &report.removed_paths)
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::fs;

    use hpc_compose::context::{ResolvedBinaries, ResolvedValue, ValueSource};

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
name: cache-demo
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

    fn context_for(compose: &std::path::Path) -> ResolvedContext {
        let binary = |name: &str| ResolvedValue {
            value: name.to_string(),
            source: ValueSource::Builtin,
        };
        ResolvedContext {
            cwd: compose.parent().expect("compose dir").to_path_buf(),
            settings_path: None,
            settings_base_dir: None,
            selected_profile: None,
            compose_file: ResolvedValue {
                value: compose.to_path_buf(),
                source: ValueSource::Cli,
            },
            binaries: ResolvedBinaries {
                enroot: binary("enroot"),
                sbatch: binary("sbatch"),
                srun: binary("srun"),
                squeue: binary("squeue"),
                sacct: binary("sacct"),
                sstat: binary("sstat"),
                scancel: binary("scancel"),
            },
            interpolation_vars: BTreeMap::new(),
            interpolation_var_sources: BTreeMap::new(),
        }
    }

    #[test]
    fn cache_commands_cover_json_and_error_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = write_compose(tmpdir.path());
        let context = context_for(&compose);
        let cache_dir = tmpdir.path().join("cache");

        list(Some(cache_dir.clone()), Some(OutputFormat::Json)).expect("list");
        inspect(
            context.clone(),
            Some("app".to_string()),
            Some(OutputFormat::Json),
        )
        .expect("inspect");
        prune(
            context.clone(),
            Some(cache_dir.clone()),
            Some(999),
            false,
            Some(OutputFormat::Json),
        )
        .expect("prune age");
        prune(context, None, None, true, Some(OutputFormat::Json)).expect("prune all unused");
        prune_no_context(Some(cache_dir.clone()), Some(999), Some(OutputFormat::Json))
            .expect("prune no context");
        assert!(
            prune_no_context(Some(cache_dir), None, None)
                .expect_err("missing days")
                .to_string()
                .contains("day value")
        );
    }
}
