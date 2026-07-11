//! Compose-file pipeline loaders shared across commands.
//!
//! These helpers turn a compose file on disk into a [`Plan`], [`RuntimePlan`],
//! or [`EffectiveComposeConfig`] via the planner/prepare pipeline. They are
//! orchestration concerns (not presentation), so they live in the commands
//! layer rather than the output module.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use hpc_compose::context::ResourceProfile;
use hpc_compose::planner::{Plan, PlanOptions, build_plan_with_options};
use hpc_compose::runtime_plan::{RuntimePlan, build_runtime_plan};
use hpc_compose::spec::{ComposeSpec, EffectiveComposeConfig};

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

/// Rebuilds a runtime plan from the effective configuration persisted with a
/// submission record. The snapshot is already interpolated and has resource
/// profile/cache defaults resolved, so it must not be combined with today's
/// context values.
pub(crate) fn load_runtime_plan_from_effective_snapshot(
    original_path: &Path,
    snapshot_yaml: &str,
) -> Result<RuntimePlan> {
    let spec = ComposeSpec::load_effective_snapshot_from_str(original_path, snapshot_yaml)?;
    let project_dir = original_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let plan = build_plan_with_options(
        original_path,
        spec,
        PlanOptions {
            allow_missing_spec_path: true,
            project_dir_override: Some(project_dir),
            ..PlanOptions::default()
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn effective_snapshot(cache_dir: &Path, service_suffix: &str) -> String {
        format!(
            r#"name: historical
x-slurm:
  cache_dir: {:?}
services:
  app:
    image: docker://alpine:3.20
    command: ["true"]
{}
"#,
            cache_dir.display().to_string(),
            service_suffix
        )
    }

    #[test]
    fn effective_snapshot_preserves_literal_dollar_expressions() {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let original_path = tmpdir.path().join("compose.yaml");
        std::fs::write(&original_path, "services: {}\n").expect("placeholder compose");
        let snapshot = effective_snapshot(
            tmpdir.path(),
            "    environment:\n      HISTORICAL_LITERAL: $HOME",
        );

        let plan = load_runtime_plan_from_effective_snapshot(&original_path, &snapshot)
            .expect("an already-interpolated snapshot must not be interpolated again");

        assert_eq!(plan.name, "historical");
        assert_eq!(plan.ordered_services.len(), 1);
        assert!(
            plan.ordered_services[0]
                .environment
                .iter()
                .any(|(name, value)| name == "HISTORICAL_LITERAL" && value == "$HOME")
        );
    }

    #[test]
    fn effective_snapshot_does_not_require_original_compose_file() {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let missing_path = tmpdir.path().join("deleted-compose.yaml");
        let snapshot = effective_snapshot(tmpdir.path(), "");

        let plan = load_runtime_plan_from_effective_snapshot(&missing_path, &snapshot)
            .expect("persisted snapshot must survive removal of the original compose file");

        assert_eq!(plan.name, "historical");
        assert_eq!(plan.ordered_services[0].name, "app");
    }
}
