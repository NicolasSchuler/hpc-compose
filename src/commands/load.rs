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
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
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
