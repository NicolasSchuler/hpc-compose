use std::collections::BTreeMap;

use anyhow::{Result, bail};
use hpc_compose::context::ResolvedContext;
use hpc_compose::planner::{PlanOptions, build_plan_with_options};
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
use hpc_compose::spec::{
    CommandSpec, ComposeSpec, DependsOnSpec, EnvironmentSpec, RuntimeConfig, ServiceEnrootConfig,
    ServiceRuntimeConfig, ServiceSlurmConfig, ServiceSpec, SlurmConfig, SoftwareEnvConfig,
};

/// Shared resource flags accepted by ephemeral `run --image` and `shell`.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResourceCliOptions {
    pub resources: Option<String>,
    pub time: Option<String>,
    pub mem: Option<String>,
    pub cpus_per_task: Option<u32>,
    pub gpus: Option<u32>,
    pub partition: Option<String>,
    pub env: Vec<String>,
}

pub(super) fn parse_env_entries(entries: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for entry in entries {
        let Some((key, value)) = entry.split_once('=') else {
            bail!("--env entries must use KEY=VALUE syntax");
        };
        validate_cli_env_name(key)?;
        if value.contains('\0') {
            bail!("--env {key}=... must not contain null bytes");
        }
        out.insert(key.to_string(), value.to_string());
    }
    Ok(out)
}

fn validate_cli_env_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("--env contains an empty environment variable name");
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        bail!("--env {name}=... is not a safe environment variable name");
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        bail!("--env {name}=... is not a safe environment variable name");
    }
    Ok(())
}

pub(super) fn slurm_from_resource_options(
    job_name: &str,
    options: &ResourceCliOptions,
) -> Result<SlurmConfig> {
    if matches!(options.cpus_per_task, Some(0)) {
        bail!("--cpus-per-task must be greater than zero");
    }
    if matches!(options.gpus, Some(0)) {
        bail!("--gpus must be greater than zero");
    }
    Ok(SlurmConfig {
        job_name: Some(job_name.to_string()),
        resources: options.resources.clone(),
        time: options.time.clone(),
        mem: options.mem.clone(),
        cpus_per_task: options.cpus_per_task,
        gpus: options.gpus,
        partition: options.partition.clone(),
        ..SlurmConfig::default()
    })
}

pub(super) fn build_ephemeral_runtime_plan(
    context: &ResolvedContext,
    image: String,
    command: Vec<String>,
    options: &ResourceCliOptions,
) -> Result<RuntimePlan> {
    let environment = EnvironmentSpec::Map(parse_env_entries(&options.env)?);
    let service = ServiceSpec {
        image: Some(image),
        command: Some(CommandSpec::Vec(command)),
        entrypoint: None,
        script: None,
        environment,
        volumes: Vec::new(),
        working_dir: None,
        depends_on: DependsOnSpec::None,
        readiness: None,
        healthcheck: None,
        assertions: None,
        software_env: SoftwareEnvConfig::default(),
        slurm: ServiceSlurmConfig::default(),
        runtime: ServiceRuntimeConfig::default(),
        enroot: ServiceEnrootConfig::default(),
    };
    build_synthetic_service_plan(context, "hpc-compose-run", "run", service, options)
}

/// Builds a runtime plan for a single synthetic service, used by `run
/// --image` and `notebook`. The compose spec is constructed in memory from
/// *job_name*, the per-service *service_name*, the supplied [`ServiceSpec`],
/// and resource flags; no compose file is required on disk.
pub(crate) fn build_synthetic_service_plan(
    context: &ResolvedContext,
    job_name: &str,
    service_name: &str,
    service: ServiceSpec,
    options: &ResourceCliOptions,
) -> Result<RuntimePlan> {
    let mut services = BTreeMap::new();
    services.insert(service_name.to_string(), service);
    let spec = ComposeSpec {
        name: Some(job_name.to_string()),
        runtime: RuntimeConfig::default(),
        software_env: SoftwareEnvConfig::default(),
        slurm: slurm_from_resource_options(job_name, options)?,
        sweep: None,
        secrets: BTreeMap::new(),
        services,
    };
    let synthetic_path = context.cwd.join(format!("{job_name}.yaml"));
    let plan = build_plan_with_options(
        &synthetic_path,
        spec,
        PlanOptions {
            cache_dir_default: Some(context.cache_dir.value.clone()),
            resource_profiles: context.resource_profiles.clone(),
            project_dir_override: Some(context.cwd.clone()),
            allow_missing_spec_path: true,
        },
    )?;
    Ok(build_runtime_plan(&plan))
}

pub(super) fn push_slurm_srun_options(args: &mut Vec<String>, slurm: &SlurmConfig) {
    args.push(format!(
        "--job-name={}",
        slurm.job_name.as_deref().unwrap_or("hpc-compose-shell")
    ));
    if let Some(nodes) = slurm.nodes {
        args.push(format!("--nodes={nodes}"));
    }
    if let Some(ntasks) = slurm.ntasks.or(Some(1)) {
        args.push(format!("--ntasks={ntasks}"));
    }
    if let Some(ntasks_per_node) = slurm.ntasks_per_node {
        args.push(format!("--ntasks-per-node={ntasks_per_node}"));
    }
    if let Some(partition) = &slurm.partition {
        args.push(format!("--partition={partition}"));
    }
    if let Some(account) = &slurm.account {
        args.push(format!("--account={account}"));
    }
    if let Some(qos) = &slurm.qos {
        args.push(format!("--qos={qos}"));
    }
    if let Some(time) = &slurm.time {
        args.push(format!("--time={time}"));
    }
    if let Some(cpus) = slurm.cpus_per_task {
        args.push(format!("--cpus-per-task={cpus}"));
    }
    if let Some(mem) = &slurm.mem {
        args.push(format!("--mem={mem}"));
    }
    if let Some(gres) = &slurm.gres {
        args.push(format!("--gres={gres}"));
    } else if let Some(gpus) = slurm.gpus {
        args.push(format!("--gpus={gpus}"));
    }
    if let Some(gpus_per_node) = slurm.gpus_per_node {
        args.push(format!("--gpus-per-node={gpus_per_node}"));
    }
    if let Some(gpus_per_task) = slurm.gpus_per_task {
        args.push(format!("--gpus-per-task={gpus_per_task}"));
    }
    if let Some(cpus_per_gpu) = slurm.cpus_per_gpu {
        args.push(format!("--cpus-per-gpu={cpus_per_gpu}"));
    }
    if let Some(mem_per_gpu) = &slurm.mem_per_gpu {
        args.push(format!("--mem-per-gpu={mem_per_gpu}"));
    }
    if let Some(gpu_bind) = &slurm.gpu_bind {
        args.push(format!("--gpu-bind={gpu_bind}"));
    }
    if let Some(cpu_bind) = &slurm.cpu_bind {
        args.push(format!("--cpu-bind={cpu_bind}"));
    }
    if let Some(mem_bind) = &slurm.mem_bind {
        args.push(format!("--mem-bind={mem_bind}"));
    }
    if let Some(distribution) = &slurm.distribution {
        args.push(format!("--distribution={distribution}"));
    }
    if let Some(hint) = &slurm.hint {
        args.push(format!("--hint={hint}"));
    }
    if let Some(constraint) = &slurm.constraint {
        args.push(format!("--constraint={constraint}"));
    }
}

pub(super) fn push_slurm_salloc_options(args: &mut Vec<String>, slurm: &SlurmConfig) {
    args.push(format!(
        "--job-name={}",
        slurm.job_name.as_deref().unwrap_or("hpc-compose-alloc")
    ));
    if let Some(nodes) = slurm.nodes {
        args.push(format!("--nodes={nodes}"));
    }
    if let Some(ntasks) = slurm.ntasks {
        args.push(format!("--ntasks={ntasks}"));
    }
    if let Some(ntasks_per_node) = slurm.ntasks_per_node {
        args.push(format!("--ntasks-per-node={ntasks_per_node}"));
    }
    if let Some(partition) = &slurm.partition {
        args.push(format!("--partition={partition}"));
    }
    if let Some(account) = &slurm.account {
        args.push(format!("--account={account}"));
    }
    if let Some(qos) = &slurm.qos {
        args.push(format!("--qos={qos}"));
    }
    if let Some(time) = &slurm.time {
        args.push(format!("--time={time}"));
    }
    if let Some(cpus) = slurm.cpus_per_task {
        args.push(format!("--cpus-per-task={cpus}"));
    }
    if let Some(mem) = &slurm.mem {
        args.push(format!("--mem={mem}"));
    }
    if let Some(gres) = &slurm.gres {
        args.push(format!("--gres={gres}"));
    } else if let Some(gpus) = slurm.gpus {
        args.push(format!("--gpus={gpus}"));
    }
    if let Some(gpus_per_node) = slurm.gpus_per_node {
        args.push(format!("--gpus-per-node={gpus_per_node}"));
    }
    if let Some(gpus_per_task) = slurm.gpus_per_task {
        args.push(format!("--gpus-per-task={gpus_per_task}"));
    }
    if let Some(cpus_per_gpu) = slurm.cpus_per_gpu {
        args.push(format!("--cpus-per-gpu={cpus_per_gpu}"));
    }
    if let Some(mem_per_gpu) = &slurm.mem_per_gpu {
        args.push(format!("--mem-per-gpu={mem_per_gpu}"));
    }
    if let Some(gpu_bind) = &slurm.gpu_bind {
        args.push(format!("--gpu-bind={gpu_bind}"));
    }
    if let Some(cpu_bind) = &slurm.cpu_bind {
        args.push(format!("--cpu-bind={cpu_bind}"));
    }
    if let Some(mem_bind) = &slurm.mem_bind {
        args.push(format!("--mem-bind={mem_bind}"));
    }
    if let Some(distribution) = &slurm.distribution {
        args.push(format!("--distribution={distribution}"));
    }
    if let Some(hint) = &slurm.hint {
        args.push(format!("--hint={hint}"));
    }
    if let Some(constraint) = &slurm.constraint {
        args.push(format!("--constraint={constraint}"));
    }
    if let Some(dependency) = slurm.dependency_cli_value() {
        args.push(format!("--dependency={dependency}"));
    }
    args.extend(slurm.submit_args.iter().cloned());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_entries_require_safe_key_value_pairs() {
        let env =
            parse_env_entries(&["A=one".to_string(), "_B=two=three".to_string()]).expect("env");
        assert_eq!(env["A"], "one");
        assert_eq!(env["_B"], "two=three");

        let missing_equals = parse_env_entries(&["A".to_string()]).expect_err("syntax");
        assert!(missing_equals.to_string().contains("KEY=VALUE"));
        let bad_name = parse_env_entries(&["1A=value".to_string()]).expect_err("name");
        assert!(
            bad_name
                .to_string()
                .contains("safe environment variable name")
        );
        let bad_value = parse_env_entries(&["A=bad\0value".to_string()]).expect_err("null");
        assert!(
            bad_value
                .to_string()
                .contains("must not contain null bytes")
        );
    }

    #[test]
    fn resource_options_validate_positive_counts() {
        let cpus = ResourceCliOptions {
            cpus_per_task: Some(0),
            ..ResourceCliOptions::default()
        };
        assert!(
            slurm_from_resource_options("job", &cpus)
                .expect_err("zero cpus")
                .to_string()
                .contains("--cpus-per-task")
        );

        let gpus = ResourceCliOptions {
            gpus: Some(0),
            ..ResourceCliOptions::default()
        };
        assert!(
            slurm_from_resource_options("job", &gpus)
                .expect_err("zero gpus")
                .to_string()
                .contains("--gpus")
        );
    }

    #[test]
    fn slurm_arg_builders_preserve_shell_and_alloc_defaults() {
        let mut slurm = SlurmConfig {
            partition: Some("debug".to_string()),
            time: Some("00:05:00".to_string()),
            cpus_per_task: Some(2),
            gpus: Some(1),
            submit_args: vec!["--mail-type=END".to_string()],
            ..SlurmConfig::default()
        };

        let mut srun_args = Vec::new();
        push_slurm_srun_options(&mut srun_args, &slurm);
        assert_eq!(srun_args[0], "--job-name=hpc-compose-shell");
        assert!(srun_args.contains(&"--ntasks=1".to_string()));
        assert!(srun_args.contains(&"--partition=debug".to_string()));
        assert!(srun_args.contains(&"--gpus=1".to_string()));
        assert!(!srun_args.contains(&"--mail-type=END".to_string()));

        slurm.job_name = Some("alloc-job".to_string());
        slurm.ntasks = Some(4);
        let mut salloc_args = Vec::new();
        push_slurm_salloc_options(&mut salloc_args, &slurm);
        assert_eq!(salloc_args[0], "--job-name=alloc-job");
        assert!(salloc_args.contains(&"--ntasks=4".to_string()));
        assert!(salloc_args.contains(&"--mail-type=END".to_string()));
    }
}
