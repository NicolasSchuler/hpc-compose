use super::{
    DIST_ENV_NAMES, DIST_SLURM_RANK_ENV_NAMES, distributed_helpers_enabled, service_step_name,
};
use crate::planner::ExecutionSpec;
use crate::prepare::RuntimeService;
use crate::spec::RuntimeBackend;

/// Converts an [`ExecutionSpec`] into the argv used inside the container.
pub fn execution_argv(execution: &ExecutionSpec, working_dir: Option<&str>) -> Vec<String> {
    match (execution, working_dir) {
        (ExecutionSpec::ImageDefault, None) => Vec::new(),
        (ExecutionSpec::Shell(script), None) => {
            vec!["/bin/sh".into(), "-lc".into(), script.clone()]
        }
        (ExecutionSpec::Exec(argv), None) => argv.clone(),
        (ExecutionSpec::Shell(script), Some(dir)) => vec![
            "/bin/sh".into(),
            "-lc".into(),
            "cd \"$1\" && shift && exec /bin/sh -lc \"$1\"".into(),
            "hpc-compose".into(),
            dir.to_string(),
            script.clone(),
        ],
        (ExecutionSpec::Exec(argv), Some(dir)) => {
            let mut wrapped = vec![
                "/bin/sh".into(),
                "-lc".into(),
                "cd \"$1\" && shift && exec \"$@\"".into(),
                "hpc-compose".into(),
                dir.to_string(),
            ];
            wrapped.extend(argv.clone());
            wrapped
        }
        (ExecutionSpec::ImageDefault, Some(_)) => unreachable!("validated earlier"),
    }
}

/// Builds the `srun` command line for one runtime service.
pub fn build_srun_command(service: &RuntimeService) -> Vec<String> {
    build_srun_command_for_backend(service, RuntimeBackend::Pyxis)
}

/// Builds the `srun` command line for one runtime service under a backend.
pub fn build_srun_command_for_backend(
    service: &RuntimeService,
    backend: RuntimeBackend,
) -> Vec<String> {
    build_srun_command_for_backend_with_extra_container_env(service, backend, &[])
}

pub(super) fn build_srun_command_for_backend_with_extra_container_env(
    service: &RuntimeService,
    backend: RuntimeBackend,
    extra_container_env: &[String],
) -> Vec<String> {
    let mut args = vec![
        "srun".to_string(),
        format!("--nodes={}", service.placement.nodes),
        "--exact".to_string(),
        "--overlap".to_string(),
        format!("--job-name={}", service_step_name(&service.name)),
    ];
    if let Some(ntasks) = service.placement.ntasks {
        args.push(format!("--ntasks={ntasks}"));
    }
    if let Some(ntasks_per_node) = service.placement.ntasks_per_node {
        args.push(format!("--ntasks-per-node={ntasks_per_node}"));
    }
    if backend == RuntimeBackend::Pyxis && matches!(service.execution, ExecutionSpec::ImageDefault)
    {
        args.push("--container-entrypoint".to_string());
    }
    let mut env_names = vec![
        "HPC_COMPOSE_PRIMARY_NODE",
        "HPC_COMPOSE_NODE_COUNT",
        "HPC_COMPOSE_NODELIST",
        "HPC_COMPOSE_NODELIST_FILE",
        "HPC_COMPOSE_HOOK_PHASE",
        "HPC_COMPOSE_SERVICE_PRIMARY_NODE",
        "HPC_COMPOSE_SERVICE_NODE_COUNT",
        "HPC_COMPOSE_SERVICE_NODELIST",
        "HPC_COMPOSE_SERVICE_NODELIST_FILE",
        "HPC_COMPOSE_SERVICE_NAME",
        "HPC_COMPOSE_SERVICE_LOG",
        "HPC_COMPOSE_SERVICE_EXIT_CODE",
        "HPC_COMPOSE_RESUME_DIR",
        "HPC_COMPOSE_ATTEMPT",
        "HPC_COMPOSE_IS_RESUME",
    ];
    if service.slurm.mpi.is_some() {
        env_names.extend([
            "HPC_COMPOSE_MPI_HOSTFILE",
            "HPC_COMPOSE_MPI_IMPLEMENTATION",
            "HPC_COMPOSE_MPI_PROFILE",
            "HPC_COMPOSE_MPI_TYPE",
            "PMI_APPNUM",
            "PMI_CONTROL_PORT",
            "PMI_FD",
            "PMI_ID",
            "PMI_JOBID",
            "PMI_KVS",
            "PMI_PORT",
            "PMI_RANK",
            "PMI_SIZE",
            "PMI_SPAWNED",
            "PMI_UNIVERSE_SIZE",
            "PMI2_ID",
            "PMI2_JOBID",
            "PMI2_RANK",
            "PMI2_SIZE",
            "PMIX_DSTORE_21_BASE_PATH",
            "PMIX_GDS_MODULE",
            "PMIX_HOSTNAME",
            "PMIX_MCA_gds",
            "PMIX_NAMESPACE",
            "PMIX_PTL_MODULE",
            "PMIX_RANK",
            "PMIX_SECURITY_MODE",
            "PMIX_SERVER_URI",
            "PMIX_SERVER_URI2",
            "PMIX_SYSTEM_TMPDIR",
            "SLURM_LOCALID",
            "SLURM_NODEID",
            "SLURM_NTASKS",
            "SLURM_PROCID",
            "SLURM_STEP_NUM_TASKS",
            "SLURM_STEP_TASKS_PER_NODE",
            "SLURM_TASKS_PER_NODE",
        ]);
    }
    if distributed_helpers_enabled(service) {
        env_names.extend(DIST_ENV_NAMES.iter().copied());
        env_names.extend(DIST_SLURM_RANK_ENV_NAMES.iter().copied());
    }
    env_names.extend(service.environment.iter().map(|(name, _)| name.as_str()));
    env_names.extend(service.slurm.software_env.env.keys().map(String::as_str));
    env_names.extend(extra_container_env.iter().map(String::as_str));
    env_names.sort_unstable();
    env_names.dedup();
    if backend == RuntimeBackend::Pyxis {
        args.push(format!("--container-env={}", env_names.join(",")));
    }
    if let Some(cpus) = service.slurm.cpus_per_task {
        args.push(format!("--cpus-per-task={cpus}"));
    }
    if let Some(gres) = &service.slurm.gres {
        args.push(format!("--gres={gres}"));
    } else if let Some(gpus) = service.slurm.gpus {
        args.push(format!("--gpus={gpus}"));
    }
    if let Some(gpus_per_node) = service.slurm.gpus_per_node {
        args.push(format!("--gpus-per-node={gpus_per_node}"));
    }
    if let Some(gpus_per_task) = service.slurm.gpus_per_task {
        args.push(format!("--gpus-per-task={gpus_per_task}"));
    }
    if let Some(cpus_per_gpu) = service.slurm.cpus_per_gpu {
        args.push(format!("--cpus-per-gpu={cpus_per_gpu}"));
    }
    if let Some(mem_per_gpu) = &service.slurm.mem_per_gpu {
        args.push(format!("--mem-per-gpu={mem_per_gpu}"));
    }
    if let Some(gpu_bind) = &service.slurm.gpu_bind {
        args.push(format!("--gpu-bind={gpu_bind}"));
    }
    if let Some(cpu_bind) = &service.slurm.cpu_bind {
        args.push(format!("--cpu-bind={cpu_bind}"));
    }
    if let Some(mem_bind) = &service.slurm.mem_bind {
        args.push(format!("--mem-bind={mem_bind}"));
    }
    if let Some(distribution) = &service.slurm.distribution {
        args.push(format!("--distribution={distribution}"));
    }
    if let Some(hint) = &service.slurm.hint {
        args.push(format!("--hint={hint}"));
    }
    if let Some(mpi) = &service.slurm.mpi {
        args.push(format!("--mpi={}", mpi.mpi_type.as_srun_value()));
    }
    args.extend(service.slurm.extra_srun_args.clone());
    args
}

/// Builds the user-visible `srun` command line for one runtime service.
pub fn display_srun_command(service: &RuntimeService) -> Vec<String> {
    display_srun_command_for_backend(service, RuntimeBackend::Pyxis)
}

/// Builds the user-visible `srun` command line under a backend.
pub fn display_srun_command_for_backend(
    service: &RuntimeService,
    backend: RuntimeBackend,
) -> Vec<String> {
    let mut args = build_srun_command_for_backend(service, backend);
    if let Some(indices) = &service.placement.node_indices {
        args.push(format!(
            "--nodelist=<allocation-indices:{}>",
            indices
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    } else if service.placement.pin_to_primary_node {
        args.push("--nodelist=$HPC_COMPOSE_PRIMARY_NODE".to_string());
    }
    if !service.placement.exclude_indices.is_empty() {
        args.push(format!(
            "--exclude=<allocation-indices:{}>",
            service
                .placement
                .exclude_indices
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    args
}
