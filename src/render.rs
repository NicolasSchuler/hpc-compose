//! Batch-script rendering for prepared runtime plans.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Result;

use crate::cluster::ClusterProfile;
use crate::planner::{ExecutionSpec, ServicePlacementMode};
use crate::prepare::{RuntimePlan, RuntimeService};
use crate::spec::{
    ArtifactCollectPolicy, DependencyCondition, MetricsCollector, ReadinessSpec,
    RendezvousRegisterConfig, RuntimeBackend, RuntimeCacheCleanupPolicy, RuntimeGpuPolicy,
    ScratchCleanupPolicy, ScratchScope, ServiceFailureMode, ServiceHookContext, ServiceHookEvent,
    SignalConfig, SlurmConfig, SoftwareEnvConfig,
};
use crate::tracked_paths;

mod artifact;
mod command;
mod local;
mod metrics;
mod rendezvous;
mod sbatch;
mod software_env;
mod stage;
mod text;

pub use command::{
    build_srun_command, build_srun_command_for_backend, display_srun_command,
    display_srun_command_for_backend, execution_argv,
};
pub use local::{LocalRenderOptions, render_local_script, render_local_script_with_options};
pub use text::log_file_name_for_service;

use artifact::render_artifact_helpers;
use command::build_srun_command_for_backend_with_extra_container_env;
use metrics::render_metrics_helpers;
use rendezvous::render_rendezvous_helpers;
use software_env::{
    effective_software_env_pairs, render_apply_software_env, render_software_env_helpers,
    software_env_export_names,
};
use stage::{has_hf_stage_in, render_hf_stage_in, render_stage_helpers};
use text::{bash_array_literal, flag, service_step_name, service_token, shell_quote};

const DIST_ENV_NAMES: &[&str] = &[
    "HPC_COMPOSE_DIST_MASTER_ADDR",
    "HPC_COMPOSE_DIST_MASTER_PORT",
    "HPC_COMPOSE_DIST_RDZV_ENDPOINT",
    "HPC_COMPOSE_DIST_NNODES",
    "HPC_COMPOSE_DIST_NODE_RANK",
    "HPC_COMPOSE_DIST_LOCAL_RANK",
    "HPC_COMPOSE_DIST_GLOBAL_RANK",
    "HPC_COMPOSE_DIST_NPROC_PER_NODE",
    "HPC_COMPOSE_DIST_WORLD_SIZE",
    "HPC_COMPOSE_DIST_HOSTFILE",
];

/// Launch-environment names exported for a service declaring
/// `x-slurm.parallelism`. Kept separate from `DIST_ENV_NAMES` because the
/// distributed helper family is gated on `nodes > 1`, whereas tensor/pipeline
/// sizes are emitted for single-node services too.
const PARALLELISM_ENV_NAMES: &[&str] = &["HPC_COMPOSE_TP_SIZE", "HPC_COMPOSE_PP_SIZE"];

const DIST_SLURM_RANK_ENV_NAMES: &[&str] = &[
    "SLURM_LOCALID",
    "SLURM_NODEID",
    "SLURM_NTASKS",
    "SLURM_PROCID",
    "SLURM_STEP_NUM_TASKS",
    "SLURM_STEP_TASKS_PER_NODE",
    "SLURM_TASKS_PER_NODE",
];

const DEFAULT_RDZV_PORT_BASE: u16 = 29_500;
const DEFAULT_RDZV_PORT_SPAN: u16 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DistributedRenderEnv {
    enabled: bool,
    nproc_per_node: u32,
    profile_env: Vec<(String, String)>,
    rdzv_port: Option<u16>,
    rdzv_port_base: u16,
    rdzv_port_span: u16,
}

/// Runtime executable paths to bake into rendered batch scripts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderOptions {
    /// Apptainer executable used by Apptainer-backed service steps.
    pub apptainer_bin: String,
    /// Singularity executable used by Singularity-backed service steps.
    pub singularity_bin: String,
    /// `huggingface-cli` used by `hf://` stage-in steps, executed cluster-side
    /// inside the Slurm allocation. Never invoked on the laptop.
    pub huggingface_cli_bin: String,
    /// Optional cluster profile used only for render-time distributed env wiring.
    pub cluster_profile: Option<ClusterProfile>,
    /// Resolved absolute *parent* of the per-job runtime root (the directory that
    /// will contain `<job_id>/`). When set, the rendered `JOB_ROOT` becomes a
    /// literal absolute path, so the running job no longer depends on
    /// `$SLURM_SUBMIT_DIR` being set and shared-visible. When `None` (e.g.
    /// dry-run previews via `inspect`/`spec render`), `JOB_ROOT` keeps the
    /// portable `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose` form.
    pub runtime_root: Option<PathBuf>,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            apptainer_bin: "apptainer".to_string(),
            singularity_bin: "singularity".to_string(),
            huggingface_cli_bin: "huggingface-cli".to_string(),
            cluster_profile: None,
            runtime_root: None,
        }
    }
}

/// Renders the complete `sbatch` script for a runtime plan.
pub fn render_script(plan: &RuntimePlan) -> Result<String> {
    render_script_with_options(plan, &RenderOptions::default())
}

/// Returns render-time distributed helper environment names for a service.
#[must_use]
pub fn distributed_environment_names_for_service(
    service: &RuntimeService,
    cluster_profile: Option<&ClusterProfile>,
) -> Vec<String> {
    if !distributed_helpers_enabled(service) {
        return Vec::new();
    }
    let mut names = DIST_ENV_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    names.extend(
        distributed_profile_env_for_service(cluster_profile, service)
            .into_iter()
            .map(|(name, _)| name),
    );
    names.sort();
    names.dedup();
    names
}

/// Returns the tensor/pipeline parallelism env names a service injects.
///
/// Unlike [`distributed_environment_names_for_service`], this is not gated on
/// `nodes > 1`: `x-slurm.parallelism` exports `HPC_COMPOSE_TP_SIZE`/
/// `HPC_COMPOSE_PP_SIZE` for single-node services too. Returns an empty vector
/// when the service does not declare parallelism.
#[must_use]
pub fn parallelism_environment_names_for_service(service: &RuntimeService) -> Vec<String> {
    if service.slurm.parallelism.is_none() {
        return Vec::new();
    }
    PARALLELISM_ENV_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

fn rendezvous_environment_names(names: &[String]) -> Vec<String> {
    let mut env = vec![
        "HPC_COMPOSE_RDZV_NAME".to_string(),
        "HPC_COMPOSE_RDZV_URL".to_string(),
        "HPC_COMPOSE_RDZV_HOST".to_string(),
        "HPC_COMPOSE_RDZV_PORT".to_string(),
        "HPC_COMPOSE_RDZV_PROTOCOL".to_string(),
        "HPC_COMPOSE_RDZV_PATH".to_string(),
        "HPC_COMPOSE_RDZV_JOB_ID".to_string(),
        "HPC_COMPOSE_RDZV_SERVICE".to_string(),
    ];
    for name in names {
        let token = crate::rendezvous::env_token(name);
        env.extend([
            format!("HPC_COMPOSE_RDZV_{token}_NAME"),
            format!("HPC_COMPOSE_RDZV_{token}_URL"),
            format!("HPC_COMPOSE_RDZV_{token}_HOST"),
            format!("HPC_COMPOSE_RDZV_{token}_PORT"),
            format!("HPC_COMPOSE_RDZV_{token}_PROTOCOL"),
            format!("HPC_COMPOSE_RDZV_{token}_PATH"),
            format!("HPC_COMPOSE_RDZV_{token}_JOB_ID"),
            format!("HPC_COMPOSE_RDZV_{token}_SERVICE"),
        ]);
    }
    env.sort();
    env.dedup();
    env
}

fn distributed_render_env(
    service: &RuntimeService,
    slurm: &SlurmConfig,
    cluster_profile: Option<&ClusterProfile>,
) -> DistributedRenderEnv {
    let enabled = distributed_helpers_enabled(service);
    let profile = cluster_profile.map(|profile| &profile.distributed);
    DistributedRenderEnv {
        enabled,
        nproc_per_node: derive_nproc_per_node(service, slurm),
        profile_env: distributed_profile_env_for_service(cluster_profile, service),
        rdzv_port: profile.and_then(|distributed| distributed.rdzv_port),
        rdzv_port_base: profile
            .and_then(|distributed| distributed.rdzv_port_base)
            .unwrap_or(DEFAULT_RDZV_PORT_BASE),
        rdzv_port_span: profile
            .and_then(|distributed| distributed.rdzv_port_span)
            .unwrap_or(DEFAULT_RDZV_PORT_SPAN),
    }
}

fn distributed_helpers_enabled(service: &RuntimeService) -> bool {
    service.placement.nodes > 1
}

fn distributed_profile_env_for_service(
    cluster_profile: Option<&ClusterProfile>,
    service: &RuntimeService,
) -> Vec<(String, String)> {
    if !distributed_helpers_enabled(service) {
        return Vec::new();
    }
    let Some(profile) = cluster_profile else {
        return Vec::new();
    };
    profile
        .distributed
        .env
        .iter()
        .filter(|(name, _)| {
            !service
                .environment
                .iter()
                .any(|(service_name, _)| service_name == *name)
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn derive_nproc_per_node(service: &RuntimeService, slurm: &SlurmConfig) -> u32 {
    nproc_per_node_from_env(service)
        .or(service.slurm.gpus_per_node)
        .or(slurm.gpus_per_node)
        .or_else(|| service.slurm.gres.as_deref().and_then(parse_gres_gpu_count))
        .or_else(|| slurm.gres.as_deref().and_then(parse_gres_gpu_count))
        .or_else(|| gpus_evenly_per_node(service.slurm.gpus, service.placement.nodes))
        .or_else(|| gpus_evenly_per_node(slurm.gpus, service.placement.nodes))
        .or(service.placement.ntasks_per_node)
        .unwrap_or(1)
}

fn nproc_per_node_from_env(service: &RuntimeService) -> Option<u32> {
    ["HPC_COMPOSE_DIST_NPROC_PER_NODE", "NPROC_PER_NODE"]
        .into_iter()
        .find_map(|key| {
            service
                .environment
                .iter()
                .find(|(name, _)| name == key)
                .and_then(|(_, value)| value.parse::<u32>().ok())
                .filter(|value| *value > 0)
        })
}

fn gpus_evenly_per_node(gpus: Option<u32>, nodes: u32) -> Option<u32> {
    let gpus = gpus?;
    (nodes > 0 && gpus > 0 && gpus % nodes == 0).then_some(gpus / nodes)
}

fn parse_gres_gpu_count(gres: &str) -> Option<u32> {
    gres.split(',').find_map(|part| {
        let part = part.trim();
        if !part.to_ascii_lowercase().contains("gpu") {
            return None;
        }
        let count = part
            .split(':')
            .next_back()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(1);
        (count > 0).then_some(count)
    })
}

/// Renders the complete `sbatch` script for a runtime plan with explicit
/// runtime executable paths.
pub fn render_script_with_options(plan: &RuntimePlan, options: &RenderOptions) -> Result<String> {
    let metrics_enabled = plan.slurm.metrics_enabled();
    let artifacts_enabled = plan.slurm.artifacts_enabled();
    let resume_enabled = plan.slurm.resume_dir().is_some();
    let scratch_enabled = plan.slurm.scratch.is_some();
    let stage_enabled = !plan.slurm.stage_in.is_empty() || !plan.slurm.stage_out.is_empty();
    let hf_stage_enabled = has_hf_stage_in(plan);
    let transfer_helpers_enabled = scratch_enabled || stage_enabled;
    let mpi_enabled = plan
        .ordered_services
        .iter()
        .any(|service| service.slurm.mpi.is_some());
    let distributed_env_enabled = plan
        .ordered_services
        .iter()
        .any(distributed_helpers_enabled);
    let hooks_enabled = plan.ordered_services.iter().any(|service| {
        service.slurm.prologue.is_some()
            || service.slurm.epilogue.is_some()
            || !service.slurm.hooks.is_empty()
    });
    let assertions_enabled = plan
        .ordered_services
        .iter()
        .any(|service| service.assertions.is_some());
    let rendezvous_client_names = plan
        .slurm
        .rendezvous
        .as_ref()
        .map(|rendezvous| rendezvous.discover.clone())
        .unwrap_or_default();
    let rendezvous_enabled = !rendezvous_client_names.is_empty()
        || plan.ordered_services.iter().any(|service| {
            service
                .slurm
                .rendezvous
                .as_ref()
                .and_then(|rendezvous| rendezvous.register.as_ref())
                .is_some()
        });
    let software_env_enabled = !plan.slurm.software_env.is_empty()
        || plan
            .ordered_services
            .iter()
            .any(|service| !service.slurm.software_env.is_empty());
    let resume_host_path = plan.slurm.resume_dir().unwrap_or("");
    // Only `Batch` (`B:`) signal delivery with a non-teardown signal needs a
    // forwarding trap; `Step` delivery reaches the job step directly and INT/TERM
    // are already handled by the existing teardown traps.
    let signal_forward_target = plan
        .slurm
        .signal
        .as_ref()
        .and_then(SignalConfig::extra_trap_target);
    let artifact_bundles = plan
        .slurm
        .artifacts
        .as_ref()
        .map(|artifacts| artifacts.normalized_bundles())
        .unwrap_or_default();
    let artifact_bundle_names = artifact_bundles.keys().cloned().collect::<Vec<_>>();
    let mut artifact_pattern_bundles = Vec::new();
    let mut artifact_source_patterns = Vec::new();
    for (bundle, patterns) in &artifact_bundles {
        for pattern in patterns {
            artifact_pattern_bundles.push(bundle.clone());
            artifact_source_patterns.push(pattern.clone());
        }
    }
    let mut dependents_by_service = BTreeMap::<String, Vec<String>>::new();
    for service in &plan.ordered_services {
        for dependency in &service.depends_on {
            dependents_by_service
                .entry(dependency.name.clone())
                .or_default()
                .push(service.name.clone());
        }
    }
    let mut out = String::new();
    out.push_str("#!/bin/bash\n");
    out.push_str("# shellcheck shell=bash\n");
    out.push_str("# shellcheck disable=SC2016\n");
    out.push_str(&format!(
        "# Generated by hpc-compose for job {}\n",
        plan.name
    ));
    sbatch::push_directive(&mut out, "job-name", &plan.name);
    sbatch::push_directive(&mut out, "nodes", plan.slurm.allocation_nodes());
    if let Some(ntasks) = plan.slurm.ntasks {
        sbatch::push_directive(&mut out, "ntasks", ntasks);
    }
    if let Some(ntasks_per_node) = plan.slurm.ntasks_per_node {
        sbatch::push_directive(&mut out, "ntasks-per-node", ntasks_per_node);
    }
    if let Some(partition) = &plan.slurm.partition {
        sbatch::push_directive(&mut out, "partition", partition);
    }
    if let Some(account) = &plan.slurm.account {
        sbatch::push_directive(&mut out, "account", account);
    }
    if let Some(qos) = &plan.slurm.qos {
        sbatch::push_directive(&mut out, "qos", qos);
    }
    if let Some(reservation) = &plan.slurm.reservation {
        sbatch::push_directive(&mut out, "reservation", reservation);
    }
    if let Some(licenses) = &plan.slurm.licenses {
        sbatch::push_directive(&mut out, "licenses", licenses);
    }
    if let Some(mail_user) = plan.slurm.notify_email_recipient() {
        sbatch::push_directive(&mut out, "mail-user", mail_user);
    }
    if let Some(mail_type) = plan.slurm.notify_mail_type_value() {
        sbatch::push_directive(&mut out, "mail-type", mail_type);
    }
    if let Some(time) = &plan.slurm.time {
        sbatch::push_directive(&mut out, "time", time);
    }
    if let Some(cpus) = plan.slurm.cpus_per_task {
        sbatch::push_directive(&mut out, "cpus-per-task", cpus);
    }
    if let Some(mem) = &plan.slurm.mem {
        sbatch::push_directive(&mut out, "mem", mem);
    }
    if let Some(gres) = &plan.slurm.gres {
        sbatch::push_directive(&mut out, "gres", gres);
    } else if let Some(gpus) = plan.slurm.gpus {
        sbatch::push_directive(&mut out, "gpus", gpus);
    }
    if let Some(gpus_per_node) = plan.slurm.gpus_per_node {
        sbatch::push_directive(&mut out, "gpus-per-node", gpus_per_node);
    }
    if let Some(gpus_per_task) = plan.slurm.gpus_per_task {
        sbatch::push_directive(&mut out, "gpus-per-task", gpus_per_task);
    }
    if let Some(cpus_per_gpu) = plan.slurm.cpus_per_gpu {
        sbatch::push_directive(&mut out, "cpus-per-gpu", cpus_per_gpu);
    }
    if let Some(mem_per_gpu) = &plan.slurm.mem_per_gpu {
        sbatch::push_directive(&mut out, "mem-per-gpu", mem_per_gpu);
    }
    if let Some(gpu_bind) = &plan.slurm.gpu_bind {
        sbatch::push_directive(&mut out, "gpu-bind", gpu_bind);
    }
    if let Some(cpu_bind) = &plan.slurm.cpu_bind {
        sbatch::push_directive(&mut out, "cpu-bind", cpu_bind);
    }
    if let Some(mem_bind) = &plan.slurm.mem_bind {
        sbatch::push_directive(&mut out, "mem-bind", mem_bind);
    }
    if let Some(distribution) = &plan.slurm.distribution {
        sbatch::push_directive(&mut out, "distribution", distribution);
    }
    if let Some(hint) = &plan.slurm.hint {
        sbatch::push_directive(&mut out, "hint", hint);
    }
    if let Some(constraint) = &plan.slurm.constraint {
        sbatch::push_directive(&mut out, "constraint", constraint);
    }
    if let Some(output) = &plan.slurm.output {
        sbatch::push_directive(&mut out, "output", output);
    } else if let Some(runtime_root) = &options.runtime_root {
        // Real submissions only: bake a literal absolute --output under a hidden,
        // job-id-free parent the CLI pre-creates host-side before sbatch (Slurm
        // opens --output before the script body runs). Keep the basename
        // independent of raw job names because %x may contain path separators.
        // Previews (runtime_root == None) keep the Slurm default so committed
        // example renders stay machine-independent.
        let default_output = format!(
            "{}/{}/{}",
            runtime_root.display(),
            tracked_paths::LOGS_DIR_NAME,
            tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN
        );
        sbatch::push_directive(&mut out, "output", &default_output);
    }
    if let Some(error) = &plan.slurm.error {
        sbatch::push_directive(&mut out, "error", error);
    }
    if let Some(chdir) = &plan.slurm.chdir {
        sbatch::push_directive(&mut out, "chdir", chdir);
    }
    if let Some(array) = &plan.slurm.array {
        sbatch::push_directive(&mut out, "array", array);
    }
    if let Some(requeue) = plan.slurm.requeue {
        sbatch::push_bare_directive(&mut out, if requeue { "requeue" } else { "no-requeue" });
    }
    if let Some(signal_value) = plan.slurm.signal_directive_value() {
        sbatch::push_directive(&mut out, "signal", signal_value);
    }
    for arg in &plan.slurm.submit_args {
        sbatch::push_raw_directive(&mut out, arg);
    }
    if let Some(burst_buffer) = &plan.slurm.burst_buffer {
        for directive in &burst_buffer.directives {
            out.push_str(directive);
            out.push('\n');
        }
    }
    out.push_str("\nset -euo pipefail\n\n");
    out.push_str("BACKEND=\"${HPC_COMPOSE_BACKEND_OVERRIDE:-slurm}\"\n");
    out.push_str("JOB_STATUS=\"RUNNING\"\n");
    out.push_str("JOB_EXIT_CODE=\"\"\n");
    out.push_str("SUPERVISOR_PID=$$\n");
    out.push_str("RECEIVED_SIGNAL=\"\"\n");
    match &options.runtime_root {
        // Real submissions bake the resolved absolute runtime root so the job
        // body never depends on `$SLURM_SUBMIT_DIR` at compute-node runtime.
        Some(runtime_root) => out.push_str(&format!(
            "JOB_ROOT={}/\"${{SLURM_JOB_ID}}\"\n",
            shell_quote(&runtime_root.display().to_string())
        )),
        // Dry-run previews keep the portable, machine-independent form.
        None => out.push_str(&format!(
            "JOB_ROOT=\"${{SLURM_SUBMIT_DIR:-$PWD}}/{}/${{SLURM_JOB_ID}}\"\n",
            tracked_paths::METADATA_DIR_NAME
        )),
    }
    out.push_str(&format!("RESUME_ENABLED={}\n", flag(resume_enabled)));
    out.push_str(&format!(
        "RESUME_HOST_PATH={}\n",
        shell_quote(resume_host_path)
    ));
    out.push_str("RESUME_CONTAINER_PATH='/hpc-compose/resume'\n");
    out.push_str(&format!(
        "RESUME_COMPOSE_NAME={}\n",
        shell_quote(&plan.name)
    ));
    out.push_str("ATTEMPT=\"${SLURM_RESTART_COUNT:-0}\"\n");
    out.push_str("IS_RESUME=0\n");
    out.push_str("if [[ \"$RESUME_ENABLED\" == \"1\" ]]; then\n");
    out.push_str(&format!(
        "  JOB_TMP=\"$JOB_ROOT/{}/$ATTEMPT\"\n",
        tracked_paths::ATTEMPTS_DIR_NAME
    ));
    out.push_str(&format!(
        "  RESUME_META_DIR=\"$RESUME_HOST_PATH/{}\"\n",
        tracked_paths::RESUME_METADATA_DIR_NAME
    ));
    out.push_str(&format!(
        "  RESUME_META_FILE=\"$RESUME_META_DIR/{}\"\n",
        tracked_paths::LATEST_RECORD_FILE_NAME
    ));
    out.push_str("  mkdir -p \"$RESUME_HOST_PATH\" \"$RESUME_META_DIR\"\n");
    out.push_str("  if (( ATTEMPT > 0 )) || [[ -f \"$RESUME_META_FILE\" ]]; then\n");
    out.push_str("    IS_RESUME=1\n");
    out.push_str("  fi\n");
    out.push_str("else\n");
    out.push_str("  JOB_TMP=\"$JOB_ROOT\"\n");
    out.push_str("  RESUME_META_DIR=\"\"\n");
    out.push_str("  RESUME_META_FILE=\"\"\n");
    out.push_str("fi\n");
    out.push_str(&format!(
        "ALLOCATION_DIR=\"$JOB_TMP/{}\"\n",
        tracked_paths::ALLOCATION_DIR_NAME
    ));
    out.push_str(&format!(
        "PRIMARY_NODE_FILE=\"$ALLOCATION_DIR/{}\"\n",
        tracked_paths::PRIMARY_NODE_FILE_NAME
    ));
    out.push_str(&format!(
        "NODELIST_FILE=\"$ALLOCATION_DIR/{}\"\n",
        tracked_paths::NODELIST_FILE_NAME
    ));
    out.push_str(&format!(
        "SERVICE_NODELIST_DIR=\"$ALLOCATION_DIR/{}\"\n",
        tracked_paths::SERVICE_NODELISTS_DIR_NAME
    ));
    out.push_str(&format!(
        "SERVICE_NODELIST_CONTAINER_DIR=\"{}\"\n",
        tracked_paths::under_job_container_dir(&format!(
            "{}/{}",
            tracked_paths::ALLOCATION_DIR_NAME,
            tracked_paths::SERVICE_NODELISTS_DIR_NAME
        ))
    ));
    if mpi_enabled {
        out.push_str(&format!(
            "MPI_HOSTFILE_DIR=\"$ALLOCATION_DIR/{}\"\n",
            tracked_paths::MPI_HOSTFILES_DIR_NAME
        ));
        out.push_str(&format!(
            "MPI_HOSTFILE_CONTAINER_DIR=\"{}\"\n",
            tracked_paths::under_job_container_dir(&format!(
                "{}/{}",
                tracked_paths::ALLOCATION_DIR_NAME,
                tracked_paths::MPI_HOSTFILES_DIR_NAME
            ))
        ));
    }
    if distributed_env_enabled {
        out.push_str(&format!(
            "DIST_HOSTFILE_DIR=\"$ALLOCATION_DIR/{}\"\n",
            tracked_paths::DISTRIBUTED_HOSTFILES_DIR_NAME
        ));
        out.push_str(&format!(
            "DIST_HOSTFILE_CONTAINER_DIR=\"{}\"\n",
            tracked_paths::under_job_container_dir(&format!(
                "{}/{}",
                tracked_paths::ALLOCATION_DIR_NAME,
                tracked_paths::DISTRIBUTED_HOSTFILES_DIR_NAME
            ))
        ));
    }
    out.push_str(&format!(
        "LOG_DIR=\"$JOB_TMP/{}\"\n",
        tracked_paths::LOGS_DIR_NAME
    ));
    if hooks_enabled {
        out.push_str(&format!(
            "HOOKS_DIR=\"$JOB_TMP/{}\"\n",
            tracked_paths::HOOKS_DIR_NAME
        ));
        out.push_str(&format!(
            "HOOKS_CONTAINER_DIR=\"{}\"\n",
            tracked_paths::under_job_container_dir(tracked_paths::HOOKS_DIR_NAME)
        ));
    }
    out.push_str(&format!(
        "STATE_FILE=\"$JOB_TMP/{}\"\n",
        tracked_paths::STATE_FILE_NAME
    ));
    out.push_str(&format!(
        "SERVICE_EXIT_MARKER_DIR=\"$JOB_TMP/{}\"\n",
        tracked_paths::SERVICE_EXITS_DIR_NAME
    ));
    if artifacts_enabled {
        out.push_str(&format!(
            "ARTIFACTS_DIR=\"$JOB_TMP/{}\"\n",
            tracked_paths::ARTIFACTS_DIR_NAME
        ));
        out.push_str(&format!(
            "ARTIFACTS_PAYLOAD_DIR=\"$ARTIFACTS_DIR/{}\"\n",
            tracked_paths::ARTIFACT_PAYLOAD_DIR_NAME
        ));
        out.push_str(&format!(
            "ARTIFACTS_MANIFEST_FILE=\"$ARTIFACTS_DIR/{}\"\n",
            tracked_paths::ARTIFACT_MANIFEST_FILE_NAME
        ));
    }
    if metrics_enabled {
        out.push_str(&format!(
            "METRICS_DIR=\"$JOB_TMP/{}\"\n",
            tracked_paths::METRICS_DIR_NAME
        ));
        out.push_str("METRICS_META_FILE=\"$METRICS_DIR/meta.json\"\n");
        out.push_str("GPU_METRICS_FILE=\"$METRICS_DIR/gpu.jsonl\"\n");
        out.push_str("GPU_PROCESSES_FILE=\"$METRICS_DIR/gpu_processes.jsonl\"\n");
        out.push_str("SLURM_METRICS_FILE=\"$METRICS_DIR/slurm.jsonl\"\n");
        out.push_str("CPU_METRICS_FILE=\"$METRICS_DIR/cpu.jsonl\"\n");
        out.push_str("METRICS_DIAGNOSTICS_DIR=\"$METRICS_DIR/diagnostics\"\n");
    }
    out.push_str(&format!(
        "CACHE_ROOT={}\n",
        shell_quote(&plan.cache_dir.display().to_string())
    ));
    if let Some(scratch) = &plan.slurm.scratch {
        out.push_str(&format!("SCRATCH_BASE={}\n", shell_quote(&scratch.base)));
        out.push_str(&format!(
            "SCRATCH_CONTAINER_PATH={}\n",
            shell_quote(&scratch.mount)
        ));
        out.push_str(&format!(
            "SCRATCH_SCOPE={}\n",
            shell_quote(match scratch.scope {
                ScratchScope::Shared => "shared",
                ScratchScope::NodeLocal => "node_local",
            })
        ));
        out.push_str("SCRATCH_HOST_PATH=\"$SCRATCH_BASE/${SLURM_JOB_ID}\"\n");
        out.push_str(&format!(
            "SCRATCH_CLEANUP_POLICY={}\n",
            shell_quote(match scratch.cleanup {
                ScratchCleanupPolicy::Always => "always",
                ScratchCleanupPolicy::OnSuccess => "on_success",
                ScratchCleanupPolicy::Never => "never",
            })
        ));
    }
    let enroot_runtime = tracked_paths::ENROOT_RUNTIME_DIR_NAME;
    out.push_str(&format!(
        "export ENROOT_CACHE_PATH=\"$CACHE_ROOT/{enroot_runtime}/${{SLURM_JOB_ID}}/cache\"\n"
    ));
    out.push_str(&format!(
        "export ENROOT_DATA_PATH=\"$CACHE_ROOT/{enroot_runtime}/${{SLURM_JOB_ID}}/data\"\n"
    ));
    out.push_str(&format!(
        "export ENROOT_TEMP_PATH=\"$CACHE_ROOT/{enroot_runtime}/${{SLURM_JOB_ID}}/tmp\"\n"
    ));
    out.push_str(&format!(
        "RUNTIME_CACHE_CLEANUP_POLICY={}\n",
        shell_quote(match plan.slurm.cleanup.runtime_cache {
            RuntimeCacheCleanupPolicy::Always => "always",
            RuntimeCacheCleanupPolicy::OnSuccess => "on_success",
            RuntimeCacheCleanupPolicy::Never => "never",
        })
    ));
    // Always defined (the enroot exports above are unconditional); the cleanup
    // trap calls it on every exit path. Default policy `never` makes it a no-op,
    // deferring to host-side `clean`/`down` reaping.
    out.push_str("cleanup_runtime_cache() {\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  case \"$RUNTIME_CACHE_CLEANUP_POLICY\" in\n");
    out.push_str("    never) return 0 ;;\n");
    out.push_str("    on_success) (( exit_code == 0 )) || return 0 ;;\n");
    out.push_str("  esac\n");
    out.push_str("  # Per-job enroot dirs are namespaced by ${SLURM_JOB_ID}; rm -rf is\n");
    out.push_str("  # scoped to this job and never touches the shared $CACHE_ROOT root.\n");
    out.push_str(
        "  rm -rf \"${ENROOT_CACHE_PATH:-}\" \"${ENROOT_DATA_PATH:-}\" \"${ENROOT_TEMP_PATH:-}\"\n",
    );
    out.push_str("}\n");
    out.push_str("mkdir -p \"$LOG_DIR\" \"$ALLOCATION_DIR\" \"$SERVICE_NODELIST_DIR\" \"$SERVICE_EXIT_MARKER_DIR\"");
    if hooks_enabled {
        out.push_str(" \"$HOOKS_DIR\"");
    }
    if artifacts_enabled {
        out.push_str(" \"$ARTIFACTS_DIR\"");
    }
    if metrics_enabled {
        out.push_str(" \"$METRICS_DIR\"");
    }
    if mpi_enabled {
        out.push_str(" \"$MPI_HOSTFILE_DIR\"");
    }
    if distributed_env_enabled {
        out.push_str(" \"$DIST_HOSTFILE_DIR\"");
    }
    if rendezvous_enabled {
        out.push_str(" \"$CACHE_ROOT/rendezvous\"");
    }
    out.push_str(" \"$ENROOT_CACHE_PATH\" \"$ENROOT_DATA_PATH\" \"$ENROOT_TEMP_PATH\"\n\n");

    out.push_str("SERVICE_PIDS=()\n");
    out.push_str("SERVICE_NAMES=()\n");
    out.push_str("SERVICE_STEP_NAMES=()\n");
    out.push_str("SERVICE_LOG_PATHS=()\n");
    out.push_str("SERVICE_HEALTHY=()\n");
    out.push_str("SERVICE_COMPLETED_SUCCESSFULLY=()\n");
    out.push_str("SERVICE_READINESS_CONFIGURED=()\n");
    out.push_str("SERVICE_FAILURE_POLICY_MODE=()\n");
    out.push_str("SERVICE_MAX_RESTARTS=()\n");
    out.push_str("SERVICE_BACKOFF_SECONDS=()\n");
    out.push_str("SERVICE_WINDOW_SECONDS=()\n");
    out.push_str("SERVICE_MAX_RESTARTS_IN_WINDOW=()\n");
    out.push_str("SERVICE_RESTART_COUNT=()\n");
    out.push_str("SERVICE_RESTART_FAILURES_IN_WINDOW=()\n");
    out.push_str("SERVICE_RESTART_FAILURE_TIMESTAMPS=()\n");
    out.push_str("SERVICE_LAST_EXIT_CODE=()\n");
    out.push_str("SERVICE_STARTED_AT=()\n");
    out.push_str("SERVICE_FINISHED_AT=()\n");
    out.push_str("SERVICE_FIRST_FAILURE_AT=()\n");
    out.push_str("SERVICE_FIRST_FAILURE_EXIT_CODE=()\n");
    out.push_str("SERVICE_FIRST_FAILURE_NODE=()\n");
    out.push_str("SERVICE_FIRST_FAILURE_RANK=()\n");
    out.push_str("SERVICE_PLACEMENT_MODE=()\n");
    out.push_str("SERVICE_STEP_NODES=()\n");
    out.push_str("SERVICE_STEP_NTASKS=()\n");
    out.push_str("SERVICE_STEP_NTASKS_PER_NODE=()\n");
    out.push_str("SERVICE_STEP_NODELIST=()\n");
    out.push_str("SERVICE_HOST_EPILOGUE_SCRIPTS=()\n");
    out.push_str("SERVICE_HOST_EPILOGUE_RAN=()\n");
    out.push_str("SERVICE_EVENT_HOOK_MANIFESTS=()\n");
    if rendezvous_enabled {
        out.push_str("SERVICE_RDZV_NAMES=()\n");
        out.push_str("SERVICE_RDZV_PORTS=()\n");
        out.push_str("SERVICE_RDZV_PROTOCOLS=()\n");
        out.push_str("SERVICE_RDZV_PATHS=()\n");
        out.push_str("SERVICE_RDZV_TTLS=()\n");
        out.push_str("SERVICE_RDZV_METADATA_JSON=()\n");
        out.push_str("SERVICE_RDZV_REGISTERED=()\n");
    }
    if assertions_enabled {
        out.push_str("SERVICE_ASSERT_EXIT_CODES=()\n");
        out.push_str("SERVICE_ASSERT_ARTIFACT_PATTERNS=()\n");
        out.push_str("SERVICE_ASSERT_MAX_DURATIONS=()\n");
        out.push_str("SERVICE_ASSERT_DURATIONS=()\n");
        out.push_str("SERVICE_ASSERT_STATUS=()\n");
        out.push_str("SERVICE_ASSERT_FAILURES=()\n");
    }
    out.push_str("ALLOCATION_NODES=()\n");
    out.push_str("SERVICE_LAUNCH_FNS=()\n");
    out.push_str("SERVICE_DEPENDENTS=()\n");
    out.push_str("CLEANING_UP=0\n");
    out.push_str("WAIT_HELPER_EXITED=0\n");
    out.push_str("WAIT_HELPER_EXIT_STATUS=\"\"\n");
    out.push_str("declare -A SERVICE_INDEX_BY_NAME=()\n\n");
    if rendezvous_enabled {
        out.push_str(&format!(
            "RDZV_CLIENT_NAMES={}\n",
            bash_array_literal(&rendezvous_client_names)
        ));
        out.push_str("RDZV_CLIENT_TIMEOUT_SECONDS=");
        out.push_str(
            &plan
                .slurm
                .rendezvous
                .as_ref()
                .and_then(|rendezvous| rendezvous.timeout_seconds)
                .unwrap_or(30)
                .to_string(),
        );
        out.push('\n');
        out.push_str(&format!(
            "RDZV_CLIENT_REQUIRED={}\n\n",
            flag(
                plan.slurm
                    .rendezvous
                    .as_ref()
                    .and_then(|rendezvous| rendezvous.require)
                    .unwrap_or(true)
            )
        ));
    }
    if artifacts_enabled {
        out.push_str(&format!(
            "ARTIFACTS_COLLECT_POLICY={}\n",
            shell_quote(match plan.slurm.artifacts_collect_policy() {
                ArtifactCollectPolicy::Always => "always",
                ArtifactCollectPolicy::OnSuccess => "on_success",
                ArtifactCollectPolicy::OnFailure => "on_failure",
            })
        ));
        out.push_str(&format!(
            "ARTIFACT_BUNDLE_NAMES={}\n",
            bash_array_literal(&artifact_bundle_names)
        ));
        out.push_str(&format!(
            "ARTIFACT_PATTERN_BUNDLES={}\n",
            bash_array_literal(&artifact_pattern_bundles)
        ));
        out.push_str(&format!(
            "ARTIFACT_SOURCE_PATTERNS={}\n\n",
            bash_array_literal(&artifact_source_patterns)
        ));
        out.push_str("ARTIFACT_COPIED_RELATIVE_PATHS=()\n");
        out.push_str("ARTIFACT_BUNDLE_MATCH_RECORDS=()\n");
        out.push_str("ARTIFACT_BUNDLE_COPIED_RECORDS=()\n");
        out.push_str("ARTIFACT_BUNDLE_WARNING_RECORDS=()\n");
        out.push_str("ARTIFACT_WARNINGS=()\n\n");
    }
    if metrics_enabled {
        let slurm_collector_enabled = plan
            .slurm
            .metrics_collectors()
            .contains(&MetricsCollector::Slurm);
        out.push_str(&format!(
            "METRICS_INTERVAL_SECONDS={}\n",
            plan.slurm.metrics_interval_seconds()
        ));
        out.push_str("SAMPLER_PID=\"\"\n");
        out.push_str("GPU_WARNING_EMITTED=0\n");
        out.push_str("SLURM_WARNING_EMITTED=0\n");
        out.push_str("CPU_WARNING_EMITTED=0\n");
        out.push_str(&format!(
            "GPU_COLLECTOR_ENABLED={}\n",
            flag(
                plan.slurm
                    .metrics_collectors()
                    .contains(&MetricsCollector::Gpu)
            )
        ));
        out.push_str(&format!(
            "SLURM_COLLECTOR_ENABLED={}\n",
            flag(slurm_collector_enabled)
        ));
        out.push_str(&format!(
            "CPU_COLLECTOR_ENABLED={}\n",
            flag(
                plan.slurm
                    .metrics_collectors()
                    .contains(&MetricsCollector::Cpu)
            )
        ));
        out.push_str("GPU_COLLECTOR_AVAILABLE=1\n");
        out.push_str("SLURM_COLLECTOR_AVAILABLE=1\n");
        out.push_str("CPU_COLLECTOR_AVAILABLE=1\n");
        out.push_str("GPU_COLLECTOR_NOTE=\"\"\n");
        out.push_str("SLURM_COLLECTOR_NOTE=\"\"\n");
        out.push_str("CPU_COLLECTOR_NOTE=\"\"\n");
        out.push_str("GPU_COLLECTOR_LAST_SAMPLED_AT=\"\"\n");
        out.push_str("SLURM_COLLECTOR_LAST_SAMPLED_AT=\"\"\n");
        out.push_str("CPU_COLLECTOR_LAST_SAMPLED_AT=\"\"\n\n");
        out.push_str("if [[ -n \"${HPC_COMPOSE_SLURM_COLLECTOR_ENABLED_OVERRIDE:-}\" ]]; then\n");
        out.push_str(
            "  SLURM_COLLECTOR_ENABLED=\"$HPC_COMPOSE_SLURM_COLLECTOR_ENABLED_OVERRIDE\"\n",
        );
        out.push_str("fi\n");
        out.push_str("if [[ -n \"${HPC_COMPOSE_SLURM_COLLECTOR_NOTE_OVERRIDE:-}\" ]]; then\n");
        out.push_str("  SLURM_COLLECTOR_NOTE=\"$HPC_COMPOSE_SLURM_COLLECTOR_NOTE_OVERRIDE\"\n");
        out.push_str("fi\n\n");
    }

    out.push_str("append_unique_mount() {\n");
    out.push_str("  local candidate=$1\n");
    out.push_str("  local existing\n");
    out.push_str("  for existing in \"${PYXIS_MOUNTS[@]:-}\"; do\n");
    out.push_str("    [[ \"$existing\" == \"$candidate\" ]] && return 0\n");
    out.push_str("  done\n");
    out.push_str("  PYXIS_MOUNTS+=(\"$candidate\")\n");
    out.push_str("}\n\n");

    out.push_str("build_pyxis_mounts() {\n");
    out.push_str("  local extra\n");
    out.push_str("  PYXIS_MOUNTS=()\n");
    out.push_str("  append_unique_mount \"$JOB_TMP:/hpc-compose/job\"\n");
    out.push_str("  if [[ \"$RESUME_ENABLED\" == \"1\" ]]; then\n");
    out.push_str("    append_unique_mount \"$RESUME_HOST_PATH:$RESUME_CONTAINER_PATH\"\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ \"${HPC_COMPOSE_SERVICE_SCRATCH_ENABLED:-0}\" == \"1\" && -n \"${SCRATCH_HOST_PATH:-}\" && -n \"${SCRATCH_CONTAINER_PATH:-}\" ]]; then\n");
    out.push_str("    append_unique_mount \"$SCRATCH_HOST_PATH:$SCRATCH_CONTAINER_PATH\"\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ -e /etc/slurm/task_prolog.hk ]]; then\n");
    out.push_str(
        "    append_unique_mount \"/etc/slurm/task_prolog.hk:/etc/slurm/task_prolog.hk\"\n",
    );
    out.push_str("    append_unique_mount \"/etc/slurm/task_prolog.hk:/etc/slurm/task_prolog\"\n");
    out.push_str("  elif [[ -e /etc/slurm/task_prolog ]]; then\n");
    out.push_str("    append_unique_mount \"/etc/slurm/task_prolog:/etc/slurm/task_prolog\"\n");
    out.push_str("    append_unique_mount \"/etc/slurm/task_prolog:/etc/slurm/task_prolog.hk\"\n");
    out.push_str("  fi\n");
    out.push_str("  [[ -d /scratch ]] && append_unique_mount \"/scratch:/scratch\"\n");
    out.push_str("  [[ -e /usr/lib64/slurm/libslurmfull.so ]] && append_unique_mount \"/usr/lib64/slurm/libslurmfull.so\"\n");
    out.push_str("  [[ -e /usr/lib64/libhwloc.so.15 ]] && append_unique_mount \"/usr/lib64/libhwloc.so.15\"\n");
    out.push_str("  for extra in \"$@\"; do\n");
    out.push_str("    append_unique_mount \"$extra\"\n");
    out.push_str("  done\n");
    out.push_str("  local joined=\"\"\n");
    out.push_str("  local item\n");
    out.push_str("  for item in \"${PYXIS_MOUNTS[@]}\"; do\n");
    out.push_str("    if [[ -n \"$joined\" ]]; then\n");
    out.push_str("      joined+=\",\"\n");
    out.push_str("    fi\n");
    out.push_str("    joined+=\"$item\"\n");
    out.push_str("  done\n");
    out.push_str("  printf '%s' \"$joined\"\n");
    out.push_str("}\n\n");

    out.push_str("json_escape() {\n");
    out.push_str("  local value=$1\n");
    out.push_str("  value=${value//\\\\/\\\\\\\\}\n");
    out.push_str("  value=${value//\\\"/\\\\\\\"}\n");
    out.push_str("  value=${value//$'\\n'/\\\\n}\n");
    out.push_str("  value=${value//$'\\r'/\\\\r}\n");
    out.push_str("  value=${value//$'\\t'/\\\\t}\n");
    out.push_str("  printf '%s' \"$value\"\n");
    out.push_str("}\n\n");

    out.push_str("trim_whitespace() {\n");
    out.push_str("  local value=${1-}\n");
    out.push_str("  value=${value#\"${value%%[![:space:]]*}\"}\n");
    out.push_str("  value=${value%\"${value##*[![:space:]]}\"}\n");
    out.push_str("  printf '%s' \"$value\"\n");
    out.push_str("}\n\n");

    out.push_str("json_string_or_null() {\n");
    out.push_str("  local value=${1-}\n");
    out.push_str("  if [[ -z \"$value\" ]]; then\n");
    out.push_str("    printf 'null'\n");
    out.push_str("  else\n");
    out.push_str("    printf '\"%s\"' \"$(json_escape \"$value\")\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("json_number_or_null() {\n");
    out.push_str("  local value=${1-}\n");
    out.push_str("  if [[ -z \"$value\" ]]; then\n");
    out.push_str("    printf 'null'\n");
    out.push_str("  else\n");
    out.push_str("    printf '%s' \"$value\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("json_number_array() {\n");
    out.push_str("  local raw_values=${1-}\n");
    out.push_str("  local -a values=()\n");
    out.push_str("  local value\n");
    out.push_str("  printf '['\n");
    out.push_str("  if [[ -n \"$raw_values\" ]]; then\n");
    out.push_str("    read -r -a values <<< \"$raw_values\"\n");
    out.push_str("    local first=1\n");
    out.push_str("    for value in \"${values[@]}\"; do\n");
    out.push_str("      [[ -z \"$value\" ]] && continue\n");
    out.push_str("      if (( first == 0 )); then\n");
    out.push_str("        printf ','\n");
    out.push_str("      fi\n");
    out.push_str("      printf '%s' \"$value\"\n");
    out.push_str("      first=0\n");
    out.push_str("    done\n");
    out.push_str("  fi\n");
    out.push_str("  printf ']'\n");
    out.push_str("}\n\n");

    if assertions_enabled {
        out.push_str("json_lines_array() {\n");
        out.push_str("  local raw=${1-}\n");
        out.push_str("  local first=1\n");
        out.push_str("  local line\n");
        out.push_str("  printf '['\n");
        out.push_str("  while IFS= read -r line; do\n");
        out.push_str("    [[ -z \"$line\" ]] && continue\n");
        out.push_str("    if (( first == 0 )); then\n");
        out.push_str("      printf ','\n");
        out.push_str("    fi\n");
        out.push_str("    printf '\"%s\"' \"$(json_escape \"$line\")\"\n");
        out.push_str("    first=0\n");
        out.push_str("  done <<< \"$raw\"\n");
        out.push_str("  printf ']'\n");
        out.push_str("}\n\n");
    }

    out.push_str("reset_wait_helper_exit_state() {\n");
    out.push_str("  WAIT_HELPER_EXITED=0\n");
    out.push_str("  WAIT_HELPER_EXIT_STATUS=\"\"\n");
    out.push_str("}\n\n");

    out.push_str("record_wait_helper_exit() {\n");
    out.push_str("  local pid=$1\n");
    out.push_str("  local status=0\n");
    out.push_str("  wait \"$pid\" || status=$?\n");
    out.push_str("  WAIT_HELPER_EXITED=1\n");
    out.push_str("  WAIT_HELPER_EXIT_STATUS=\"$status\"\n");
    out.push_str("}\n\n");

    out.push_str("replace_with_symlink() {\n");
    out.push_str("  local link_path=$1\n");
    out.push_str("  local target=$2\n");
    out.push_str("  if [[ -L \"$link_path\" || -f \"$link_path\" ]]; then\n");
    out.push_str("    rm -f \"$link_path\"\n");
    out.push_str("  elif [[ -d \"$link_path\" ]]; then\n");
    out.push_str("    rm -rf \"$link_path\"\n");
    out.push_str("  fi\n");
    out.push_str("  ln -s \"$target\" \"$link_path\"\n");
    out.push_str("}\n\n");

    out.push_str("update_latest_runtime_links() {\n");
    out.push_str("  [[ \"$RESUME_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  replace_with_symlink \"$JOB_ROOT/allocation\" \"$ALLOCATION_DIR\"\n");
    out.push_str("  replace_with_symlink \"$JOB_ROOT/logs\" \"$LOG_DIR\"\n");
    out.push_str("  replace_with_symlink \"$JOB_ROOT/state.json\" \"$STATE_FILE\"\n");
    if metrics_enabled {
        out.push_str("  replace_with_symlink \"$JOB_ROOT/metrics\" \"$METRICS_DIR\"\n");
    }
    if artifacts_enabled {
        out.push_str("  replace_with_symlink \"$JOB_ROOT/artifacts\" \"$ARTIFACTS_DIR\"\n");
    }
    out.push_str("}\n\n");

    out.push_str("resolve_allocation_metadata() {\n");
    out.push_str("  local -a allocation_nodes=()\n");
    if plan.slurm.is_multi_node() {
        out.push_str("  if [[ -z \"${SLURM_JOB_NODELIST:-}\" ]]; then\n");
        out.push_str(
            "    echo \"SLURM_JOB_NODELIST is required to derive allocation metadata\" >&2\n",
        );
        out.push_str("    exit 1\n");
        out.push_str("  fi\n");
        out.push_str("  if ! command -v scontrol >/dev/null 2>&1; then\n");
        out.push_str("    echo \"scontrol is required to derive allocation metadata\" >&2\n");
        out.push_str("    exit 1\n");
        out.push_str("  fi\n");
        out.push_str(
            "  mapfile -t allocation_nodes < <(scontrol show hostnames \"$SLURM_JOB_NODELIST\")\n",
        );
        out.push_str("  if (( ${#allocation_nodes[@]} == 0 )); then\n");
        out.push_str("    echo \"failed to expand SLURM_JOB_NODELIST via scontrol\" >&2\n");
        out.push_str("    exit 1\n");
        out.push_str("  fi\n");
        out.push_str("  HPC_COMPOSE_PRIMARY_NODE=\"${allocation_nodes[0]}\"\n");
        out.push_str("  HPC_COMPOSE_NODE_COUNT=${#allocation_nodes[@]}\n");
        out.push_str("  HPC_COMPOSE_NODELIST=\"${allocation_nodes[*]}\"\n");
    } else {
        out.push_str("  local primary_node=\"${SLURMD_NODENAME:-${HOSTNAME:-}}\"\n");
        out.push_str("  if [[ -z \"$primary_node\" ]]; then\n");
        out.push_str("    primary_node=$(hostname)\n");
        out.push_str("  fi\n");
        out.push_str("  if [[ -z \"$primary_node\" ]]; then\n");
        out.push_str(
            "    echo \"failed to derive primary node metadata for single-node allocation\" >&2\n",
        );
        out.push_str("    exit 1\n");
        out.push_str("  fi\n");
        out.push_str("  allocation_nodes=(\"$primary_node\")\n");
        out.push_str("  HPC_COMPOSE_PRIMARY_NODE=\"$primary_node\"\n");
        out.push_str("  HPC_COMPOSE_NODE_COUNT=1\n");
        out.push_str("  HPC_COMPOSE_NODELIST=\"$primary_node\"\n");
    }
    out.push_str(&format!(
        "  HPC_COMPOSE_NODELIST_FILE=\"{}\"\n",
        tracked_paths::under_job_container_dir(&format!(
            "{}/{}",
            tracked_paths::ALLOCATION_DIR_NAME,
            tracked_paths::NODELIST_FILE_NAME
        ))
    ));
    out.push_str("  printf '%s\\n' \"${allocation_nodes[@]}\" > \"$NODELIST_FILE\"\n");
    out.push_str("  printf '%s\\n' \"$HPC_COMPOSE_PRIMARY_NODE\" > \"$PRIMARY_NODE_FILE\"\n");
    out.push_str("  ALLOCATION_NODES=(\"${allocation_nodes[@]}\")\n");
    out.push_str("  export HPC_COMPOSE_PRIMARY_NODE HPC_COMPOSE_NODE_COUNT HPC_COMPOSE_NODELIST HPC_COMPOSE_NODELIST_FILE\n");
    out.push_str("}\n\n");

    out.push_str("nodes_for_indices() {\n");
    out.push_str("  local -a nodes=()\n");
    out.push_str("  local index\n");
    out.push_str("  for index in \"$@\"; do\n");
    out.push_str("    if [[ -z \"$index\" ]]; then\n");
    out.push_str("      continue\n");
    out.push_str("    fi\n");
    out.push_str("    if (( index < 0 || index >= ${#ALLOCATION_NODES[@]} )); then\n");
    out.push_str("      echo \"service placement references allocation node index $index, but ${#ALLOCATION_NODES[@]} node(s) are available\" >&2\n");
    out.push_str("      exit 1\n");
    out.push_str("    fi\n");
    out.push_str("    nodes+=(\"${ALLOCATION_NODES[index]}\")\n");
    out.push_str("  done\n");
    out.push_str("  printf '%s' \"${nodes[*]}\"\n");
    out.push_str("}\n\n");

    out.push_str("comma_join_words() {\n");
    out.push_str("  local raw=${1:-}\n");
    out.push_str("  local -a words=()\n");
    out.push_str("  local joined=\"\"\n");
    out.push_str("  local word\n");
    out.push_str("  read -r -a words <<< \"$raw\"\n");
    out.push_str("  for word in \"${words[@]}\"; do\n");
    out.push_str("    [[ -z \"$word\" ]] && continue\n");
    out.push_str("    if [[ -n \"$joined\" ]]; then\n");
    out.push_str("      joined+=\",\"\n");
    out.push_str("    fi\n");
    out.push_str("    joined+=\"$word\"\n");
    out.push_str("  done\n");
    out.push_str("  printf '%s' \"$joined\"\n");
    out.push_str("}\n\n");

    out.push_str("word_count() {\n");
    out.push_str("  local raw=${1:-}\n");
    out.push_str("  local -a words=()\n");
    out.push_str("  if [[ -z \"$raw\" ]]; then\n");
    out.push_str("    printf '0'\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  read -r -a words <<< \"$raw\"\n");
    out.push_str("  printf '%s' \"${#words[@]}\"\n");
    out.push_str("}\n\n");

    out.push_str("first_word() {\n");
    out.push_str("  local raw=${1:-}\n");
    out.push_str("  local -a words=()\n");
    out.push_str("  read -r -a words <<< \"$raw\"\n");
    out.push_str("  printf '%s' \"${words[0]:-}\"\n");
    out.push_str("}\n\n");

    if distributed_env_enabled {
        out.push_str("hpc_compose_dist_port() {\n");
        out.push_str("  local fixed=${1:-}\n");
        out.push_str("  local base=${2:-29500}\n");
        out.push_str("  local span=${3:-1000}\n");
        out.push_str("  local offset=${4:-0}\n");
        out.push_str("  if [[ -n \"$fixed\" ]]; then\n");
        out.push_str("    printf '%s' \"$fixed\"\n");
        out.push_str("    return 0\n");
        out.push_str("  fi\n");
        out.push_str("  local job_digits=${SLURM_JOB_ID//[^0-9]/}\n");
        out.push_str("  if [[ -z \"$job_digits\" ]]; then\n");
        out.push_str("    job_digits=0\n");
        out.push_str("  fi\n");
        out.push_str("  printf '%s' $(( base + ((job_digits + offset) % span) ))\n");
        out.push_str("}\n\n");
    }

    out.push_str("write_nodelist_file() {\n");
    out.push_str("  local path=$1\n");
    out.push_str("  local nodelist=$2\n");
    out.push_str("  local -a nodes=()\n");
    out.push_str("  local node\n");
    out.push_str("  mkdir -p \"$(dirname \"$path\")\"\n");
    out.push_str("  : > \"$path\"\n");
    out.push_str("  read -r -a nodes <<< \"$nodelist\"\n");
    out.push_str("  for node in \"${nodes[@]}\"; do\n");
    out.push_str("    [[ -z \"$node\" ]] && continue\n");
    out.push_str("    printf '%s\\n' \"$node\" >> \"$path\"\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    if mpi_enabled || distributed_env_enabled {
        out.push_str("write_mpi_hostfile() {\n");
        out.push_str("  local hostfile=$1\n");
        out.push_str("  local nodelist=$2\n");
        out.push_str("  local slots=${3:-}\n");
        out.push_str("  local -a nodes=()\n");
        out.push_str("  local node\n");
        out.push_str("  mkdir -p \"$(dirname \"$hostfile\")\"\n");
        out.push_str("  : > \"$hostfile\"\n");
        out.push_str("  read -r -a nodes <<< \"$nodelist\"\n");
        out.push_str("  for node in \"${nodes[@]}\"; do\n");
        out.push_str("    [[ -z \"$node\" ]] && continue\n");
        out.push_str("    if [[ -n \"$slots\" ]]; then\n");
        out.push_str("      printf '%s slots=%s\\n' \"$node\" \"$slots\" >> \"$hostfile\"\n");
        out.push_str("    else\n");
        out.push_str("      printf '%s\\n' \"$node\" >> \"$hostfile\"\n");
        out.push_str("    fi\n");
        out.push_str("  done\n");
        out.push_str("}\n\n");
    }

    if hooks_enabled {
        out.push_str("run_host_hook() {\n");
        out.push_str("  local script_path=$1\n");
        out.push_str("  local service_name=$2\n");
        out.push_str("  local phase=$3\n");
        out.push_str("  local logfile=$4\n");
        out.push_str("  local service_exit_code=${5:-}\n");
        out.push_str("  (\n");
        out.push_str("    export HPC_COMPOSE_SERVICE_NAME=\"$service_name\"\n");
        out.push_str("    export HPC_COMPOSE_HOOK_PHASE=\"$phase\"\n");
        out.push_str("    export HPC_COMPOSE_SERVICE_LOG=\"$logfile\"\n");
        out.push_str("    export HPC_COMPOSE_SERVICE_EXIT_CODE=\"$service_exit_code\"\n");
        out.push_str("    bash \"$script_path\"\n");
        out.push_str("  ) >>\"$logfile\" 2>&1\n");
        out.push_str("}\n\n");

        out.push_str("run_service_event_hooks() {\n");
        out.push_str("  local index=$1\n");
        out.push_str("  local event=$2\n");
        out.push_str("  local service_exit_code=$3\n");
        out.push_str("  local manifest=${SERVICE_EVENT_HOOK_MANIFESTS[index]:-}\n");
        out.push_str("  [[ -n \"$manifest\" && -f \"$manifest\" ]] || return 0\n");
        out.push_str("  local service_name=${SERVICE_NAMES[index]:-unknown}\n");
        out.push_str("  local logfile=${SERVICE_LOG_PATHS[index]:-}\n");
        out.push_str("  [[ -n \"$logfile\" ]] || return 0\n");
        out.push_str("  local restart_count=${SERVICE_RESTART_COUNT[index]:-0}\n");
        out.push_str("  local max_restarts=${SERVICE_MAX_RESTARTS[index]:-0}\n");
        out.push_str("  local window_seconds=${SERVICE_WINDOW_SECONDS[index]:-0}\n");
        out.push_str(
            "  local max_restarts_in_window=${SERVICE_MAX_RESTARTS_IN_WINDOW[index]:-0}\n",
        );
        out.push_str(
            "  local restart_failures_in_window=${SERVICE_RESTART_FAILURES_IN_WINDOW[index]:-0}\n",
        );
        out.push_str("  local hook_event script_path\n");
        out.push_str("  while IFS=$'\\t' read -r hook_event script_path; do\n");
        out.push_str(
            "    [[ \"$hook_event\" == \"$event\" && -n \"$script_path\" ]] || continue\n",
        );
        out.push_str("    local hook_status=0\n");
        out.push_str("    (\n");
        out.push_str("      export HPC_COMPOSE_SERVICE_NAME=\"$service_name\"\n");
        out.push_str("      export HPC_COMPOSE_HOOK_PHASE=\"$event\"\n");
        out.push_str("      export HPC_COMPOSE_SERVICE_LOG=\"$logfile\"\n");
        out.push_str("      export HPC_COMPOSE_SERVICE_EXIT_CODE=\"$service_exit_code\"\n");
        out.push_str("      export HPC_COMPOSE_ATTEMPT=\"$ATTEMPT\"\n");
        out.push_str("      export HPC_COMPOSE_RESTART_COUNT=\"$restart_count\"\n");
        out.push_str("      export HPC_COMPOSE_MAX_RESTARTS=\"$max_restarts\"\n");
        out.push_str("      export HPC_COMPOSE_WINDOW_SECONDS=\"$window_seconds\"\n");
        out.push_str(
            "      export HPC_COMPOSE_MAX_RESTARTS_IN_WINDOW=\"$max_restarts_in_window\"\n",
        );
        out.push_str(
            "      export HPC_COMPOSE_RESTART_FAILURES_IN_WINDOW=\"$restart_failures_in_window\"\n",
        );
        out.push_str("      bash \"$script_path\"\n");
        out.push_str("    ) >>\"$logfile\" 2>&1 || hook_status=$?\n");
        out.push_str("    if (( hook_status != 0 )); then\n");
        out.push_str("      echo \"Event hook '$event' for service '$service_name' exited with status $hook_status\" >>\"$logfile\" 2>&1\n");
        out.push_str("    fi\n");
        out.push_str("  done < \"$manifest\"\n");
        out.push_str("  return 0\n");
        out.push_str("}\n\n");
    }

    out.push_str("write_resume_metadata() {\n");
    out.push_str("  [[ \"$RESUME_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  local tmp_resume=\"$RESUME_META_FILE.tmp\"\n");
    out.push_str("  {\n");
    out.push_str("    printf '{\\n'\n");
    out.push_str("    printf '  \"schema_version\": 1,\\n'\n");
    out.push_str(
        "    printf '  \"compose_name\": \"%s\",\\n' \"$(json_escape \"$RESUME_COMPOSE_NAME\")\"\n",
    );
    out.push_str("    printf '  \"job_id\": \"%s\",\\n' \"$(json_escape \"$SLURM_JOB_ID\")\"\n");
    out.push_str("    printf '  \"attempt\": %s,\\n' \"$ATTEMPT\"\n");
    out.push_str("    printf '  \"updated_at\": \"%s\"\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"\n");
    out.push_str("    printf '}\\n'\n");
    out.push_str("  } > \"$tmp_resume\"\n");
    out.push_str("  mv \"$tmp_resume\" \"$RESUME_META_FILE\"\n");
    out.push_str("}\n\n");

    if artifacts_enabled {
        render_artifact_helpers(&mut out);
    }
    if transfer_helpers_enabled {
        render_stage_helpers(&mut out, plan);
    }
    if hf_stage_enabled {
        render_hf_stage_in(&mut out, plan, &options.huggingface_cli_bin);
    }

    if metrics_enabled {
        render_metrics_helpers(&mut out);
    }
    if software_env_enabled {
        render_software_env_helpers(&mut out);
    }

    out.push_str("write_state_file() {\n");
    out.push_str("  local tmp_state=\"$STATE_FILE.tmp\"\n");
    out.push_str("  {\n");
    out.push_str("    printf '{\\n'\n");
    out.push_str("    printf '  \"backend\": \"%s\",\\n' \"$(json_escape \"$BACKEND\")\"\n");
    out.push_str("    printf '  \"job_status\": \"%s\",\\n' \"$(json_escape \"$JOB_STATUS\")\"\n");
    out.push_str(
        "    printf '  \"job_exit_code\": %s,\\n' \"$(json_number_or_null \"$JOB_EXIT_CODE\")\"\n",
    );
    out.push_str("    printf '  \"supervisor_pid\": %s,\\n' \"$(json_number_or_null \"$SUPERVISOR_PID\")\"\n");
    out.push_str("    if [[ \"$RESUME_ENABLED\" == \"1\" ]]; then\n");
    out.push_str("      printf '  \"attempt\": %s,\\n' \"$ATTEMPT\"\n");
    out.push_str("      printf '  \"is_resume\": %s,\\n' \"$(if [[ \"$IS_RESUME\" == \"1\" ]]; then printf true; else printf false; fi)\"\n");
    out.push_str(
        "      printf '  \"resume_dir\": \"%s\",\\n' \"$(json_escape \"$RESUME_HOST_PATH\")\"\n",
    );
    out.push_str("    else\n");
    out.push_str("      printf '  \"attempt\": null,\\n'\n");
    out.push_str("      printf '  \"is_resume\": null,\\n'\n");
    out.push_str("      printf '  \"resume_dir\": null,\\n'\n");
    out.push_str("    fi\n");
    out.push_str("    printf '  \"services\": ['\n");
    out.push_str("    local first=1\n");
    out.push_str("    local i\n");
    out.push_str("    for i in \"${!SERVICE_NAMES[@]}\"; do\n");
    out.push_str("      if (( first == 0 )); then\n");
    out.push_str("        printf ','\n");
    out.push_str("      fi\n");
    out.push_str("      printf '\\n    {\"service_name\":\"%s\",\"step_name\":\"%s\",\"log_path\":\"%s\",\"launch_index\":%s,\"launcher_pid\":%s,\"healthy\":%s,\"completed_successfully\":%s,\"readiness_configured\":%s,\"failure_policy_mode\":\"%s\",\"restart_count\":%s,\"max_restarts\":%s,\"window_seconds\":%s,\"max_restarts_in_window\":%s,\"restart_failures_in_window\":%s,\"restart_failure_timestamps\":%s,\"last_exit_code\":%s,\"started_at\":%s,\"finished_at\":%s,\"first_failure_at\":%s,\"first_failure_exit_code\":%s,\"first_failure_node\":%s,\"first_failure_rank\":%s,\"placement_mode\":%s,\"nodes\":%s,\"ntasks\":%s,\"ntasks_per_node\":%s,\"nodelist\":%s' \\\n");
    out.push_str("        \"$(json_escape \"${SERVICE_NAMES[i]}\")\" \\\n");
    out.push_str("        \"$(json_escape \"${SERVICE_STEP_NAMES[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_escape \"${SERVICE_LOG_PATHS[i]:-}\")\" \\\n");
    out.push_str("        \"$i\" \\\n");
    out.push_str("        \"${SERVICE_PIDS[i]:-0}\" \\\n");
    out.push_str("        \"$(if [[ \"${SERVICE_HEALTHY[i]:-0}\" == \"1\" ]]; then printf true; else printf false; fi)\" \\\n");
    out.push_str("        \"$(if [[ \"${SERVICE_COMPLETED_SUCCESSFULLY[i]:-0}\" == \"1\" ]]; then printf true; else printf false; fi)\" \\\n");
    out.push_str("        \"$(if [[ -n \"${SERVICE_READINESS_CONFIGURED[i]:-}\" ]] && [[ \"${SERVICE_READINESS_CONFIGURED[i]}\" == \"1\" ]]; then printf true; else printf false; fi)\" \\\n");
    out.push_str("        \"$(json_escape \"${SERVICE_FAILURE_POLICY_MODE[i]:-fail_job}\")\" \\\n");
    out.push_str("        \"${SERVICE_RESTART_COUNT[i]:-0}\" \\\n");
    out.push_str("        \"${SERVICE_MAX_RESTARTS[i]:-0}\" \\\n");
    out.push_str("        \"${SERVICE_WINDOW_SECONDS[i]:-0}\" \\\n");
    out.push_str("        \"${SERVICE_MAX_RESTARTS_IN_WINDOW[i]:-0}\" \\\n");
    out.push_str("        \"${SERVICE_RESTART_FAILURES_IN_WINDOW[i]:-0}\" \\\n");
    out.push_str(
        "        \"$(json_number_array \"${SERVICE_RESTART_FAILURE_TIMESTAMPS[i]:-}\")\" \\\n",
    );
    out.push_str("        \"$(if [[ -n \"${SERVICE_LAST_EXIT_CODE[i]:-}\" ]]; then printf '%s' \"${SERVICE_LAST_EXIT_CODE[i]}\"; else printf null; fi)\" \\\n");
    out.push_str("        \"$(json_number_or_null \"${SERVICE_STARTED_AT[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_number_or_null \"${SERVICE_FINISHED_AT[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_number_or_null \"${SERVICE_FIRST_FAILURE_AT[i]:-}\")\" \\\n");
    out.push_str(
        "        \"$(json_number_or_null \"${SERVICE_FIRST_FAILURE_EXIT_CODE[i]:-}\")\" \\\n",
    );
    out.push_str("        \"$(json_string_or_null \"${SERVICE_FIRST_FAILURE_NODE[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"${SERVICE_FIRST_FAILURE_RANK[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"${SERVICE_PLACEMENT_MODE[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_number_or_null \"${SERVICE_STEP_NODES[i]:-}\")\" \\\n");
    out.push_str("        \"$(json_number_or_null \"${SERVICE_STEP_NTASKS[i]:-}\")\" \\\n");
    out.push_str(
        "        \"$(json_number_or_null \"${SERVICE_STEP_NTASKS_PER_NODE[i]:-}\")\" \\\n",
    );
    out.push_str("        \"$(json_string_or_null \"${SERVICE_STEP_NODELIST[i]:-}\")\"\n");
    if assertions_enabled {
        out.push_str("      printf ',\"assertions\":{\"configured\":%s,\"status\":\"%s\",\"expected_exit_code\":%s,\"artifacts_contain\":%s,\"max_duration_seconds\":%s,\"duration_seconds\":%s,\"failures\":%s}' \\\n");
        out.push_str("        \"$(if [[ -n \"${SERVICE_ASSERT_EXIT_CODES[i]:-}\" || -n \"${SERVICE_ASSERT_ARTIFACT_PATTERNS[i]:-}\" || -n \"${SERVICE_ASSERT_MAX_DURATIONS[i]:-}\" ]]; then printf true; else printf false; fi)\" \\\n");
        out.push_str("        \"$(json_escape \"${SERVICE_ASSERT_STATUS[i]:-none}\")\" \\\n");
        out.push_str(
            "        \"$(json_number_or_null \"${SERVICE_ASSERT_EXIT_CODES[i]:-}\")\" \\\n",
        );
        out.push_str(
            "        \"$(json_string_or_null \"${SERVICE_ASSERT_ARTIFACT_PATTERNS[i]:-}\")\" \\\n",
        );
        out.push_str(
            "        \"$(json_number_or_null \"${SERVICE_ASSERT_MAX_DURATIONS[i]:-}\")\" \\\n",
        );
        out.push_str(
            "        \"$(json_number_or_null \"${SERVICE_ASSERT_DURATIONS[i]:-}\")\" \\\n",
        );
        out.push_str("        \"$(json_lines_array \"${SERVICE_ASSERT_FAILURES[i]:-}\")\"\n");
    }
    out.push_str("      printf '}'\n");
    out.push_str("      first=0\n");
    out.push_str("    done\n");
    out.push_str("    printf '\\n  ]\\n}\\n'\n");
    out.push_str("  } > \"$tmp_state\"\n");
    out.push_str("  mv \"$tmp_state\" \"$STATE_FILE\"\n");
    out.push_str("}\n\n");

    out.push_str("resolve_allocation_metadata\n");
    if scratch_enabled {
        out.push_str("init_scratch\n");
    }
    out.push_str("update_latest_runtime_links\n");
    out.push_str("write_resume_metadata\n");
    out.push_str("write_state_file\n\n");

    out.push_str("kill_services() {\n");
    out.push_str("  for pid in \"${SERVICE_PIDS[@]:-}\"; do\n");
    out.push_str("    [[ -z \"$pid\" ]] && continue\n");
    out.push_str("    if kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      kill \"$pid\" 2>/dev/null || true\n");
    out.push_str("    fi\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    if signal_forward_target.is_some() {
        // Batch-shell (`B:`) delivery only reaches this supervisor, so relay the
        // early-warning signal to each running service without exiting: the job
        // keeps running so the application can checkpoint before the time limit.
        out.push_str("forward_configured_signal() {\n");
        out.push_str("  local sig=$1\n");
        out.push_str(
            "  echo \"received early-warning signal $sig; forwarding to running services\" >&2\n",
        );
        out.push_str("  for pid in \"${SERVICE_PIDS[@]:-}\"; do\n");
        out.push_str("    [[ -z \"$pid\" ]] && continue\n");
        out.push_str("    kill -s \"$sig\" \"$pid\" 2>/dev/null || true\n");
        out.push_str("  done\n");
        out.push_str("}\n\n");
    }

    out.push_str("reap_services_after_cleanup() {\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!SERVICE_PIDS[@]}\"; do\n");
    out.push_str("    local pid=${SERVICE_PIDS[i]:-}\n");
    out.push_str("    [[ -z \"$pid\" ]] && continue\n");
    out.push_str("    local status=0\n");
    out.push_str("    wait \"$pid\" || status=$?\n");
    out.push_str("    handle_service_exit \"$i\" \"$status\" || true\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    if assertions_enabled {
        out.push_str("append_assert_failure() {\n");
        out.push_str("  local index=$1\n");
        out.push_str("  local message=$2\n");
        out.push_str("  if [[ -n \"${SERVICE_ASSERT_FAILURES[index]:-}\" ]]; then\n");
        out.push_str("    SERVICE_ASSERT_FAILURES[index]+=$'\\n'\n");
        out.push_str("  fi\n");
        out.push_str("  SERVICE_ASSERT_FAILURES[index]+=\"$message\"\n");
        out.push_str("}\n\n");

        out.push_str("assertion_configured() {\n");
        out.push_str("  local index=$1\n");
        out.push_str("  [[ -n \"${SERVICE_ASSERT_EXIT_CODES[index]:-}\" || -n \"${SERVICE_ASSERT_ARTIFACT_PATTERNS[index]:-}\" || -n \"${SERVICE_ASSERT_MAX_DURATIONS[index]:-}\" ]]\n");
        out.push_str("}\n\n");

        out.push_str("evaluate_service_assertions() {\n");
        out.push_str("  local index=$1\n");
        out.push_str("  local name=${SERVICE_NAMES[index]:-unknown}\n");
        out.push_str("  if ! assertion_configured \"$index\"; then\n");
        out.push_str("    SERVICE_ASSERT_STATUS[index]=\"none\"\n");
        out.push_str("    SERVICE_ASSERT_FAILURES[index]=\"\"\n");
        out.push_str("    return 0\n");
        out.push_str("  fi\n");
        out.push_str("  SERVICE_ASSERT_STATUS[index]=\"passed\"\n");
        out.push_str("  SERVICE_ASSERT_FAILURES[index]=\"\"\n");
        out.push_str("  if [[ -n \"$RECEIVED_SIGNAL\" ]]; then\n");
        out.push_str("    SERVICE_ASSERT_STATUS[index]=\"skipped\"\n");
        out.push_str("    append_assert_failure \"$index\" \"skipped because job received signal $RECEIVED_SIGNAL\"\n");
        out.push_str("    return 0\n");
        out.push_str("  fi\n");
        out.push_str("  local failed=0\n");
        out.push_str("  local expected_exit=${SERVICE_ASSERT_EXIT_CODES[index]:-}\n");
        out.push_str("  if [[ -n \"$expected_exit\" ]]; then\n");
        out.push_str("    local actual_exit=${SERVICE_LAST_EXIT_CODE[index]:-}\n");
        out.push_str("    if [[ -z \"$actual_exit\" ]]; then\n");
        out.push_str("      append_assert_failure \"$index\" \"expected exit_code $expected_exit, but no exit code was recorded\"\n");
        out.push_str("      failed=1\n");
        out.push_str("    elif [[ \"$actual_exit\" != \"$expected_exit\" ]]; then\n");
        out.push_str("      append_assert_failure \"$index\" \"expected exit_code $expected_exit, got $actual_exit\"\n");
        out.push_str("      failed=1\n");
        out.push_str("    fi\n");
        out.push_str("  fi\n");
        out.push_str("  local artifact_pattern=${SERVICE_ASSERT_ARTIFACT_PATTERNS[index]:-}\n");
        out.push_str("  if [[ -n \"$artifact_pattern\" ]]; then\n");
        out.push_str("    local host_pattern=\"$JOB_TMP${artifact_pattern#/hpc-compose/job}\"\n");
        out.push_str("    local shopt_state\n");
        out.push_str("    shopt_state=$(shopt -p nullglob globstar dotglob)\n");
        out.push_str("    shopt -s nullglob globstar dotglob\n");
        out.push_str("    local match_count=0\n");
        out.push_str("    local matched\n");
        out.push_str("    while IFS= read -r matched; do\n");
        out.push_str("      [[ -n \"$matched\" ]] || continue\n");
        out.push_str("      match_count=$((match_count + 1))\n");
        out.push_str("      break\n");
        out.push_str("    done < <(compgen -G \"$host_pattern\" || true)\n");
        out.push_str("    eval \"$shopt_state\"\n");
        out.push_str("    if (( match_count == 0 )); then\n");
        out.push_str("      append_assert_failure \"$index\" \"expected artifacts_contain '$artifact_pattern' to match at least one path\"\n");
        out.push_str("      failed=1\n");
        out.push_str("    fi\n");
        out.push_str("  fi\n");
        out.push_str("  local max_duration=${SERVICE_ASSERT_MAX_DURATIONS[index]:-}\n");
        out.push_str("  if [[ -n \"$max_duration\" ]]; then\n");
        out.push_str("    local started_at=${SERVICE_STARTED_AT[index]:-}\n");
        out.push_str("    local finished_at=${SERVICE_FINISHED_AT[index]:-}\n");
        out.push_str("    if [[ -z \"$started_at\" || -z \"$finished_at\" ]]; then\n");
        out.push_str("      append_assert_failure \"$index\" \"expected max_duration_seconds $max_duration, but service runtime was not fully recorded\"\n");
        out.push_str("      failed=1\n");
        out.push_str("    else\n");
        out.push_str("      local duration=$(( finished_at >= started_at ? finished_at - started_at : 0 ))\n");
        out.push_str("      SERVICE_ASSERT_DURATIONS[index]=\"$duration\"\n");
        out.push_str("      if (( duration > max_duration )); then\n");
        out.push_str("        append_assert_failure \"$index\" \"expected max_duration_seconds <= $max_duration, got $duration\"\n");
        out.push_str("        failed=1\n");
        out.push_str("      fi\n");
        out.push_str("    fi\n");
        out.push_str("  fi\n");
        out.push_str("  if (( failed != 0 )); then\n");
        out.push_str("    SERVICE_ASSERT_STATUS[index]=\"failed\"\n");
        out.push_str("    echo \"Assertions failed for service '$name': ${SERVICE_ASSERT_FAILURES[index]//$'\\n'/; }\" >&2\n");
        out.push_str("    return 1\n");
        out.push_str("  fi\n");
        out.push_str("  return 0\n");
        out.push_str("}\n\n");

        out.push_str("evaluate_assertions() {\n");
        out.push_str("  local status=0\n");
        out.push_str("  local i\n");
        out.push_str("  for i in \"${!SERVICE_NAMES[@]}\"; do\n");
        out.push_str("    evaluate_service_assertions \"$i\" || status=1\n");
        out.push_str("  done\n");
        out.push_str("  return \"$status\"\n");
        out.push_str("}\n\n");
    }

    if rendezvous_enabled {
        render_rendezvous_helpers(&mut out);
    }

    out.push_str("cleanup() {\n");
    out.push_str("  local code=$?\n");
    out.push_str("  trap - EXIT INT TERM\n");
    out.push_str("  CLEANING_UP=1\n");
    if metrics_enabled {
        out.push_str("  stop_metrics_sampler\n");
    }
    out.push_str("  if [[ -n \"$RECEIVED_SIGNAL\" ]]; then\n");
    out.push_str("    JOB_STATUS=\"CANCELLED\"\n");
    out.push_str("  elif (( code == 0 )); then\n");
    out.push_str("    JOB_STATUS=\"COMPLETED\"\n");
    out.push_str("  else\n");
    out.push_str("    JOB_STATUS=\"FAILED\"\n");
    out.push_str("  fi\n");
    out.push_str("  JOB_EXIT_CODE=\"$code\"\n");
    out.push_str("  local stage_out_status=0\n");
    if assertions_enabled {
        out.push_str("  local assertion_status=0\n");
    }
    out.push_str("  kill_services\n");
    out.push_str("  reap_services_after_cleanup\n");
    if assertions_enabled {
        out.push_str("  evaluate_assertions || assertion_status=$?\n");
        out.push_str("  if (( assertion_status != 0 )); then\n");
        out.push_str("    if (( code == 0 )); then\n");
        out.push_str("      code=$assertion_status\n");
        out.push_str("    fi\n");
        out.push_str("    JOB_STATUS=\"FAILED\"\n");
        out.push_str("    JOB_EXIT_CODE=\"$code\"\n");
        out.push_str("  fi\n");
    }
    if rendezvous_enabled {
        out.push_str("  deregister_rendezvous_records || true\n");
    }
    out.push_str("  write_state_file\n");
    if artifacts_enabled {
        out.push_str("  collect_artifacts \"$code\" || true\n");
    }
    if stage_enabled {
        out.push_str("  stage_out_paths \"$code\" || stage_out_status=$?\n");
        out.push_str("  if (( stage_out_status != 0 )); then\n");
        out.push_str("    echo \"Stage-out failed with status $stage_out_status; skipping scratch cleanup to preserve outputs\" >&2\n");
        out.push_str("    if (( code == 0 )); then\n");
        out.push_str("      code=$stage_out_status\n");
        out.push_str("      JOB_STATUS=\"FAILED\"\n");
        out.push_str("      JOB_EXIT_CODE=\"$code\"\n");
        out.push_str("      write_state_file\n");
        out.push_str("    fi\n");
        out.push_str("  fi\n");
    }
    if scratch_enabled {
        out.push_str("  if (( stage_out_status == 0 )); then\n");
        out.push_str("    cleanup_scratch \"$code\" || true\n");
        out.push_str("  fi\n");
    }
    // Runs after stage-out/artifacts so outputs persist before the per-job
    // runtime cache is reaped. No-op unless x-slurm.cleanup.runtime_cache opts in.
    out.push_str("  cleanup_runtime_cache \"$code\" || true\n");
    out.push_str("  exit \"$code\"\n");
    out.push_str("}\n");
    out.push_str("trap cleanup EXIT\n");
    out.push_str("trap 'RECEIVED_SIGNAL=INT; exit 130' INT\n");
    out.push_str("trap 'RECEIVED_SIGNAL=TERM; exit 143' TERM\n");
    if let Some(target) = signal_forward_target {
        out.push_str(&format!(
            "trap 'forward_configured_signal {target}' {target}\n"
        ));
    }
    out.push('\n');

    out.push_str("register_service() {\n");
    out.push_str("  local name=$1\n");
    out.push_str("  local pid=$2\n");
    out.push_str("  local step_name=$3\n");
    out.push_str("  local log_path=$4\n");
    out.push_str("  local failure_mode=$5\n");
    out.push_str("  local max_restarts=$6\n");
    out.push_str("  local backoff_seconds=$7\n");
    out.push_str("  local window_seconds=$8\n");
    out.push_str("  local max_restarts_in_window=$9\n");
    out.push_str("  local launch_fn=${10}\n");
    out.push_str("  local dependents_csv=${11}\n");
    out.push_str("  local placement_mode=${12}\n");
    out.push_str("  local step_nodes=${13}\n");
    out.push_str("  local step_ntasks=${14}\n");
    out.push_str("  local step_ntasks_per_node=${15}\n");
    out.push_str("  local step_nodelist=${16}\n");
    out.push_str("  local readiness_configured=${17}\n");
    out.push_str("  local host_epilogue_script=${18}\n");
    out.push_str("  local event_hooks_manifest=${19}\n");
    out.push_str("  local index=${SERVICE_INDEX_BY_NAME[\"$name\"]:-}\n");
    out.push_str("  local launched_at\n");
    out.push_str("  launched_at=$(date +%s)\n");
    out.push_str("  if [[ -n \"$index\" ]]; then\n");
    out.push_str("    SERVICE_PIDS[index]=\"$pid\"\n");
    out.push_str("    SERVICE_STEP_NAMES[index]=\"$step_name\"\n");
    out.push_str("    SERVICE_LOG_PATHS[index]=\"$log_path\"\n");
    out.push_str("    SERVICE_HEALTHY[index]=\"0\"\n");
    out.push_str("    SERVICE_COMPLETED_SUCCESSFULLY[index]=\"0\"\n");
    out.push_str("    SERVICE_READINESS_CONFIGURED[index]=\"$readiness_configured\"\n");
    out.push_str("    SERVICE_PLACEMENT_MODE[index]=\"$placement_mode\"\n");
    out.push_str("    SERVICE_STEP_NODES[index]=\"$step_nodes\"\n");
    out.push_str("    SERVICE_STEP_NTASKS[index]=\"$step_ntasks\"\n");
    out.push_str("    SERVICE_STEP_NTASKS_PER_NODE[index]=\"$step_ntasks_per_node\"\n");
    out.push_str("    SERVICE_STEP_NODELIST[index]=\"$step_nodelist\"\n");
    out.push_str("    if [[ -z \"${SERVICE_STARTED_AT[index]:-}\" ]]; then\n");
    out.push_str("      SERVICE_STARTED_AT[index]=\"$launched_at\"\n");
    out.push_str("    fi\n");
    out.push_str("    SERVICE_FINISHED_AT[index]=\"\"\n");
    out.push_str("    SERVICE_HOST_EPILOGUE_SCRIPTS[index]=\"$host_epilogue_script\"\n");
    out.push_str("    SERVICE_HOST_EPILOGUE_RAN[index]=\"0\"\n");
    out.push_str("    SERVICE_EVENT_HOOK_MANIFESTS[index]=\"$event_hooks_manifest\"\n");
    if rendezvous_enabled {
        out.push_str("    SERVICE_RDZV_REGISTERED[index]=\"0\"\n");
    }
    out.push_str("    SERVICE_LAUNCH_FNS[index]=\"$launch_fn\"\n");
    out.push_str("    if [[ -n \"$dependents_csv\" ]]; then\n");
    out.push_str("      SERVICE_DEPENDENTS[index]=\"$dependents_csv\"\n");
    out.push_str("    fi\n");
    out.push_str("  else\n");
    out.push_str("    index=${#SERVICE_PIDS[@]}\n");
    out.push_str("    SERVICE_PIDS+=(\"$pid\")\n");
    out.push_str("    SERVICE_NAMES+=(\"$name\")\n");
    out.push_str("    SERVICE_STEP_NAMES+=(\"$step_name\")\n");
    out.push_str("    SERVICE_LOG_PATHS+=(\"$log_path\")\n");
    out.push_str("    SERVICE_HEALTHY+=(\"0\")\n");
    out.push_str("    SERVICE_COMPLETED_SUCCESSFULLY+=(\"0\")\n");
    out.push_str("    SERVICE_READINESS_CONFIGURED+=(\"$readiness_configured\")\n");
    out.push_str("    SERVICE_FAILURE_POLICY_MODE+=(\"$failure_mode\")\n");
    out.push_str("    SERVICE_MAX_RESTARTS+=(\"$max_restarts\")\n");
    out.push_str("    SERVICE_BACKOFF_SECONDS+=(\"$backoff_seconds\")\n");
    out.push_str("    SERVICE_WINDOW_SECONDS+=(\"$window_seconds\")\n");
    out.push_str("    SERVICE_MAX_RESTARTS_IN_WINDOW+=(\"$max_restarts_in_window\")\n");
    out.push_str("    SERVICE_RESTART_COUNT+=(\"0\")\n");
    out.push_str("    SERVICE_RESTART_FAILURES_IN_WINDOW+=(\"0\")\n");
    out.push_str("    SERVICE_RESTART_FAILURE_TIMESTAMPS+=(\"\")\n");
    out.push_str("    SERVICE_LAST_EXIT_CODE+=(\"\")\n");
    out.push_str("    SERVICE_STARTED_AT+=(\"$launched_at\")\n");
    out.push_str("    SERVICE_FINISHED_AT+=(\"\")\n");
    out.push_str("    SERVICE_FIRST_FAILURE_AT+=(\"\")\n");
    out.push_str("    SERVICE_FIRST_FAILURE_EXIT_CODE+=(\"\")\n");
    out.push_str("    SERVICE_FIRST_FAILURE_NODE+=(\"\")\n");
    out.push_str("    SERVICE_FIRST_FAILURE_RANK+=(\"\")\n");
    out.push_str("    SERVICE_PLACEMENT_MODE+=(\"$placement_mode\")\n");
    out.push_str("    SERVICE_STEP_NODES+=(\"$step_nodes\")\n");
    out.push_str("    SERVICE_STEP_NTASKS+=(\"$step_ntasks\")\n");
    out.push_str("    SERVICE_STEP_NTASKS_PER_NODE+=(\"$step_ntasks_per_node\")\n");
    out.push_str("    SERVICE_STEP_NODELIST+=(\"$step_nodelist\")\n");
    out.push_str("    SERVICE_HOST_EPILOGUE_SCRIPTS+=(\"$host_epilogue_script\")\n");
    out.push_str("    SERVICE_HOST_EPILOGUE_RAN+=(\"0\")\n");
    out.push_str("    SERVICE_EVENT_HOOK_MANIFESTS+=(\"$event_hooks_manifest\")\n");
    if rendezvous_enabled {
        out.push_str("    SERVICE_RDZV_NAMES+=(\"\")\n");
        out.push_str("    SERVICE_RDZV_PORTS+=(\"\")\n");
        out.push_str("    SERVICE_RDZV_PROTOCOLS+=(\"\")\n");
        out.push_str("    SERVICE_RDZV_PATHS+=(\"\")\n");
        out.push_str("    SERVICE_RDZV_TTLS+=(\"\")\n");
        out.push_str("    SERVICE_RDZV_METADATA_JSON+=(\"{}\")\n");
        out.push_str("    SERVICE_RDZV_REGISTERED+=(\"0\")\n");
    }
    if assertions_enabled {
        out.push_str("    SERVICE_ASSERT_EXIT_CODES+=(\"\")\n");
        out.push_str("    SERVICE_ASSERT_ARTIFACT_PATTERNS+=(\"\")\n");
        out.push_str("    SERVICE_ASSERT_MAX_DURATIONS+=(\"\")\n");
        out.push_str("    SERVICE_ASSERT_DURATIONS+=(\"\")\n");
        out.push_str("    SERVICE_ASSERT_STATUS+=(\"none\")\n");
        out.push_str("    SERVICE_ASSERT_FAILURES+=(\"\")\n");
    }
    out.push_str("    SERVICE_LAUNCH_FNS+=(\"$launch_fn\")\n");
    out.push_str("    SERVICE_DEPENDENTS+=(\"$dependents_csv\")\n");
    out.push_str("    SERVICE_INDEX_BY_NAME[\"$name\"]=$index\n");
    out.push_str("  fi\n");
    out.push_str("  write_state_file\n");
    out.push_str("}\n\n");

    out.push_str("prune_restart_window() {\n");
    out.push_str("  local index=$1\n");
    out.push_str("  local now=${2:-$(date +%s)}\n");
    out.push_str("  local window_seconds=${SERVICE_WINDOW_SECONDS[index]:-0}\n");
    out.push_str("  local raw_timestamps=${SERVICE_RESTART_FAILURE_TIMESTAMPS[index]:-}\n");
    out.push_str("  local -a kept=()\n");
    out.push_str("  local -a timestamps=()\n");
    out.push_str("  local ts\n");
    out.push_str("  if [[ -n \"$raw_timestamps\" ]]; then\n");
    out.push_str("    read -r -a timestamps <<< \"$raw_timestamps\"\n");
    out.push_str("    for ts in \"${timestamps[@]}\"; do\n");
    out.push_str("      [[ -z \"$ts\" ]] && continue\n");
    out.push_str("      if (( now - ts < window_seconds )); then\n");
    out.push_str("        kept+=(\"$ts\")\n");
    out.push_str("      fi\n");
    out.push_str("    done\n");
    out.push_str("  fi\n");
    out.push_str("  SERVICE_RESTART_FAILURE_TIMESTAMPS[index]=\"${kept[*]:-}\"\n");
    out.push_str("  SERVICE_RESTART_FAILURES_IN_WINDOW[index]=\"${#kept[@]}\"\n");
    out.push_str("}\n\n");

    out.push_str("service_index_for() {\n");
    out.push_str("  local name=$1\n");
    out.push_str("  local index=${SERVICE_INDEX_BY_NAME[\"$name\"]:-}\n");
    out.push_str("  if [[ -z \"$index\" ]]; then\n");
    out.push_str("    echo \"Dependency '$name' was not launched\" >&2\n");
    out.push_str("    return 1\n");
    out.push_str("  fi\n");
    out.push_str("  printf '%s' \"$index\"\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_sleep() {\n");
    out.push_str("  local pid=$1\n");
    out.push_str("  local name=$2\n");
    out.push_str("  local seconds=$3\n");
    out.push_str("  local start\n");
    out.push_str("  start=$(date +%s)\n");
    out.push_str("  reset_wait_helper_exit_state\n");
    out.push_str("  while (( $(date +%s) - start < seconds )); do\n");
    out.push_str("    if ! kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      record_wait_helper_exit \"$pid\"\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("  if ! kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("    record_wait_helper_exit \"$pid\"\n");
    out.push_str("    return 1\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_tcp() {\n");
    out.push_str("  local pid=$1\n");
    out.push_str("  local name=$2\n");
    out.push_str("  local host=$3\n");
    out.push_str("  local port=$4\n");
    out.push_str("  local timeout=${5:-60}\n");
    out.push_str("  local start\n");
    out.push_str("  start=$(date +%s)\n");
    out.push_str("  reset_wait_helper_exit_state\n");
    out.push_str("  until bash -lc \"</dev/tcp/${host}/${port}\" >/dev/null 2>&1; do\n");
    out.push_str("    if ! kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      record_wait_helper_exit \"$pid\"\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    if (( $(date +%s) - start >= timeout )); then\n");
    out.push_str("      echo \"Timed out waiting for ${host}:${port} for service '$name'\" >&2\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_log() {\n");
    out.push_str("  local pid=$1\n");
    out.push_str("  local name=$2\n");
    out.push_str("  local logfile=$3\n");
    out.push_str("  local pattern=$4\n");
    out.push_str("  local timeout=${5:-60}\n");
    out.push_str("  local start\n");
    out.push_str("  start=$(date +%s)\n");
    out.push_str("  reset_wait_helper_exit_state\n");
    out.push_str("  until grep -E -q \"$pattern\" \"$logfile\" 2>/dev/null; do\n");
    out.push_str("    if ! kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      record_wait_helper_exit \"$pid\"\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    if (( $(date +%s) - start >= timeout )); then\n");
    out.push_str(
        "      echo \"Timed out waiting for readiness log pattern for service '$name'\" >&2\n",
    );
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_http() {\n");
    out.push_str("  local pid=$1\n");
    out.push_str("  local name=$2\n");
    out.push_str("  local url=$3\n");
    out.push_str("  local expected=$4\n");
    out.push_str("  local timeout=${5:-60}\n");
    out.push_str("  local start\n");
    out.push_str("  start=$(date +%s)\n");
    out.push_str("  reset_wait_helper_exit_state\n");
    out.push_str("  while true; do\n");
    out.push_str("    if ! kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      record_wait_helper_exit \"$pid\"\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    local code\n");
    out.push_str("    code=$(curl --silent --output /dev/null --write-out '%{http_code}' \"$url\" 2>/dev/null || true)\n");
    out.push_str("    if [[ \"$code\" == \"$expected\" ]]; then\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    if (( $(date +%s) - start >= timeout )); then\n");
    out.push_str(
        "      echo \"Timed out waiting for HTTP $expected from $url for service '$name'\" >&2\n",
    );
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_service_started() {\n");
    out.push_str("  local dependency=$1\n");
    out.push_str("  local target=$2\n");
    out.push_str("  local index\n");
    out.push_str("  while true; do\n");
    out.push_str("    index=$(service_index_for \"$dependency\") || return 1\n");
    out.push_str("    local pid=${SERVICE_PIDS[index]:-}\n");
    out.push_str("    if [[ -z \"$pid\" ]]; then\n");
    out.push_str("      echo \"Dependency '$dependency' for service '$target' does not have a tracked pid\" >&2\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    if kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    local status=0\n");
    out.push_str("    wait \"$pid\" || status=$?\n");
    out.push_str("    handle_service_exit \"$index\" \"$status\"\n");
    out.push_str("    local handled_status=$?\n");
    out.push_str(
        "    if (( handled_status == 0 )) && [[ -n \"${SERVICE_PIDS[index]:-}\" ]]; then\n",
    );
    out.push_str("      continue\n");
    out.push_str("    fi\n");
    out.push_str("    if (( handled_status == 0 )); then\n");
    out.push_str("      echo \"Dependency '$dependency' exited with status $status before service '$target' could start\" >&2\n");
    out.push_str("    fi\n");
    out.push_str("    return 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_service_healthy() {\n");
    out.push_str("  local dependency=$1\n");
    out.push_str("  local target=$2\n");
    out.push_str("  local wait_fn=$3\n");
    out.push_str("  local index\n");
    out.push_str("  while true; do\n");
    out.push_str("    index=$(service_index_for \"$dependency\") || return 1\n");
    out.push_str("    wait_for_service_started \"$dependency\" \"$target\" || return 1\n");
    out.push_str("    index=$(service_index_for \"$dependency\") || return 1\n");
    out.push_str("    if [[ \"${SERVICE_HEALTHY[index]:-0}\" == \"1\" ]]; then\n");
    if rendezvous_enabled {
        out.push_str("      register_service_rendezvous_by_index \"$index\" || return 1\n");
    }
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    local pid=${SERVICE_PIDS[index]}\n");
    out.push_str("    if \"$wait_fn\" \"$pid\" \"$dependency\"; then\n");
    out.push_str("      SERVICE_HEALTHY[index]=\"1\"\n");
    if rendezvous_enabled {
        out.push_str("      register_service_rendezvous_by_index \"$index\" || return 1\n");
    }
    out.push_str("      write_state_file\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    if [[ \"$WAIT_HELPER_EXITED\" == \"1\" ]]; then\n");
    out.push_str("      local status=$WAIT_HELPER_EXIT_STATUS\n");
    out.push_str("      handle_service_exit \"$index\" \"$status\"\n");
    out.push_str("      local handled_status=$?\n");
    out.push_str(
        "      if (( handled_status == 0 )) && [[ -n \"${SERVICE_PIDS[index]:-}\" ]]; then\n",
    );
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      if (( handled_status == 0 )); then\n");
    out.push_str("        echo \"Dependency '$dependency' exited with status $status before it became healthy for service '$target'\" >&2\n");
    out.push_str("      fi\n");
    out.push_str("    fi\n");
    out.push_str("    return 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("wait_for_service_completed_successfully() {\n");
    out.push_str("  local dependency=$1\n");
    out.push_str("  local target=$2\n");
    out.push_str("  local index\n");
    out.push_str("  while true; do\n");
    out.push_str("    index=$(service_index_for \"$dependency\") || return 1\n");
    out.push_str("    if [[ \"${SERVICE_COMPLETED_SUCCESSFULLY[index]:-0}\" == \"1\" ]]; then\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    local pid=${SERVICE_PIDS[index]:-}\n");
    out.push_str("    if [[ -z \"$pid\" ]]; then\n");
    out.push_str("      if [[ \"${SERVICE_LAST_EXIT_CODE[index]:-}\" == \"0\" ]]; then\n");
    out.push_str("        SERVICE_COMPLETED_SUCCESSFULLY[index]=\"1\"\n");
    out.push_str("        write_state_file\n");
    out.push_str("        return 0\n");
    out.push_str("      fi\n");
    out.push_str("      echo \"Dependency '$dependency' for service '$target' did not complete successfully\" >&2\n");
    out.push_str("      return 1\n");
    out.push_str("    fi\n");
    out.push_str("    local status=0\n");
    out.push_str("    wait \"$pid\" || status=$?\n");
    out.push_str("    handle_service_exit \"$index\" \"$status\"\n");
    out.push_str("    local handled_status=$?\n");
    out.push_str("    if (( handled_status != 0 )); then\n");
    out.push_str("      echo \"Dependency '$dependency' failed with status $handled_status before service '$target' could start\" >&2\n");
    out.push_str("      return \"$handled_status\"\n");
    out.push_str("    fi\n");
    out.push_str("    if [[ \"${SERVICE_COMPLETED_SUCCESSFULLY[index]:-0}\" == \"1\" ]]; then\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("emit_dependency_failure_diagnostic() {\n");
    out.push_str("  local failed_service=$1\n");
    out.push_str("  local index\n");
    out.push_str("  index=$(service_index_for \"$failed_service\") || return 0\n");
    out.push_str("  local dependents_csv=${SERVICE_DEPENDENTS[index]:-}\n");
    out.push_str("  [[ -z \"$dependents_csv\" ]] && return 0\n");
    out.push_str("  local formatted=${dependents_csv//,/ , }\n");
    out.push_str("  echo \"Service '$failed_service' is required by: $formatted\" >&2\n");
    out.push_str("}\n\n");

    out.push_str("write_service_exit_marker() {\n");
    out.push_str("  local index=$1\n");
    out.push_str("  local status=$2\n");
    out.push_str("  local service_name=${SERVICE_NAMES[index]:-unknown}\n");
    out.push_str("  local marker_name\n");
    out.push_str("  marker_name=$(printf '%s' \"$service_name\" | tr -c 'A-Za-z0-9_.-' '_')\n");
    out.push_str("  local marker=\"$SERVICE_EXIT_MARKER_DIR/${marker_name}.jsonl\"\n");
    out.push_str("  mkdir -p \"$SERVICE_EXIT_MARKER_DIR\"\n");
    out.push_str("  printf '{\"service\":\"%s\",\"exit_code\":%s,\"at_unix\":%s,\"node\":%s,\"rank\":%s,\"nodelist\":%s}\\n' \\\n");
    out.push_str("    \"$(json_escape \"$service_name\")\" \\\n");
    out.push_str("    \"$status\" \\\n");
    out.push_str("    \"$(date +%s)\" \\\n");
    out.push_str("    \"$(json_string_or_null \"${SERVICE_FIRST_FAILURE_NODE[index]:-}\")\" \\\n");
    out.push_str("    \"$(json_string_or_null \"${SERVICE_FIRST_FAILURE_RANK[index]:-}\")\" \\\n");
    out.push_str(
        "    \"$(json_string_or_null \"${SERVICE_STEP_NODELIST[index]:-}\")\" >> \"$marker\"\n",
    );
    out.push_str("}\n\n");

    out.push_str("handle_service_exit() {\n");
    out.push_str("  local index=$1\n");
    out.push_str("  local status=$2\n");
    out.push_str("  local name=${SERVICE_NAMES[index]:-}\n");
    out.push_str("  local mode=${SERVICE_FAILURE_POLICY_MODE[index]:-fail_job}\n");
    out.push_str("  local max_restarts=${SERVICE_MAX_RESTARTS[index]:-0}\n");
    out.push_str("  local restart_count=${SERVICE_RESTART_COUNT[index]:-0}\n");
    out.push_str("  local backoff_seconds=${SERVICE_BACKOFF_SECONDS[index]:-0}\n");
    out.push_str("  local window_seconds=${SERVICE_WINDOW_SECONDS[index]:-0}\n");
    out.push_str("  local max_restarts_in_window=${SERVICE_MAX_RESTARTS_IN_WINDOW[index]:-0}\n");
    out.push_str("  local launch_fn=${SERVICE_LAUNCH_FNS[index]:-}\n");
    out.push_str("  local logfile=${SERVICE_LOG_PATHS[index]:-}\n");
    out.push_str("  local host_epilogue_script=${SERVICE_HOST_EPILOGUE_SCRIPTS[index]:-}\n");
    out.push_str("  SERVICE_PIDS[index]=''\n");
    out.push_str("  local effective_status=$status\n");
    out.push_str("  if [[ -n \"$host_epilogue_script\" && \"${SERVICE_HOST_EPILOGUE_RAN[index]:-0}\" != \"1\" ]]; then\n");
    out.push_str("    SERVICE_HOST_EPILOGUE_RAN[index]=\"1\"\n");
    out.push_str("    local epilogue_status=0\n");
    out.push_str("    run_host_hook \"$host_epilogue_script\" \"$name\" epilogue \"$logfile\" \"$status\" || epilogue_status=$?\n");
    out.push_str("    if (( epilogue_status != 0 )); then\n");
    out.push_str(
        "      echo \"Service '$name' epilogue exited with status $epilogue_status\" >&2\n",
    );
    out.push_str("      if (( status == 0 )); then\n");
    out.push_str("        effective_status=$epilogue_status\n");
    out.push_str("      fi\n");
    out.push_str("    fi\n");
    out.push_str("  fi\n");
    out.push_str("  status=$effective_status\n");
    out.push_str("  SERVICE_FINISHED_AT[index]=\"$(date +%s)\"\n");
    out.push_str("  SERVICE_LAST_EXIT_CODE[index]=\"$status\"\n");
    out.push_str("  if (( status != 0 )) && [[ -z \"${SERVICE_FIRST_FAILURE_EXIT_CODE[index]:-}\" ]]; then\n");
    out.push_str("    SERVICE_FIRST_FAILURE_AT[index]=\"$(date +%s)\"\n");
    out.push_str("    SERVICE_FIRST_FAILURE_EXIT_CODE[index]=\"$status\"\n");
    out.push_str("    SERVICE_FIRST_FAILURE_NODE[index]=\"$(first_word \"${SERVICE_STEP_NODELIST[index]:-}\")\"\n");
    out.push_str("    SERVICE_FIRST_FAILURE_RANK[index]=\"\"\n");
    out.push_str("  fi\n");
    // Lifecycle marker: a timestamped command-exit line in the service log, so a
    // completed/failed run is visible inline alongside its output (complements the
    // machine-readable exit marker written next).
    out.push_str("  printf '[hpc-compose] %s service %s: command exited rc=%s\\n' \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\" \"${SERVICE_NAMES[index]:-?}\" \"$status\" >>\"${SERVICE_LOG_PATHS[index]}\" 2>/dev/null || true\n");
    out.push_str("  write_service_exit_marker \"$index\" \"$status\" || true\n");
    out.push_str("  if [[ \"$mode\" == \"restart_on_failure\" ]]; then\n");
    out.push_str("    prune_restart_window \"$index\"\n");
    out.push_str("  fi\n");
    out.push_str("  write_state_file\n");
    out.push_str("  if [[ \"$CLEANING_UP\" == \"1\" ]]; then\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  if (( status == 0 )); then\n");
    out.push_str("    SERVICE_COMPLETED_SUCCESSFULLY[index]=\"1\"\n");
    out.push_str("    write_state_file\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ \"$mode\" == \"ignore\" ]]; then\n");
    out.push_str(
        "    echo \"Service '$name' exited with status $status; continuing because failure_policy is ignore\" >&2\n",
    );
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ \"$mode\" == \"restart_on_failure\" ]]; then\n");
    out.push_str(
        "    local restart_failures_in_window=${SERVICE_RESTART_FAILURES_IN_WINDOW[index]:-0}\n",
    );
    out.push_str("    if (( restart_count >= max_restarts )); then\n");
    out.push_str(
        "      echo \"Service '$name' exited with status $status after $restart_count/$max_restarts restarts\" >&2\n",
    );
    out.push_str("      emit_dependency_failure_diagnostic \"$name\"\n");
    out.push_str("      return \"$status\"\n");
    out.push_str("    fi\n");
    out.push_str("    if (( restart_failures_in_window >= max_restarts_in_window )); then\n");
    out.push_str(
        "      echo \"Service '$name' exited with status $status after $restart_failures_in_window/$max_restarts_in_window restart-triggering exits in ${window_seconds}s\" >&2\n",
    );
    if hooks_enabled {
        out.push_str(
            "      run_service_event_hooks \"$index\" window_exhausted \"$status\" || true\n",
        );
    }
    out.push_str("      emit_dependency_failure_diagnostic \"$name\"\n");
    out.push_str("      return \"$status\"\n");
    out.push_str("    fi\n");
    out.push_str("    local now\n");
    out.push_str("    now=$(date +%s)\n");
    out.push_str(
        "    SERVICE_RESTART_FAILURE_TIMESTAMPS[index]=\"${SERVICE_RESTART_FAILURE_TIMESTAMPS[index]:-} $now\"\n",
    );
    out.push_str(
        "    SERVICE_RESTART_FAILURE_TIMESTAMPS[index]=\"${SERVICE_RESTART_FAILURE_TIMESTAMPS[index]# }\"\n",
    );
    out.push_str("    prune_restart_window \"$index\" \"$now\"\n");
    out.push_str("    local next_restart=$((restart_count + 1))\n");
    out.push_str("    SERVICE_RESTART_COUNT[index]=\"$next_restart\"\n");
    out.push_str("    write_state_file\n");
    out.push_str("    if [[ -z \"$launch_fn\" ]]; then\n");
    out.push_str(
        "      echo \"Service '$name' requested restart but no launch function is registered\" >&2\n",
    );
    out.push_str("      emit_dependency_failure_diagnostic \"$name\"\n");
    out.push_str("      return \"$status\"\n");
    out.push_str("    fi\n");
    if hooks_enabled {
        out.push_str("    run_service_event_hooks \"$index\" restart \"$status\" || true\n");
    }
    out.push_str("    if (( backoff_seconds > 0 )); then\n");
    out.push_str("      sleep \"$backoff_seconds\"\n");
    out.push_str("    fi\n");
    out.push_str(
        "    echo \"Service '$name' exited with status $status; restarting ($next_restart/$max_restarts)\" >&2\n",
    );
    out.push_str("    \"$launch_fn\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  echo \"Service '$name' exited with status $status\" >&2\n");
    out.push_str("  emit_dependency_failure_diagnostic \"$name\"\n");
    out.push_str("  return \"$status\"\n");
    out.push_str("}\n\n");

    out.push_str("restart_service_for_dev() {\n");
    out.push_str("  local name=$1\n");
    out.push_str("  local index=${SERVICE_INDEX_BY_NAME[\"$name\"]:-}\n");
    out.push_str("  if [[ -z \"$index\" ]]; then\n");
    out.push_str("    echo \"dev reload requested unknown service '$name'\" >&2\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  local pid=${SERVICE_PIDS[index]:-}\n");
    out.push_str("  if [[ -n \"$pid\" ]]; then\n");
    out.push_str("    if kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("      echo \"Dev reload: stopping service '$name'\" >&2\n");
    out.push_str("      kill \"$pid\" 2>/dev/null || true\n");
    out.push_str("    fi\n");
    out.push_str("    wait \"$pid\" 2>/dev/null || true\n");
    out.push_str("  fi\n");
    out.push_str("  SERVICE_PIDS[index]=''\n");
    out.push_str("  SERVICE_HEALTHY[index]='0'\n");
    out.push_str("  SERVICE_COMPLETED_SUCCESSFULLY[index]='0'\n");
    out.push_str("  SERVICE_LAST_EXIT_CODE[index]=''\n");
    out.push_str("  SERVICE_FINISHED_AT[index]=''\n");
    out.push_str("  write_state_file\n");
    out.push_str("  local launch_fn=${SERVICE_LAUNCH_FNS[index]:-}\n");
    out.push_str("  if [[ -z \"$launch_fn\" ]]; then\n");
    out.push_str("    echo \"dev reload requested service '$name' but no launch function is registered\" >&2\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  echo \"Dev reload: restarting service '$name'\" >&2\n");
    out.push_str("  \"$launch_fn\"\n");
    out.push_str("}\n\n");

    out.push_str("process_dev_restart_requests() {\n");
    out.push_str("  [[ -n \"${HPC_COMPOSE_DEV_CONTROL_DIR:-}\" ]] || return 0\n");
    out.push_str("  local request_dir=\"$HPC_COMPOSE_DEV_CONTROL_DIR/restart\"\n");
    out.push_str("  [[ -d \"$request_dir\" ]] || return 0\n");
    out.push_str("  local shopt_state\n");
    out.push_str("  shopt_state=$(shopt -p nullglob)\n");
    out.push_str("  shopt -s nullglob\n");
    out.push_str("  local -a request_files=(\"$request_dir\"/*.request)\n");
    out.push_str("  eval \"$shopt_state\"\n");
    out.push_str("  local request_file\n");
    out.push_str("  for request_file in \"${request_files[@]}\"; do\n");
    out.push_str("    [[ -f \"$request_file\" ]] || continue\n");
    out.push_str("    local service_name\n");
    out.push_str("    while IFS= read -r service_name; do\n");
    out.push_str("      [[ -z \"$service_name\" ]] && continue\n");
    out.push_str("      restart_service_for_dev \"$service_name\"\n");
    out.push_str("    done < \"$request_file\"\n");
    out.push_str("    rm -f \"$request_file\" || true\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("monitor_services() {\n");
    out.push_str("  while true; do\n");
    out.push_str("    process_dev_restart_requests\n");
    out.push_str("    local tracked=0\n");
    out.push_str("    for i in \"${!SERVICE_PIDS[@]}\"; do\n");
    out.push_str("      local pid=${SERVICE_PIDS[i]}\n");
    out.push_str("      [[ -z \"$pid\" ]] && continue\n");
    out.push_str("      tracked=$((tracked + 1))\n");
    out.push_str("      if kill -0 \"$pid\" 2>/dev/null; then\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      if wait \"$pid\"; then\n");
    out.push_str("        handle_service_exit \"$i\" 0\n");
    out.push_str("        local handled_status=$?\n");
    out.push_str("        if (( handled_status != 0 )); then\n");
    out.push_str("          return \"$handled_status\"\n");
    out.push_str("        fi\n");
    out.push_str("      else\n");
    out.push_str("        local status=$?\n");
    out.push_str("        handle_service_exit \"$i\" \"$status\"\n");
    out.push_str("        local handled_status=$?\n");
    out.push_str("        if (( handled_status != 0 )); then\n");
    out.push_str("          return \"$handled_status\"\n");
    out.push_str("        fi\n");
    out.push_str("      fi\n");
    out.push_str("    done\n");
    out.push_str("    if (( tracked == 0 )); then\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    if !plan.slurm.software_env.is_empty() {
        render_apply_software_env(&mut out, &plan.slurm.software_env, "");
        out.push('\n');
    }
    for setup in &plan.slurm.setup {
        out.push_str(setup);
        out.push('\n');
    }
    if !plan.slurm.setup.is_empty() {
        out.push('\n');
    }
    if metrics_enabled {
        out.push_str("start_metrics_sampler\n\n");
    }
    if stage_enabled {
        out.push_str("stage_in_paths\n\n");
    }
    if hf_stage_enabled {
        out.push_str("stage_in_huggingface_artifacts\n\n");
    }
    if !rendezvous_client_names.is_empty() {
        out.push_str("resolve_rendezvous_dependencies\n\n");
    }

    for service in &plan.ordered_services {
        render_readiness_wait(&mut out, service);
        out.push('\n');
    }

    for (service_index, service) in plan.ordered_services.iter().enumerate() {
        let dependents = dependents_by_service
            .get(&service.name)
            .cloned()
            .unwrap_or_default();
        let service_context = RenderServiceContext {
            service_index,
            dependents: &dependents,
            global_software_env: &plan.slurm.software_env,
            slurm: &plan.slurm,
            runtime: &plan.runtime,
            options,
            scratch_configured: scratch_enabled,
            allocation_gpu_requested: allocation_requests_gpu(&plan.slurm),
        };
        render_service(&mut out, service, &service_context);
        out.push('\n');
    }

    for service in &plan.ordered_services {
        render_dependency_waits(&mut out, service);
        let fn_name = format!("launch_{}", service_token(&service.name));
        out.push_str(&format!("{fn_name}\n"));
        out.push('\n');
    }

    out.push_str("monitor_services\n");
    Ok(out)
}

struct RenderServiceContext<'a> {
    service_index: usize,
    dependents: &'a [String],
    global_software_env: &'a SoftwareEnvConfig,
    slurm: &'a SlurmConfig,
    runtime: &'a crate::spec::RuntimeConfig,
    options: &'a RenderOptions,
    scratch_configured: bool,
    allocation_gpu_requested: bool,
}

fn render_service_rendezvous_registration(
    out: &mut String,
    register: &RendezvousRegisterConfig,
    readiness_wait_fn: Option<&str>,
) {
    let protocol = register.protocol.as_deref().unwrap_or("http");
    let path = register.path.as_deref().unwrap_or("");
    let ttl = register.ttl_seconds.unwrap_or(3600);
    out.push_str("  local rdzv_index=${SERVICE_INDEX_BY_NAME[\"$service_name\"]}\n");
    out.push_str(&format!(
        "  SERVICE_RDZV_NAMES[rdzv_index]={}\n",
        shell_quote(&register.name)
    ));
    out.push_str(&format!(
        "  SERVICE_RDZV_PORTS[rdzv_index]={}\n",
        register.port
    ));
    out.push_str(&format!(
        "  SERVICE_RDZV_PROTOCOLS[rdzv_index]={}\n",
        shell_quote(protocol)
    ));
    out.push_str(&format!(
        "  SERVICE_RDZV_PATHS[rdzv_index]={}\n",
        shell_quote(path)
    ));
    out.push_str(&format!("  SERVICE_RDZV_TTLS[rdzv_index]={ttl}\n"));
    let metadata_json =
        serde_json::to_string(&register.metadata).expect("rendezvous metadata serializes");
    out.push_str(&format!(
        "  SERVICE_RDZV_METADATA_JSON[rdzv_index]={}\n",
        shell_quote(&metadata_json)
    ));
    out.push_str("  SERVICE_RDZV_REGISTERED[rdzv_index]=\"0\"\n");
    if let Some(wait_fn) = readiness_wait_fn {
        out.push_str(&format!(
            "  if {wait_fn} \"$pid\" \"$service_name\"; then\n"
        ));
        out.push_str("    SERVICE_HEALTHY[rdzv_index]=\"1\"\n");
        out.push_str("    register_service_rendezvous_by_index \"$rdzv_index\"\n");
        out.push_str("    write_state_file\n");
        out.push_str("  else\n");
        out.push_str("    return 1\n");
        out.push_str("  fi\n");
    } else {
        out.push_str("  register_service_rendezvous_by_index \"$rdzv_index\"\n");
    }
}

fn render_service(out: &mut String, service: &RuntimeService, context: &RenderServiceContext<'_>) {
    let service_id = service_token(&service.name);
    let fn_name = format!("launch_{service_id}");
    let step_name = service_step_name(&service.name);
    let log_file_name = log_file_name_for_service(&service.name);
    let container_log_path = tracked_paths::under_job_container_dir(&format!(
        "{}/{log_file_name}",
        tracked_paths::LOGS_DIR_NAME
    ));
    let command_args = execution_argv(&service.execution, service.working_dir.as_deref());
    let distributed = distributed_render_env(
        service,
        context.slurm,
        context.options.cluster_profile.as_ref(),
    );
    let distributed_extra_container_env = distributed
        .profile_env
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let software_env_keys =
        software_env_export_names(context.global_software_env, &service.slurm.software_env);
    let mut extra_container_env = distributed_extra_container_env;
    extra_container_env.extend(software_env_keys.clone());
    if let Some(rendezvous) = &context.slurm.rendezvous {
        extra_container_env.extend(rendezvous_environment_names(&rendezvous.discover));
    }
    if context.slurm.array.is_some() {
        extra_container_env.extend(
            [
                "SLURM_ARRAY_JOB_ID",
                "SLURM_ARRAY_TASK_ID",
                "SLURM_ARRAY_TASK_COUNT",
                "SLURM_ARRAY_TASK_MAX",
                "SLURM_ARRAY_TASK_MIN",
                "SLURM_ARRAY_TASK_STEP",
            ]
            .into_iter()
            .map(str::to_string),
        );
    }
    let srun_args = build_srun_command_for_backend_with_extra_container_env(
        service,
        context.runtime.backend,
        &extra_container_env,
    );
    let dependents_csv = context.dependents.join(",");
    let service_env = service
        .environment
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>();
    let software_env =
        effective_software_env_pairs(context.global_software_env, &service.slurm.software_env);
    let host_prologue = service
        .slurm
        .prologue
        .as_ref()
        .filter(|hook| hook.context == ServiceHookContext::Host);
    let host_epilogue = service
        .slurm
        .epilogue
        .as_ref()
        .filter(|hook| hook.context == ServiceHookContext::Host);
    let container_prologue = service
        .slurm
        .prologue
        .as_ref()
        .filter(|hook| hook.context == ServiceHookContext::Container);
    let container_epilogue = service
        .slurm
        .epilogue
        .as_ref()
        .filter(|hook| hook.context == ServiceHookContext::Container);
    let has_container_hooks = container_prologue.is_some() || container_epilogue.is_some();

    out.push_str(&format!("{fn_name}() {{\n"));
    out.push_str(&format!(
        "  local service_name={}\n",
        shell_quote(&service.name)
    ));
    out.push_str(&format!("  local logfile=\"$LOG_DIR/{log_file_name}\"\n"));
    out.push_str("  if [[ -z \"${SERVICE_INDEX_BY_NAME[\"$service_name\"]:-}\" ]]; then\n");
    out.push_str("    : > \"$logfile\"\n");
    out.push_str("  fi\n");
    out.push_str("  local host_prologue_script=\"\"\n");
    out.push_str("  local host_epilogue_script=\"\"\n");
    out.push_str("  local event_hooks_manifest=\"\"\n");
    if let Some(hook) = host_prologue {
        let file = hook_file_name(&service_id, "host-prologue");
        let target = format!("\"$HOOKS_DIR/{file}\"");
        let body = host_hook_file_body(&hook.script);
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_HOOK_{}_HOST_PROLOGUE", service_id),
            &body,
        );
        out.push_str(&format!("  host_prologue_script=\"$HOOKS_DIR/{file}\"\n"));
    }
    if let Some(hook) = host_epilogue {
        let file = hook_file_name(&service_id, "host-epilogue");
        let target = format!("\"$HOOKS_DIR/{file}\"");
        let body = host_hook_file_body(&hook.script);
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_HOOK_{}_HOST_EPILOGUE", service_id),
            &body,
        );
        out.push_str(&format!("  host_epilogue_script=\"$HOOKS_DIR/{file}\"\n"));
    }
    if !service.slurm.hooks.is_empty() {
        let manifest_file = format!("{service_id}.event-hooks.tsv");
        out.push_str(&format!(
            "  event_hooks_manifest=\"$HOOKS_DIR/{manifest_file}\"\n"
        ));
        out.push_str("  : > \"$event_hooks_manifest\"\n");
        for (index, hook) in service.slurm.hooks.iter().enumerate() {
            let event = hook_event_label(hook.on);
            let file = hook_file_name(&service_id, &format!("host-event-{event}-{index}"));
            let target = format!("\"$HOOKS_DIR/{file}\"");
            let body = host_hook_file_body(&hook.script);
            push_hook_file(
                out,
                &target,
                &format!(
                    "HPC_COMPOSE_HOOK_{}_HOST_EVENT_{}_{}",
                    service_id, event, index
                ),
                &body,
            );
            out.push_str(&format!(
                "  printf '%s\\t%s\\n' {} \"$HOOKS_DIR/{file}\" >> \"$event_hooks_manifest\"\n",
                shell_quote(event)
            ));
        }
    }
    if let Some(hook) = container_prologue {
        let file = hook_file_name(&service_id, "container-prologue");
        let target = format!("\"$HOOKS_DIR/{file}\"");
        let body = container_hook_file_body(&hook.script);
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_HOOK_{}_CONTAINER_PROLOGUE", service_id),
            &body,
        );
    }
    if let Some(hook) = container_epilogue {
        let file = hook_file_name(&service_id, "container-epilogue");
        let target = format!("\"$HOOKS_DIR/{file}\"");
        let body = container_hook_file_body(&hook.script);
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_HOOK_{}_CONTAINER_EPILOGUE", service_id),
            &body,
        );
    }
    if has_container_hooks {
        let wrapper_file = hook_file_name(&service_id, "container-wrapper");
        let hooks_container_dir =
            tracked_paths::under_job_container_dir(tracked_paths::HOOKS_DIR_NAME);
        let prologue_file = container_prologue.map(|_| {
            format!(
                "{hooks_container_dir}/{}",
                hook_file_name(&service_id, "container-prologue")
            )
        });
        let epilogue_file = container_epilogue.map(|_| {
            format!(
                "{hooks_container_dir}/{}",
                hook_file_name(&service_id, "container-epilogue")
            )
        });
        let body = container_wrapper_body(
            &service.name,
            &container_log_path,
            prologue_file.as_deref(),
            epilogue_file.as_deref(),
        );
        let target = format!("\"$HOOKS_DIR/{wrapper_file}\"");
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_HOOK_{}_CONTAINER_WRAPPER", service_id),
            &body,
        );
    }
    out.push_str(&format!(
        "  local -a service_mounts={}\n",
        bash_array_literal(&service.volumes)
    ));
    out.push_str(&format!(
        "  local -a srun_cmd={}\n",
        bash_array_literal(&srun_args)
    ));
    if distributed.enabled && matches!(service.execution, ExecutionSpec::ImageDefault) {
        let prolog_file = format!("{service_id}.dist-rank-prolog.sh");
        let target = format!("\"$ALLOCATION_DIR/{prolog_file}\"");
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_DIST_RANK_PROLOG_{service_id}"),
            &distributed_rank_task_prolog_body(),
        );
        out.push_str(&format!(
            "  local dist_rank_task_prolog=\"$ALLOCATION_DIR/{prolog_file}\"\n"
        ));
        out.push_str("  chmod +x \"$dist_rank_task_prolog\"\n");
        out.push_str("  srun_cmd+=(\"--task-prolog=$dist_rank_task_prolog\")\n");
    }
    out.push_str(&format!(
        "  local -a service_cmd={}\n",
        bash_array_literal(&command_args)
    ));
    if has_container_hooks {
        let wrapper_file = hook_file_name(&service_id, "container-wrapper");
        out.push_str(&format!(
            "  local container_wrapper=\"$HOOKS_CONTAINER_DIR/{wrapper_file}\"\n"
        ));
        out.push_str("  service_cmd=(\"/bin/sh\" \"$container_wrapper\" \"${service_cmd[@]}\")\n");
    }
    if distributed.enabled && !matches!(service.execution, ExecutionSpec::ImageDefault) {
        let wrapper_file = format!("{service_id}.dist-env.sh");
        let target = format!("\"$ALLOCATION_DIR/{wrapper_file}\"");
        push_hook_file(
            out,
            &target,
            &format!("HPC_COMPOSE_DIST_ENV_{service_id}"),
            &distributed_env_wrapper_body(),
        );
        out.push_str(&format!(
            "  local distributed_env_wrapper=\"{}\"\n",
            tracked_paths::under_job_container_dir(&format!(
                "{}/{wrapper_file}",
                tracked_paths::ALLOCATION_DIR_NAME
            ))
        ));
        out.push_str(
            "  service_cmd=(\"/bin/sh\" \"$distributed_env_wrapper\" \"${service_cmd[@]}\")\n",
        );
    }
    out.push_str(&format!(
        "  local scratch_enabled={}\n",
        if context.scratch_configured && service_scratch_enabled(service) {
            "1"
        } else {
            "0"
        }
    ));
    out.push_str("  local runtime_mounts\n");
    out.push_str("  local HPC_COMPOSE_SERVICE_SCRATCH_ENABLED=\"$scratch_enabled\"\n");
    out.push_str("  runtime_mounts=$(build_pyxis_mounts \"${service_mounts[@]}\")\n");
    if context.runtime.backend == RuntimeBackend::Pyxis {
        out.push_str("  srun_cmd+=(--container-image=");
        out.push_str(&shell_quote(&service.runtime_image.display().to_string()));
        out.push_str(")\n");
        out.push_str("  if [[ -n \"$runtime_mounts\" ]]; then\n");
        out.push_str("    srun_cmd+=(\"--container-mounts=$runtime_mounts\")\n");
        out.push_str("  fi\n");
    }
    if let Some(indices) = &service.placement.node_indices {
        let index_args = indices.iter().map(u32::to_string).collect::<Vec<_>>();
        out.push_str(&format!(
            "  local -a service_node_indices={}\n",
            bash_array_literal(&index_args)
        ));
        out.push_str("  local service_nodelist\n");
        out.push_str("  service_nodelist=$(nodes_for_indices \"${service_node_indices[@]}\")\n");
        out.push_str("  local service_srun_nodelist\n");
        out.push_str("  service_srun_nodelist=$(comma_join_words \"$service_nodelist\")\n");
        out.push_str("  srun_cmd+=(\"--nodelist=$service_srun_nodelist\")\n");
    } else if service.placement.pin_to_primary_node {
        out.push_str("  local service_nodelist=\"$HPC_COMPOSE_PRIMARY_NODE\"\n");
        out.push_str("  local service_srun_nodelist=\"$HPC_COMPOSE_PRIMARY_NODE\"\n");
        out.push_str("  srun_cmd+=(\"--nodelist=$service_srun_nodelist\")\n");
    } else {
        out.push_str("  local service_nodelist=\"$HPC_COMPOSE_NODELIST\"\n");
        out.push_str("  local service_srun_nodelist=\"\"\n");
    }
    if !service.placement.exclude_indices.is_empty() {
        let exclude_args = service
            .placement
            .exclude_indices
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>();
        out.push_str(&format!(
            "  local -a service_exclude_indices={}\n",
            bash_array_literal(&exclude_args)
        ));
        out.push_str("  local service_exclude_nodelist\n");
        out.push_str(
            "  service_exclude_nodelist=$(nodes_for_indices \"${service_exclude_indices[@]}\")\n",
        );
        out.push_str("  local service_srun_exclude\n");
        out.push_str("  service_srun_exclude=$(comma_join_words \"$service_exclude_nodelist\")\n");
        out.push_str("  srun_cmd+=(\"--exclude=$service_srun_exclude\")\n");
    }
    out.push_str("  local service_primary_node\n");
    out.push_str("  service_primary_node=$(first_word \"$service_nodelist\")\n");
    out.push_str("  local service_node_count\n");
    out.push_str("  service_node_count=$(word_count \"$service_nodelist\")\n");
    out.push_str(&format!(
        "  local service_nodelist_file=\"$SERVICE_NODELIST_DIR/{}.nodes.txt\"\n",
        service_id
    ));
    out.push_str(&format!(
        "  local service_nodelist_container=\"$SERVICE_NODELIST_CONTAINER_DIR/{}.nodes.txt\"\n",
        service_id
    ));
    out.push_str("  write_nodelist_file \"$service_nodelist_file\" \"$service_nodelist\"\n");
    if distributed.enabled {
        let dist_hostfile_name = format!("{}.hostfile", service_token(&service.name));
        out.push_str(&format!(
            "  local dist_nproc_per_node={}\n",
            distributed.nproc_per_node
        ));
        out.push_str("  local dist_world_size=$(( service_node_count * dist_nproc_per_node ))\n");
        out.push_str(&format!(
            "  local dist_hostfile=\"$DIST_HOSTFILE_DIR/{}\"\n",
            dist_hostfile_name
        ));
        out.push_str(&format!(
            "  local dist_hostfile_container=\"$DIST_HOSTFILE_CONTAINER_DIR/{}\"\n",
            dist_hostfile_name
        ));
        out.push_str(
            "  write_mpi_hostfile \"$dist_hostfile\" \"$service_nodelist\" \"$dist_nproc_per_node\"\n",
        );
        let fixed_port = distributed
            .rdzv_port
            .map(|port| port.to_string())
            .unwrap_or_default();
        out.push_str("  local dist_master_port\n");
        out.push_str(&format!(
            "  dist_master_port=$(hpc_compose_dist_port {} {} {} {})\n",
            shell_quote(&fixed_port),
            distributed.rdzv_port_base,
            distributed.rdzv_port_span,
            context.service_index
        ));
    }
    if let Some(mpi) = &service.slurm.mpi {
        let hostfile_name = format!("{}.hostfile", service_token(&service.name));
        let slots = mpi_hostfile_slots(service)
            .map(|value| value.to_string())
            .unwrap_or_default();
        out.push_str(&format!(
            "  local mpi_hostfile=\"$MPI_HOSTFILE_DIR/{}\"\n",
            hostfile_name
        ));
        out.push_str(&format!(
            "  local mpi_hostfile_container=\"$MPI_HOSTFILE_CONTAINER_DIR/{}\"\n",
            hostfile_name
        ));
        out.push_str(&format!(
            "  write_mpi_hostfile \"$mpi_hostfile\" \"$service_nodelist\" {}\n",
            shell_quote(&slots)
        ));
        out.push_str(&format!(
            "  local mpi_type={}\n",
            shell_quote(mpi.mpi_type.as_srun_value())
        ));
    }
    out.push_str("  echo \"Starting service $service_name\"\n");
    out.push_str("  local -a launch_env=()\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_PRIMARY_NODE=$HPC_COMPOSE_PRIMARY_NODE\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_NODE_COUNT=$HPC_COMPOSE_NODE_COUNT\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_NODELIST=$HPC_COMPOSE_NODELIST\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_NODELIST_FILE=$HPC_COMPOSE_NODELIST_FILE\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_SERVICE_NAME=$service_name\")\n");
    out.push_str(&format!(
        "  launch_env+=(\"HPC_COMPOSE_SERVICE_LOG={container_log_path}\")\n"
    ));
    out.push_str("  launch_env+=(\"HPC_COMPOSE_SERVICE_PRIMARY_NODE=$service_primary_node\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_SERVICE_NODE_COUNT=$service_node_count\")\n");
    out.push_str("  launch_env+=(\"HPC_COMPOSE_SERVICE_NODELIST=$service_nodelist\")\n");
    out.push_str(
        "  launch_env+=(\"HPC_COMPOSE_SERVICE_NODELIST_FILE=$service_nodelist_container\")\n",
    );
    // Portable per-job scratch dir. In container backends $JOB_TMP is bind-mounted
    // at /hpc-compose/job, so that is the path services see; the host backend has
    // no mount, so point services at $JOB_TMP directly. Writing under
    // $HPC_COMPOSE_JOB_DIR keeps the same spec working on both backends and lands
    // files where artifact collection looks (artifacts declared as
    // /hpc-compose/job/** remap to $JOB_TMP/** on the host).
    if context.runtime.backend == RuntimeBackend::Host {
        out.push_str("  launch_env+=(\"HPC_COMPOSE_JOB_DIR=$JOB_TMP\")\n");
    } else {
        out.push_str("  launch_env+=(\"HPC_COMPOSE_JOB_DIR=/hpc-compose/job\")\n");
    }
    if let Some(parallelism) = &service.slurm.parallelism {
        // Descriptive tensor/pipeline sizes. Emitted for single-node services
        // too, so this lives OUTSIDE the `distributed.enabled` gate. No Slurm
        // flag is involved; these are literal env exports only.
        out.push_str(&format!(
            "  launch_env+=(\"HPC_COMPOSE_TP_SIZE={}\")\n",
            parallelism.tensor
        ));
        out.push_str(&format!(
            "  launch_env+=(\"HPC_COMPOSE_PP_SIZE={}\")\n",
            parallelism.pipeline
        ));
    }
    if distributed.enabled {
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_MASTER_ADDR=$service_primary_node\")\n");
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_MASTER_PORT=$dist_master_port\")\n");
        out.push_str(
            "  launch_env+=(\"HPC_COMPOSE_DIST_RDZV_ENDPOINT=$service_primary_node:$dist_master_port\")\n",
        );
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_NNODES=$service_node_count\")\n");
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_NPROC_PER_NODE=$dist_nproc_per_node\")\n");
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_WORLD_SIZE=$dist_world_size\")\n");
        out.push_str("  launch_env+=(\"HPC_COMPOSE_DIST_HOSTFILE=$dist_hostfile_container\")\n");
        if !distributed.profile_env.is_empty() {
            let profile_env = distributed
                .profile_env
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>();
            out.push_str(&format!(
                "  local -a distributed_profile_env={}\n",
                bash_array_literal(&profile_env)
            ));
            out.push_str("  launch_env+=(\"${distributed_profile_env[@]}\")\n");
        }
    }
    out.push_str("  if [[ \"$scratch_enabled\" == \"1\" ]]; then\n");
    if context.runtime.backend == RuntimeBackend::Host {
        out.push_str("    launch_env+=(\"HPC_COMPOSE_SCRATCH_DIR=$SCRATCH_HOST_PATH\")\n");
    } else {
        out.push_str("    launch_env+=(\"HPC_COMPOSE_SCRATCH_DIR=$SCRATCH_CONTAINER_PATH\")\n");
    }
    out.push_str("    launch_env+=(\"HPC_COMPOSE_SCRATCH_HOST_PATH=$SCRATCH_HOST_PATH\")\n");
    out.push_str("  fi\n");
    if service.slurm.mpi.is_some() {
        out.push_str("  launch_env+=(\"HPC_COMPOSE_MPI_HOSTFILE=$mpi_hostfile_container\")\n");
        out.push_str("  launch_env+=(\"HPC_COMPOSE_MPI_TYPE=$mpi_type\")\n");
        if let Some(mpi) = &service.slurm.mpi {
            if let Some(profile) = mpi.profile {
                out.push_str(&format!(
                    "  launch_env+=({})\n",
                    shell_quote(&format!("HPC_COMPOSE_MPI_PROFILE={}", profile.as_str()))
                ));
            }
            if let Some(implementation) = mpi.resolved_implementation() {
                out.push_str(&format!(
                    "  launch_env+=({})\n",
                    shell_quote(&format!(
                        "HPC_COMPOSE_MPI_IMPLEMENTATION={}",
                        implementation.as_str()
                    ))
                ));
            }
        }
    }
    if context.slurm.rendezvous.is_some() {
        out.push_str("  launch_env+=(\"${RDZV_LAUNCH_ENV[@]:-}\")\n");
    }
    out.push_str("  if [[ \"$RESUME_ENABLED\" == \"1\" ]]; then\n");
    // Same host/container split as $HPC_COMPOSE_JOB_DIR and $HPC_COMPOSE_SCRATCH_DIR
    // above: $RESUME_CONTAINER_PATH (/hpc-compose/resume) is only bind-mounted under
    // container backends, so the host backend must see $RESUME_HOST_PATH directly
    // (mkdir'd above) — otherwise a resuming host service reads an unmounted path.
    if context.runtime.backend == RuntimeBackend::Host {
        out.push_str("    launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_HOST_PATH\")\n");
    } else {
        out.push_str("    launch_env+=(\"HPC_COMPOSE_RESUME_DIR=$RESUME_CONTAINER_PATH\")\n");
    }
    out.push_str("    launch_env+=(\"HPC_COMPOSE_ATTEMPT=$ATTEMPT\")\n");
    out.push_str("    launch_env+=(\"HPC_COMPOSE_IS_RESUME=$IS_RESUME\")\n");
    out.push_str("  fi\n");
    if !service.environment.is_empty() {
        out.push_str(&format!(
            "  local -a service_env={}\n",
            bash_array_literal(&service_env)
        ));
        out.push_str("  launch_env+=(\"${service_env[@]}\")\n");
    }
    if !software_env.is_empty() {
        out.push_str(&format!(
            "  local -a software_env={}\n",
            bash_array_literal(&software_env)
        ));
        out.push_str("  launch_env+=(\"${software_env[@]}\")\n");
    }
    out.push_str("  local pid\n");
    out.push_str("  local prologue_status=0\n");
    out.push_str("  if [[ -n \"$host_prologue_script\" ]]; then\n");
    out.push_str("    run_host_hook \"$host_prologue_script\" \"$service_name\" prologue \"$logfile\" \"\" || prologue_status=$?\n");
    out.push_str("  fi\n");
    out.push_str("  if (( prologue_status != 0 )); then\n");
    out.push_str(
        "    echo \"Service '$service_name' prologue exited with status $prologue_status\" >&2\n",
    );
    out.push_str("    host_epilogue_script=\"\"\n");
    out.push_str("    ( exit \"$prologue_status\" ) &\n");
    out.push_str("    pid=$!\n");
    out.push_str("  else\n");
    render_runtime_command(
        out,
        service,
        context.runtime,
        context.options,
        context.allocation_gpu_requested,
    );
    out.push_str("    (\n");
    out.push_str("      set -euo pipefail\n");
    if !service.slurm.software_env.is_empty() {
        render_apply_software_env(out, &service.slurm.software_env, "      ");
    }
    // Lifecycle marker: a timestamped line in the service log right before the
    // container launch, so the gap before the command's own first output (srun
    // scheduling + container image extract) is visible instead of looking stuck.
    out.push_str("      printf '[hpc-compose] %s service %s: container starting via srun (image extract on first node use can take a moment)\\n' \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\" \"$service_name\" >>\"$logfile\" 2>&1\n");
    out.push_str("      if (( ${#launch_env[@]} == 0 )); then\n");
    out.push_str("        \"${srun_cmd[@]}\" \"${runtime_cmd[@]}\" >>\"$logfile\" 2>&1\n");
    out.push_str("      else\n");
    out.push_str("        env \"${launch_env[@]}\" \"${srun_cmd[@]}\" \"${runtime_cmd[@]}\" >>\"$logfile\" 2>&1\n");
    out.push_str("      fi\n");
    out.push_str("    ) &\n");
    out.push_str("    pid=$!\n");
    out.push_str("  fi\n");
    out.push_str(&format!(
        "  register_service {} \"$pid\" {} \"$logfile\" {} {} {} {} {} {} {} {} {} {} {} {} {} \"$host_epilogue_script\" \"$event_hooks_manifest\"\n",
        shell_quote(&service.name),
        shell_quote(&step_name),
        shell_quote(failure_policy_mode_label(service.failure_policy.mode)),
        service.failure_policy.max_restarts,
        service.failure_policy.backoff_seconds,
        service.failure_policy.window_seconds,
        service.failure_policy.max_restarts_in_window,
        shell_quote(&fn_name),
        shell_quote(&dependents_csv),
        shell_quote(placement_mode_label(service.placement.mode)),
        service.placement.nodes,
        shell_quote(
            &service
                .placement
                .ntasks
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ),
        shell_quote(
            &service
                .placement
                .ntasks_per_node
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ),
        "\"$service_nodelist\"",
        if service.readiness.is_some() {
            "1"
        } else {
            "0"
        }
    ));
    if let Some(register) = service
        .slurm
        .rendezvous
        .as_ref()
        .and_then(|rendezvous| rendezvous.register.as_ref())
    {
        let readiness_wait_fn = service
            .readiness
            .as_ref()
            .map(|_| format!("wait_until_{}_ready", service_token(&service.name)));
        render_service_rendezvous_registration(out, register, readiness_wait_fn.as_deref());
    }
    if let Some(assertions) = &service.assertions {
        let expected_exit = assertions
            .exit_code
            .map(|value| value.to_string())
            .unwrap_or_default();
        let artifact_pattern = assertions
            .normalized_artifacts_contain()
            .unwrap_or_default();
        let max_duration = assertions
            .max_duration_seconds
            .map(|value| value.to_string())
            .unwrap_or_default();
        out.push_str("  local assert_index=${SERVICE_INDEX_BY_NAME[\"$service_name\"]}\n");
        out.push_str(&format!(
            "  SERVICE_ASSERT_EXIT_CODES[assert_index]={}\n",
            shell_quote(&expected_exit)
        ));
        out.push_str(&format!(
            "  SERVICE_ASSERT_ARTIFACT_PATTERNS[assert_index]={}\n",
            shell_quote(&artifact_pattern)
        ));
        out.push_str(&format!(
            "  SERVICE_ASSERT_MAX_DURATIONS[assert_index]={}\n",
            shell_quote(&max_duration)
        ));
        out.push_str("  SERVICE_ASSERT_DURATIONS[assert_index]=\"\"\n");
        out.push_str("  SERVICE_ASSERT_STATUS[assert_index]=\"pending\"\n");
        out.push_str("  SERVICE_ASSERT_FAILURES[assert_index]=\"\"\n");
        out.push_str("  write_state_file\n");
    }
    out.push_str("}\n");
}

fn render_runtime_command(
    out: &mut String,
    service: &RuntimeService,
    runtime: &crate::spec::RuntimeConfig,
    render_options: &RenderOptions,
    allocation_gpu_requested: bool,
) {
    match runtime.backend {
        RuntimeBackend::Pyxis | RuntimeBackend::Host => {
            out.push_str("    local -a runtime_cmd=(\"${service_cmd[@]}\")\n");
        }
        RuntimeBackend::Apptainer | RuntimeBackend::Singularity => {
            let binary = match runtime.backend {
                RuntimeBackend::Apptainer => render_options.apptainer_bin.as_str(),
                RuntimeBackend::Singularity => render_options.singularity_bin.as_str(),
                RuntimeBackend::Pyxis | RuntimeBackend::Host => unreachable!(),
            };
            let subcommand = if matches!(service.execution, ExecutionSpec::ImageDefault) {
                "run"
            } else {
                "exec"
            };
            out.push_str(&format!(
                "    local -a runtime_cmd=({} {})\n",
                shell_quote(binary),
                shell_quote(subcommand)
            ));
            if service_needs_nv(service, runtime, allocation_gpu_requested) {
                out.push_str("    runtime_cmd+=(--nv)\n");
            }
            out.push_str("    if [[ -n \"$runtime_mounts\" ]]; then\n");
            out.push_str("      runtime_cmd+=(--bind \"$runtime_mounts\")\n");
            out.push_str("    fi\n");
            out.push_str("    runtime_cmd+=(");
            out.push_str(&shell_quote(&service.runtime_image.display().to_string()));
            out.push_str(")\n");
            out.push_str("    runtime_cmd+=(\"${service_cmd[@]}\")\n");
        }
    }
}

fn service_scratch_enabled(service: &RuntimeService) -> bool {
    service
        .slurm
        .scratch
        .as_ref()
        .and_then(|scratch| scratch.enabled)
        .unwrap_or(true)
}

fn allocation_requests_gpu(slurm: &crate::spec::SlurmConfig) -> bool {
    slurm.gpus.unwrap_or(0) > 0
        || slurm.gpus_per_node.unwrap_or(0) > 0
        || slurm.gpus_per_task.unwrap_or(0) > 0
        || slurm.cpus_per_gpu.unwrap_or(0) > 0
        || slurm.mem_per_gpu.is_some()
        || slurm
            .gres
            .as_deref()
            .is_some_and(|gres| gres.contains("gpu"))
}

fn service_needs_nv(
    service: &RuntimeService,
    runtime: &crate::spec::RuntimeConfig,
    allocation_gpu_requested: bool,
) -> bool {
    match runtime.gpu {
        RuntimeGpuPolicy::None => false,
        RuntimeGpuPolicy::Nvidia => true,
        RuntimeGpuPolicy::Auto => {
            allocation_gpu_requested
                || service.slurm.gpus.unwrap_or(0) > 0
                || service.slurm.gpus_per_node.unwrap_or(0) > 0
                || service.slurm.gpus_per_task.unwrap_or(0) > 0
                || service.slurm.cpus_per_gpu.unwrap_or(0) > 0
                || service.slurm.mem_per_gpu.is_some()
                || service
                    .slurm
                    .gres
                    .as_deref()
                    .is_some_and(|gres| gres.contains("gpu"))
        }
    }
}

fn render_readiness_wait(out: &mut String, service: &RuntimeService) {
    let fn_name = format!("wait_until_{}_ready", service_token(&service.name));
    out.push_str(&format!("{fn_name}() {{\n"));
    out.push_str("  local pid=$1\n");
    out.push_str("  local name=$2\n");
    if let Some(readiness) = &service.readiness {
        match readiness {
            ReadinessSpec::Sleep { seconds } => {
                out.push_str(&format!("  wait_for_sleep \"$pid\" \"$name\" {seconds}\n"));
            }
            ReadinessSpec::Tcp {
                host,
                port,
                timeout_seconds,
            } => {
                let host = host.as_deref().unwrap_or("127.0.0.1");
                let timeout = timeout_seconds.unwrap_or(60);
                out.push_str(&format!(
                    "  wait_for_tcp \"$pid\" \"$name\" {} {} {}\n",
                    shell_quote(host),
                    port,
                    timeout
                ));
            }
            ReadinessSpec::Log {
                pattern,
                timeout_seconds,
            } => {
                let timeout = timeout_seconds.unwrap_or(60);
                out.push_str(&format!(
                    "  wait_for_log \"$pid\" \"$name\" \"$LOG_DIR/{}\" {} {}\n",
                    log_file_name_for_service(&service.name),
                    shell_quote(pattern),
                    timeout
                ));
            }
            ReadinessSpec::Http {
                url,
                status_code,
                timeout_seconds,
            } => {
                let timeout = timeout_seconds.unwrap_or(60);
                out.push_str(&format!(
                    "  wait_for_http \"$pid\" \"$name\" {} {} {}\n",
                    shell_quote(url),
                    status_code,
                    timeout
                ));
            }
        }
    } else {
        out.push_str("  :\n");
    }
    out.push_str("}\n");
}

fn render_dependency_waits(out: &mut String, service: &RuntimeService) {
    for dependency in &service.depends_on {
        match dependency.condition {
            DependencyCondition::ServiceStarted => out.push_str(&format!(
                "wait_for_service_started {} {}\n",
                shell_quote(&dependency.name),
                shell_quote(&service.name)
            )),
            DependencyCondition::ServiceHealthy => out.push_str(&format!(
                "wait_for_service_healthy {} {} wait_until_{}_ready\n",
                shell_quote(&dependency.name),
                shell_quote(&service.name),
                service_token(&dependency.name)
            )),
            DependencyCondition::ServiceCompletedSuccessfully => out.push_str(&format!(
                "wait_for_service_completed_successfully {} {}\n",
                shell_quote(&dependency.name),
                shell_quote(&service.name)
            )),
        }
    }
}

fn hook_file_name(service_id: &str, suffix: &str) -> String {
    format!("{service_id}.{suffix}.sh")
}

fn push_hook_file(out: &mut String, target_expr: &str, delimiter_base: &str, body: &str) {
    let delimiter = heredoc_delimiter(delimiter_base, body);
    out.push_str(&format!("  cat > {target_expr} <<'{delimiter}'\n"));
    out.push_str(body);
    if !body.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&delimiter);
    out.push('\n');
}

fn heredoc_delimiter(base: &str, body: &str) -> String {
    let mut delimiter = base.to_string();
    let mut counter = 0;
    while body.lines().any(|line| line == delimiter) {
        counter += 1;
        delimiter = format!("{base}_{counter}");
    }
    delimiter
}

fn host_hook_file_body(script: &str) -> String {
    let mut body = String::from("#!/bin/bash\nset -euo pipefail\n");
    body.push_str(script);
    if !script.ends_with('\n') {
        body.push('\n');
    }
    body
}

fn container_hook_file_body(script: &str) -> String {
    let mut body = String::from("#!/bin/sh\nset -eu\n");
    body.push_str(script);
    if !script.ends_with('\n') {
        body.push('\n');
    }
    body
}

fn container_wrapper_body(
    service_name: &str,
    container_log_path: &str,
    prologue_script: Option<&str>,
    epilogue_script: Option<&str>,
) -> String {
    let prologue_script = prologue_script.unwrap_or("");
    let epilogue_script = epilogue_script.unwrap_or("");
    format!(
        r#"#!/bin/sh
set -u
export HPC_COMPOSE_SERVICE_NAME={}
export HPC_COMPOSE_SERVICE_LOG={}
if [ "$#" -eq 0 ]; then
  echo "container hook wrapper for service '$HPC_COMPOSE_SERVICE_NAME' has no command to run" >&2
  exit 127
fi
if [ -n {} ]; then
  export HPC_COMPOSE_HOOK_PHASE=prologue
  export HPC_COMPOSE_SERVICE_EXIT_CODE=
  hook_status=0
  /bin/sh {} || hook_status=$?
  if [ "$hook_status" -ne 0 ]; then
    exit "$hook_status"
  fi
fi
service_status=0
"$@" || service_status=$?
if [ -n {} ]; then
  export HPC_COMPOSE_HOOK_PHASE=epilogue
  export HPC_COMPOSE_SERVICE_EXIT_CODE="$service_status"
  hook_status=0
  /bin/sh {} || hook_status=$?
  if [ "$hook_status" -ne 0 ] && [ "$service_status" -eq 0 ]; then
    exit "$hook_status"
  fi
fi
exit "$service_status"
"#,
        shell_quote(service_name),
        shell_quote(container_log_path),
        shell_quote(prologue_script),
        shell_quote(prologue_script),
        shell_quote(epilogue_script),
        shell_quote(epilogue_script),
    )
}

fn distributed_env_wrapper_body() -> String {
    r#"#!/bin/sh
set -u
if [ "$#" -eq 0 ]; then
  echo "distributed environment wrapper has no command to run" >&2
  exit 127
fi
hpc_compose_current_node() {
  if [ -n "${SLURMD_NODENAME:-}" ]; then
    printf '%s' "$SLURMD_NODENAME"
  elif [ -n "${HOSTNAME:-}" ]; then
    printf '%s' "$HOSTNAME"
  else
    hostname
  fi
}
hpc_compose_dist_node_rank() {
  current_node="$(hpc_compose_current_node)"
  rank=0
  for node in ${HPC_COMPOSE_SERVICE_NODELIST:-}; do
    if [ "$node" = "$current_node" ]; then
      printf '%s' "$rank"
      return 0
    fi
    rank=$((rank + 1))
  done
  printf '%s' "${SLURM_NODEID:-0}"
}
export HPC_COMPOSE_DIST_NODE_RANK="${HPC_COMPOSE_DIST_NODE_RANK:-$(hpc_compose_dist_node_rank)}"
export HPC_COMPOSE_DIST_LOCAL_RANK="${HPC_COMPOSE_DIST_LOCAL_RANK:-${SLURM_LOCALID:-0}}"
export HPC_COMPOSE_DIST_GLOBAL_RANK="${HPC_COMPOSE_DIST_GLOBAL_RANK:-${SLURM_PROCID:-0}}"
exec "$@"
"#
    .to_string()
}

fn distributed_rank_task_prolog_body() -> String {
    r#"#!/bin/sh
set -u
hpc_compose_current_node() {
  if [ -n "${SLURMD_NODENAME:-}" ]; then
    printf '%s' "$SLURMD_NODENAME"
  elif [ -n "${HOSTNAME:-}" ]; then
    printf '%s' "$HOSTNAME"
  else
    hostname
  fi
}
hpc_compose_dist_node_rank() {
  current_node="$(hpc_compose_current_node)"
  rank=0
  for node in ${HPC_COMPOSE_SERVICE_NODELIST:-}; do
    if [ "$node" = "$current_node" ]; then
      printf '%s' "$rank"
      return 0
    fi
    rank=$((rank + 1))
  done
  printf '%s' "${SLURM_NODEID:-0}"
}
printf 'export HPC_COMPOSE_DIST_NODE_RANK=%s\n' "${HPC_COMPOSE_DIST_NODE_RANK:-$(hpc_compose_dist_node_rank)}"
printf 'export HPC_COMPOSE_DIST_LOCAL_RANK=%s\n' "${HPC_COMPOSE_DIST_LOCAL_RANK:-${SLURM_LOCALID:-0}}"
printf 'export HPC_COMPOSE_DIST_GLOBAL_RANK=%s\n' "${HPC_COMPOSE_DIST_GLOBAL_RANK:-${SLURM_PROCID:-0}}"
"#
    .to_string()
}

fn failure_policy_mode_label(mode: ServiceFailureMode) -> &'static str {
    match mode {
        ServiceFailureMode::FailJob => "fail_job",
        ServiceFailureMode::Ignore => "ignore",
        ServiceFailureMode::RestartOnFailure => "restart_on_failure",
    }
}

fn hook_event_label(event: ServiceHookEvent) -> &'static str {
    match event {
        ServiceHookEvent::Restart => "restart",
        ServiceHookEvent::WindowExhausted => "window_exhausted",
    }
}

fn placement_mode_label(mode: ServicePlacementMode) -> &'static str {
    match mode {
        ServicePlacementMode::PrimaryNode => "primary_node",
        ServicePlacementMode::Partitioned => "partitioned",
        ServicePlacementMode::Distributed => "distributed",
    }
}
fn mpi_hostfile_slots(service: &RuntimeService) -> Option<u32> {
    if let Some(ntasks_per_node) = service.placement.ntasks_per_node {
        return Some(ntasks_per_node);
    }
    if service.placement.nodes == 1 {
        return service.placement.ntasks;
    }
    None
}

#[cfg(test)]
mod tests;
