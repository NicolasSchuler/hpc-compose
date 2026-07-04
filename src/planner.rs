//! Normalization from parsed spec into an execution plan.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::context::ResourceProfile;
use crate::domain::{MountParts, resolve_node_index_expr, split_mount_parts};
use crate::readiness_util::readiness_uses_implicit_localhost;
use crate::spec::{
    CommandSpec, ComposeSpec, DependencyCondition, PrepareSpec, ReadinessSpec, RuntimeBackend,
    RuntimeConfig, ServiceAssertSpec, ServiceDependency, ServiceFailureMode, ServiceFailurePolicy,
    ServiceSlurmConfig, SlurmConfig,
};

use crate::tracked_paths::JOB_CONTAINER_DIR;

/// Mount destinations that the runtime reserves for itself; user-declared
/// volume mounts may not target any of these paths.
const RESERVED_RUNTIME_MOUNT_DESTINATIONS: &[&str] = &[JOB_CONTAINER_DIR];

/// A normalized application plan derived from a compose file.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct Plan {
    pub name: String,
    pub project_dir: PathBuf,
    pub spec_path: PathBuf,
    pub runtime: RuntimeConfig,
    pub cache_dir: PathBuf,
    pub slurm: SlurmConfig,
    pub ordered_services: Vec<PlannedService>,
}

/// Optional inputs used while building a normalized plan.
#[derive(Debug, Clone, Default)]
pub struct PlanOptions {
    /// Cache directory to use when the compose spec omits `x-slurm.cache_dir`.
    pub cache_dir_default: Option<PathBuf>,
    /// Settings-defined Slurm resource profiles addressable through `x-slurm.resources`.
    pub resource_profiles: BTreeMap<String, ResourceProfile>,
    /// Project directory to use when the spec path is synthetic.
    pub project_dir_override: Option<PathBuf>,
    /// Allow planning against a synthetic spec path that does not exist on disk.
    pub allow_missing_spec_path: bool,
}

/// A normalized service entry inside a [`Plan`].
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct PlannedService {
    pub name: String,
    pub image: ImageSource,
    pub execution: ExecutionSpec,
    pub environment: Vec<(String, String)>,
    pub volumes: Vec<String>,
    pub working_dir: Option<String>,
    pub depends_on: Vec<ServiceDependency>,
    pub readiness: Option<ReadinessSpec>,
    pub assertions: Option<ServiceAssertSpec>,
    pub failure_policy: ServiceFailurePolicy,
    pub placement: ServicePlacement,
    pub slurm: ServiceSlurmConfig,
    pub prepare: Option<PreparedImageSpec>,
}

/// Service placement mode inside one Slurm allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServicePlacementMode {
    /// The service is pinned to the allocation's primary node.
    PrimaryNode,
    /// The service is pinned to an explicit subset of allocation nodes.
    Partitioned,
    /// The service spans the full allocation.
    Distributed,
}

/// The effective `srun` placement geometry for one service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServicePlacement {
    /// Placement mode used for this service.
    pub mode: ServicePlacementMode,
    /// Number of nodes requested for the step.
    pub nodes: u32,
    /// Explicit task count for the step when one is requested.
    pub ntasks: Option<u32>,
    /// Explicit tasks-per-node count for the step when one is requested.
    pub ntasks_per_node: Option<u32>,
    /// Whether this step should be pinned to the primary node.
    pub pin_to_primary_node: bool,
    /// Zero-based allocation node indices selected for this step when known.
    pub node_indices: Option<Vec<u32>>,
    /// Zero-based allocation node indices excluded from this step.
    pub exclude_indices: Vec<u32>,
    /// Whether this step is allowed to overlap another service placement.
    pub allow_overlap: bool,
}

impl Default for ServicePlacement {
    fn default() -> Self {
        Self {
            mode: ServicePlacementMode::PrimaryNode,
            nodes: 1,
            ntasks: Some(1),
            ntasks_per_node: None,
            pin_to_primary_node: false,
            node_indices: None,
            exclude_indices: Vec::new(),
            allow_overlap: false,
        }
    }
}

/// Where a service image comes from after normalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ImageSource {
    /// A local `.sqsh` or `.squashfs` file used directly at runtime.
    LocalSqsh(PathBuf),
    /// A local Apptainer/Singularity `.sif` file used directly at runtime.
    LocalSif(PathBuf),
    /// A remote image reference imported through Enroot.
    Remote(String),
    /// No container image because the service runs on the host runtime.
    Host,
}

/// The final command form passed to the runtime container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ExecutionSpec {
    /// Use the image's default entrypoint and command.
    ImageDefault,
    /// Run a shell-form command.
    Shell(String),
    /// Run an exec-form argv vector.
    Exec(Vec<String>),
}

/// A normalized image prepare block attached to a service.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct PreparedImageSpec {
    pub commands: Vec<String>,
    pub mounts: Vec<String>,
    pub env: Vec<(String, String)>,
    pub root: bool,
    pub force_rebuild: bool,
}

/// Builds a normalized plan from a validated compose spec.
///
/// # Errors
///
/// Returns an error when the compose file path cannot be normalized, the spec
/// violates semantic validation rules, or service dependencies and placement
/// cannot be resolved into one supported Slurm allocation plan.
pub fn build_plan(spec_path: &Path, spec: ComposeSpec) -> Result<Plan> {
    build_plan_with_options(spec_path, spec, PlanOptions::default())
}

/// Builds a normalized plan with explicit planning options.
///
/// # Errors
///
/// Returns an error when the compose file path cannot be normalized, the spec
/// violates semantic validation rules, or service dependencies and placement
/// cannot be resolved into one supported Slurm allocation plan.
pub fn build_plan_with_options(
    spec_path: &Path,
    mut spec: ComposeSpec,
    options: PlanOptions,
) -> Result<Plan> {
    apply_resource_profile_defaults(&mut spec.slurm, &options.resource_profiles)?;
    // Enforcement chokepoint: run the full spec validation + normalization here,
    // not just the slurm sub-check. `ComposeSpec` has all-public fields and a
    // derived `Deserialize`, so a serde-constructed or hand-built spec can reach
    // the planner without ever going through `load`. Validating here guarantees
    // every invariant the renderer assumes holds regardless of how the spec was
    // built. `validate` is idempotent, so re-running it after `load` already
    // validated (the CLI path) is a harmless no-op.
    spec.validate()?;
    let spec_path = if options.allow_missing_spec_path {
        crate::path_util::absolute_path(
            spec_path,
            options
                .project_dir_override
                .as_deref()
                .unwrap_or_else(|| Path::new(".")),
        )
    } else {
        normalize_existing_path(spec_path)?
    };
    let project_dir = options
        .project_dir_override
        .clone()
        .or_else(|| spec_path.parent().map(Path::to_path_buf))
        .context("compose file must have a parent directory")?;

    let name = if let Some(job_name) = spec.slurm.job_name.clone() {
        job_name
    } else if let Some(spec_name) = spec.name.clone() {
        spec_name
    } else {
        "hpc-compose".to_string()
    };

    if spec.services.is_empty() {
        bail!("spec must define at least one service");
    }

    let cache_dir = resolve_cache_dir(
        &spec.slurm,
        &project_dir,
        options.cache_dir_default.as_deref(),
    )?;

    let mut temp = BTreeMap::new();
    for (name, service) in &spec.services {
        let depends_on = service.depends_on.entries()?;
        let environment = service.environment.to_pairs()?;
        let host_mpi_environment = service
            .slurm
            .mpi
            .as_ref()
            .and_then(|mpi| mpi.host_mpi.as_ref())
            .map(|host_mpi| host_mpi.env.to_pairs())
            .transpose()?
            .unwrap_or_default();
        let host_mpi_bind_paths = service
            .slurm
            .mpi
            .as_ref()
            .and_then(|mpi| mpi.host_mpi.as_ref())
            .map(|host_mpi| host_mpi.bind_paths.len())
            .unwrap_or_default();
        if spec.runtime.backend == RuntimeBackend::Host && !service.volumes.is_empty() {
            bail!(
                "service '{name}' uses volumes with runtime.backend=host; host runtime does not apply container bind mounts"
            );
        }
        if spec.runtime.backend == RuntimeBackend::Host && host_mpi_bind_paths > 0 {
            bail!(
                "service '{name}' uses x-slurm.mpi.host_mpi.bind_paths with runtime.backend=host; host runtime does not apply container bind mounts"
            );
        }
        let mut volumes = Vec::with_capacity(service.volumes.len());
        for mount in &service.volumes {
            let mount = normalize_mount(mount, &project_dir)?;
            ensure_runtime_mount_destination_allowed(name, &mount)?;
            volumes.push(mount);
        }
        if let Some(host_mpi) = service
            .slurm
            .mpi
            .as_ref()
            .and_then(|mpi| mpi.host_mpi.as_ref())
        {
            for mount in &host_mpi.bind_paths {
                let mount = normalize_mount(mount, &project_dir)?;
                ensure_runtime_mount_destination_allowed(name, &mount)?;
                volumes.push(mount);
            }
        }
        let mut environment = environment;
        environment.extend(host_mpi_environment);
        let working_dir = service.working_dir.clone();
        let execution = build_execution(
            service.entrypoint.as_ref(),
            service.command.as_ref(),
            working_dir.as_deref(),
            name,
        )?;
        if spec.runtime.backend == RuntimeBackend::Host
            && matches!(execution, ExecutionSpec::ImageDefault)
        {
            bail!(
                "service '{name}' uses runtime.backend=host without an explicit command or entrypoint"
            );
        }
        if spec.runtime.backend == RuntimeBackend::Host && service.slurm.has_container_hook() {
            bail!(
                "service '{name}' uses a container-context x-slurm prologue/epilogue hook with runtime.backend=host"
            );
        }
        if matches!(execution, ExecutionSpec::ImageDefault) && service.slurm.has_container_hook() {
            bail!(
                "service '{name}' uses a container-context x-slurm prologue/epilogue hook without an explicit command or entrypoint; define one so hpc-compose can wrap it"
            );
        }
        let image = normalize_image(
            service.image.as_deref(),
            spec.runtime.backend,
            &project_dir,
            name,
        )?;
        let prepare = normalize_prepare(
            service.runtime.prepare.clone(),
            service.enroot.prepare.clone(),
            spec.runtime.backend,
            &project_dir,
            name,
        )?;
        let failure_policy = service.slurm.normalized_failure_policy(name)?;

        temp.insert(
            name.clone(),
            PlannedService {
                name: name.clone(),
                image,
                execution,
                environment,
                volumes,
                working_dir,
                depends_on,
                readiness: service.readiness.clone(),
                assertions: service.assertions.clone(),
                failure_policy,
                placement: ServicePlacement {
                    mode: ServicePlacementMode::PrimaryNode,
                    nodes: 1,
                    ntasks: Some(1),
                    ntasks_per_node: None,
                    pin_to_primary_node: false,
                    node_indices: None,
                    exclude_indices: Vec::new(),
                    allow_overlap: false,
                },
                slurm: service.slurm.clone(),
                prepare,
            },
        );
    }

    assign_service_placements(&spec.slurm, &mut temp)?;

    validate_dependency_conditions(&temp)?;
    let ordered_names = topo_sort(&temp)?;
    let mut ordered_services = Vec::with_capacity(ordered_names.len());
    for name in ordered_names {
        ordered_services.push(temp.get(&name).cloned().expect("service exists"));
    }

    Ok(Plan {
        name,
        project_dir,
        spec_path,
        runtime: spec.runtime,
        cache_dir,
        slurm: spec.slurm,
        ordered_services,
    })
}

/// Applies resource-profile defaults to a Slurm config.
///
/// # Errors
///
/// Returns an error when the spec references a profile that settings did not
/// define.
pub fn apply_resource_profile_defaults(
    slurm: &mut SlurmConfig,
    profiles: &BTreeMap<String, ResourceProfile>,
) -> Result<()> {
    let Some(name) = slurm.resources.clone() else {
        return Ok(());
    };
    let profile = profiles.get(&name).with_context(|| {
        format!("x-slurm.resources references undefined resource profile '{name}'")
    })?;

    apply_string_default(&mut slurm.partition, &profile.partition);
    apply_string_default(&mut slurm.account, &profile.account);
    apply_string_default(&mut slurm.qos, &profile.qos);
    apply_string_default(&mut slurm.time, &profile.time);
    apply_copy_default(&mut slurm.nodes, profile.nodes);
    apply_copy_default(&mut slurm.ntasks, profile.ntasks);
    apply_copy_default(&mut slurm.ntasks_per_node, profile.ntasks_per_node);
    apply_copy_default(&mut slurm.cpus_per_task, profile.cpus_per_task);
    apply_string_default(&mut slurm.mem, &profile.mem);
    apply_string_default(&mut slurm.gres, &profile.gres);
    apply_copy_default(&mut slurm.gpus, profile.gpus);
    apply_copy_default(&mut slurm.gpus_per_node, profile.gpus_per_node);
    apply_copy_default(&mut slurm.gpus_per_task, profile.gpus_per_task);
    apply_copy_default(&mut slurm.cpus_per_gpu, profile.cpus_per_gpu);
    apply_string_default(&mut slurm.mem_per_gpu, &profile.mem_per_gpu);
    apply_string_default(&mut slurm.gpu_bind, &profile.gpu_bind);
    apply_string_default(&mut slurm.cpu_bind, &profile.cpu_bind);
    apply_string_default(&mut slurm.mem_bind, &profile.mem_bind);
    apply_string_default(&mut slurm.distribution, &profile.distribution);
    apply_string_default(&mut slurm.hint, &profile.hint);
    apply_string_default(&mut slurm.constraint, &profile.constraint);
    Ok(())
}

fn apply_string_default(target: &mut Option<String>, default: &Option<String>) {
    if target.is_none() {
        *target = default.clone();
    }
}

fn apply_copy_default<T: Copy>(target: &mut Option<T>, default: Option<T>) {
    if target.is_none() {
        *target = default;
    }
}

fn assign_service_placements(
    slurm: &SlurmConfig,
    services: &mut BTreeMap<String, PlannedService>,
) -> Result<()> {
    let allocation_nodes = slurm.allocation_nodes();
    let mut resolved = BTreeMap::new();
    let mut marks = HashMap::new();
    let names = services.keys().cloned().collect::<Vec<_>>();

    for name in &names {
        let placement = resolve_service_placement_by_name(
            name,
            services,
            slurm,
            allocation_nodes,
            &mut resolved,
            &mut marks,
        )?;
        resolved.insert(name.clone(), placement);
    }

    for (name, placement) in resolved {
        let service = services
            .get_mut(&name)
            .expect("resolved placement belongs to existing service");
        service.placement = placement;
    }

    validate_service_placement_readiness(allocation_nodes, services)?;
    validate_service_placement_overlaps(allocation_nodes, services)?;
    validate_mpi_expected_ranks(services)?;

    Ok(())
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum PlacementMark {
    Visiting,
    Visited,
}

fn resolve_service_placement_by_name(
    name: &str,
    services: &BTreeMap<String, PlannedService>,
    slurm: &SlurmConfig,
    allocation_nodes: u32,
    resolved: &mut BTreeMap<String, ServicePlacement>,
    marks: &mut HashMap<String, PlacementMark>,
) -> Result<ServicePlacement> {
    if let Some(placement) = resolved.get(name) {
        return Ok(placement.clone());
    }
    match marks.get(name).copied() {
        Some(PlacementMark::Visiting) => {
            bail!("x-slurm.placement.share_with cycle detected around service '{name}'");
        }
        Some(PlacementMark::Visited) => {
            return Ok(resolved
                .get(name)
                .expect("visited placement is resolved")
                .clone());
        }
        None => {}
    }

    let service = services
        .get(name)
        .with_context(|| format!("service '{name}' does not exist"))?;
    marks.insert(name.to_string(), PlacementMark::Visiting);
    let default_distributed_service = allocation_nodes > 1 && services.len() == 1;
    let placement = match &service.slurm.placement {
        Some(spec) => {
            if let Some(target) = spec.share_with.as_deref() {
                let shared = resolve_service_placement_by_name(
                    target,
                    services,
                    slurm,
                    allocation_nodes,
                    resolved,
                    marks,
                )
                .with_context(|| {
                    format!(
                        "service '{}' x-slurm.placement.share_with references '{target}'",
                        service.name
                    )
                })?;
                resolve_shared_service_placement(service, slurm, &shared)?
            } else {
                resolve_explicit_service_placement(service, slurm, allocation_nodes)?
            }
        }
        None => resolve_legacy_service_placement(
            service,
            slurm,
            allocation_nodes,
            default_distributed_service,
        )?,
    };
    marks.insert(name.to_string(), PlacementMark::Visited);
    resolved.insert(name.to_string(), placement.clone());
    Ok(placement)
}

fn resolve_shared_service_placement(
    service: &PlannedService,
    slurm: &SlurmConfig,
    shared: &ServicePlacement,
) -> Result<ServicePlacement> {
    ensure_service_nodes_match(service, shared.nodes)?;
    let (ntasks, ntasks_per_node) = resolve_step_tasks(service, slurm, shared.nodes);
    Ok(ServicePlacement {
        mode: shared.mode,
        nodes: shared.nodes,
        ntasks,
        ntasks_per_node,
        pin_to_primary_node: shared.pin_to_primary_node,
        node_indices: shared.node_indices.clone(),
        exclude_indices: shared.exclude_indices.clone(),
        allow_overlap: true,
    })
}

fn resolve_explicit_service_placement(
    service: &PlannedService,
    slurm: &SlurmConfig,
    allocation_nodes: u32,
) -> Result<ServicePlacement> {
    let spec = service
        .slurm
        .placement
        .as_ref()
        .expect("explicit placement exists");
    if let Some(nodes) = service.slurm.nodes
        && nodes > allocation_nodes
    {
        bail!(
            "service '{}' requests x-slurm.nodes={}, but the allocation only reserves {} node(s)",
            service.name,
            nodes,
            allocation_nodes
        );
    }

    let exclude_indices = spec
        .exclude
        .as_deref()
        .map(|expr| {
            parse_node_index_expr(
                expr,
                allocation_nodes,
                &format!("service '{}' x-slurm.placement.exclude", service.name),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let exclude_set = exclude_indices.iter().copied().collect::<BTreeSet<_>>();
    let mut node_indices = if let Some(expr) = spec.node_range.as_deref() {
        parse_node_index_expr(
            expr,
            allocation_nodes,
            &format!("service '{}' x-slurm.placement.node_range", service.name),
        )?
        .into_iter()
        .filter(|index| !exclude_set.contains(index))
        .collect::<Vec<_>>()
    } else if let Some(count) = spec.node_count {
        select_eligible_node_indices(
            allocation_nodes,
            &exclude_set,
            spec.start_index.unwrap_or(0),
            count,
            &service.name,
        )?
    } else if let Some(percent) = spec.node_percent {
        let eligible_count =
            allocation_nodes.saturating_sub(u32::try_from(exclude_set.len()).unwrap_or(u32::MAX));
        let count = ((u64::from(eligible_count) * u64::from(percent)).div_ceil(100)) as u32;
        select_eligible_node_indices(
            allocation_nodes,
            &exclude_set,
            spec.start_index.unwrap_or(0),
            count.max(1),
            &service.name,
        )?
    } else {
        unreachable!("share_with handled before explicit selector resolution");
    };
    node_indices.sort_unstable();
    node_indices.dedup();
    if node_indices.is_empty() {
        bail!(
            "service '{}' x-slurm.placement resolves to an empty node set",
            service.name
        );
    }

    let nodes = u32::try_from(node_indices.len()).context("node count overflow")?;
    ensure_service_nodes_match(service, nodes)?;
    let full_allocation = nodes == allocation_nodes
        && exclude_indices.is_empty()
        && node_indices.iter().copied().eq(0..allocation_nodes);
    let mode = if full_allocation {
        ServicePlacementMode::Distributed
    } else if node_indices == [0] {
        ServicePlacementMode::PrimaryNode
    } else {
        ServicePlacementMode::Partitioned
    };
    let (ntasks, ntasks_per_node) = resolve_step_tasks(service, slurm, nodes);

    Ok(ServicePlacement {
        mode,
        nodes,
        ntasks,
        ntasks_per_node,
        pin_to_primary_node: node_indices == [0],
        node_indices: (!full_allocation).then_some(node_indices),
        exclude_indices,
        allow_overlap: spec.allow_overlap,
    })
}

fn resolve_legacy_service_placement(
    service: &PlannedService,
    slurm: &SlurmConfig,
    allocation_nodes: u32,
    default_distributed_service: bool,
) -> Result<ServicePlacement> {
    if let Some(nodes) = service.slurm.nodes
        && nodes > allocation_nodes
    {
        bail!(
            "service '{}' requests x-slurm.nodes={}, but the allocation only reserves {} node(s)",
            service.name,
            nodes,
            allocation_nodes
        );
    }

    let distributed = if allocation_nodes == 1 {
        if let Some(nodes) = service.slurm.nodes
            && nodes != 1
        {
            bail!(
                "service '{}' requests x-slurm.nodes={}, but single-node allocations only support x-slurm.nodes=1",
                service.name,
                nodes
            );
        }
        false
    } else {
        match service.slurm.nodes {
            Some(1) => false,
            Some(nodes) if nodes == allocation_nodes => true,
            Some(nodes) => {
                bail!(
                    "service '{}' requests partial x-slurm.nodes={} without x-slurm.placement; use x-slurm.placement.node_count to choose allocation nodes explicitly",
                    service.name,
                    nodes,
                );
            }
            None => default_distributed_service,
        }
    };

    let (nodes, mode, pin_to_primary_node, node_indices, allow_overlap) = if distributed {
        (
            allocation_nodes,
            ServicePlacementMode::Distributed,
            false,
            None,
            false,
        )
    } else {
        (
            1,
            ServicePlacementMode::PrimaryNode,
            allocation_nodes > 1,
            None,
            true,
        )
    };
    let (ntasks, ntasks_per_node) = resolve_step_tasks(service, slurm, nodes);

    Ok(ServicePlacement {
        mode,
        nodes,
        ntasks,
        ntasks_per_node,
        pin_to_primary_node,
        node_indices,
        exclude_indices: Vec::new(),
        allow_overlap,
    })
}

fn resolve_step_tasks(
    service: &PlannedService,
    slurm: &SlurmConfig,
    nodes: u32,
) -> (Option<u32>, Option<u32>) {
    if nodes > 1 {
        (
            service.slurm.ntasks.or(slurm.ntasks),
            service.slurm.ntasks_per_node.or_else(|| {
                if service.slurm.ntasks.is_some() {
                    None
                } else {
                    slurm
                        .ntasks_per_node
                        .or_else(|| slurm.ntasks.is_none().then_some(1))
                }
            }),
        )
    } else {
        (
            service
                .slurm
                .ntasks
                .or_else(|| service.slurm.ntasks_per_node.is_none().then_some(1)),
            service.slurm.ntasks_per_node,
        )
    }
}

fn validate_mpi_expected_ranks(services: &BTreeMap<String, PlannedService>) -> Result<()> {
    for service in services.values() {
        let Some(mpi) = &service.slurm.mpi else {
            continue;
        };
        let Some(expected) = mpi.expected_ranks else {
            continue;
        };
        let resolved = resolved_rank_count(&service.placement);
        if resolved != expected {
            bail!(
                "service '{}' sets x-slurm.mpi.expected_ranks={}, but resolved Slurm task geometry launches {} rank(s)",
                service.name,
                expected,
                resolved
            );
        }
    }
    Ok(())
}

fn resolved_rank_count(placement: &ServicePlacement) -> u32 {
    placement
        .ntasks
        .or_else(|| {
            placement
                .ntasks_per_node
                .map(|per_node| per_node * placement.nodes)
        })
        .unwrap_or(1)
}

fn ensure_service_nodes_match(service: &PlannedService, resolved_nodes: u32) -> Result<()> {
    if let Some(nodes) = service.slurm.nodes
        && nodes != resolved_nodes
    {
        bail!(
            "service '{}' sets x-slurm.nodes={} but x-slurm.placement resolves to {} node(s)",
            service.name,
            nodes,
            resolved_nodes
        );
    }
    Ok(())
}

fn parse_node_index_expr(expr: &str, allocation_nodes: u32, label: &str) -> Result<Vec<u32>> {
    resolve_node_index_expr(expr, allocation_nodes, label)
}

fn select_eligible_node_indices(
    allocation_nodes: u32,
    exclude_set: &BTreeSet<u32>,
    start_index: u32,
    count: u32,
    service_name: &str,
) -> Result<Vec<u32>> {
    if start_index >= allocation_nodes {
        bail!(
            "service '{service_name}' x-slurm.placement.start_index={} is outside the {} node allocation",
            start_index,
            allocation_nodes
        );
    }
    let nodes = (start_index..allocation_nodes)
        .filter(|index| !exclude_set.contains(index))
        .take(count as usize)
        .collect::<Vec<_>>();
    if nodes.len() != count as usize {
        bail!(
            "service '{service_name}' x-slurm.placement requests {} node(s), but only {} eligible node(s) are available from start_index {}",
            count,
            nodes.len(),
            start_index
        );
    }
    Ok(nodes)
}

fn validate_service_placement_readiness(
    allocation_nodes: u32,
    services: &BTreeMap<String, PlannedService>,
) -> Result<()> {
    for service in services.values() {
        if !readiness_uses_implicit_localhost(service.readiness.as_ref()) {
            continue;
        }
        if service_allocation_indices(&service.placement, allocation_nodes) == BTreeSet::from([0]) {
            continue;
        }
        bail!(
            "service '{}' uses readiness that relies on localhost semantics, but its placement is not confined to the allocation primary node; use sleep/log readiness or explicit non-local hosts",
            service.name
        );
    }
    Ok(())
}

fn validate_service_placement_overlaps(
    allocation_nodes: u32,
    services: &BTreeMap<String, PlannedService>,
) -> Result<()> {
    let names = services.keys().collect::<Vec<_>>();
    for (left_pos, left_name) in names.iter().enumerate() {
        let left = &services[*left_name];
        let left_indices = service_allocation_indices(&left.placement, allocation_nodes);
        for right_name in names.iter().skip(left_pos + 1) {
            let right = &services[*right_name];
            if left.placement.allow_overlap || right.placement.allow_overlap {
                continue;
            }
            let right_indices = service_allocation_indices(&right.placement, allocation_nodes);
            let overlap = left_indices
                .intersection(&right_indices)
                .copied()
                .collect::<Vec<_>>();
            if !overlap.is_empty() {
                bail!(
                    "services '{}' and '{}' overlap on allocation node index/indices {}; set x-slurm.placement.allow_overlap=true or use share_with for intentional co-location",
                    left.name,
                    right.name,
                    overlap
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
        }
    }
    Ok(())
}

fn service_allocation_indices(
    placement: &ServicePlacement,
    allocation_nodes: u32,
) -> BTreeSet<u32> {
    if let Some(indices) = &placement.node_indices {
        return indices.iter().copied().collect();
    }
    match placement.mode {
        ServicePlacementMode::PrimaryNode => BTreeSet::from([0]),
        ServicePlacementMode::Partitioned => BTreeSet::new(),
        ServicePlacementMode::Distributed => (0..allocation_nodes).collect(),
    }
}

fn normalize_prepare(
    runtime_prepare: Option<PrepareSpec>,
    enroot_prepare: Option<PrepareSpec>,
    backend: RuntimeBackend,
    project_dir: &Path,
    service_name: &str,
) -> Result<Option<PreparedImageSpec>> {
    if backend == RuntimeBackend::Host && (runtime_prepare.is_some() || enroot_prepare.is_some()) {
        bail!("service '{service_name}' uses image prepare with runtime.backend=host");
    }
    let (prepare, label) = match (runtime_prepare, enroot_prepare) {
        (Some(prepare), None) => (prepare, "x-runtime.prepare"),
        (None, Some(prepare)) => (prepare, "x-enroot.prepare"),
        (None, None) => {
            return Ok(None);
        }
        (Some(_), Some(_)) => unreachable!("validated earlier"),
    };
    if backend != RuntimeBackend::Pyxis && label == "x-enroot.prepare" {
        bail!(
            "service '{service_name}' uses x-enroot.prepare with runtime.backend={}; use x-runtime.prepare",
            backend.as_str()
        );
    }
    let prepare = build_prepare_plan(prepare, project_dir, service_name, label)?;
    Ok(Some(prepare))
}

fn build_prepare_plan(
    prepare: PrepareSpec,
    project_dir: &Path,
    service_name: &str,
    label: &str,
) -> Result<PreparedImageSpec> {
    if prepare.commands.is_empty() {
        bail!("service '{service_name}' uses {label} but does not define any prepare.commands");
    }

    let mut mounts = Vec::with_capacity(prepare.mounts.len());
    for mount in &prepare.mounts {
        mounts.push(normalize_mount(mount, project_dir)?);
    }

    Ok(PreparedImageSpec {
        commands: prepare.commands,
        mounts: mounts.clone(),
        env: prepare.env.to_pairs()?,
        root: prepare.root,
        force_rebuild: !mounts.is_empty(),
    })
}

fn normalize_image(
    image: Option<&str>,
    backend: RuntimeBackend,
    project_dir: &Path,
    service_name: &str,
) -> Result<ImageSource> {
    if backend == RuntimeBackend::Host {
        return Ok(ImageSource::Host);
    }

    let Some(image) = image else {
        bail!("service '{service_name}' must define image unless runtime.backend=host");
    };
    if image.contains("://") {
        if image.starts_with("docker://")
            || image.starts_with("dockerd://")
            || image.starts_with("podman://")
        {
            return Ok(ImageSource::Remote(image.to_string()));
        }
        bail!(
            "unsupported image scheme in '{image}'; use docker://, dockerd://, podman://, a local .sqsh path, or a local .sif path"
        );
    }

    if backend.uses_pyxis() && looks_like_local_sif(image) {
        bail!(
            "service '{service_name}' uses local SIF image '{image}', but runtime.backend=pyxis expects a remote image or local .sqsh/.squashfs"
        );
    }
    if backend.uses_sif() && looks_like_local_sqsh(image) {
        bail!(
            "service '{service_name}' uses local Enroot image '{image}', but runtime.backend={} expects a remote image or local .sif",
            backend.as_str()
        );
    }
    if looks_like_local_sqsh(image) {
        return Ok(ImageSource::LocalSqsh(resolve_path(image, project_dir)?));
    }
    if looks_like_local_sif(image) {
        return Ok(ImageSource::LocalSif(resolve_path(image, project_dir)?));
    }

    if looks_like_explicit_local_path(image) {
        bail!(
            "local image path '{image}' must point to a .sqsh/.squashfs or .sif file; Dockerfiles and build contexts are not supported in v1"
        );
    }

    Ok(ImageSource::Remote(format!("docker://{image}")))
}

fn looks_like_local_sif(value: &str) -> bool {
    value.ends_with(".sif") || (looks_like_explicit_local_path(value) && value.contains(".sif"))
}

fn normalize_mount(mount: &str, project_dir: &Path) -> Result<String> {
    let parsed = ParsedMount::parse(mount)?;
    let host_path = resolve_path(parsed.host, project_dir)?;
    Ok(match parsed.mode {
        Some(mode) => format!("{}:{}:{mode}", host_path.display(), parsed.container),
        None => format!("{}:{}", host_path.display(), parsed.container),
    })
}

fn ensure_runtime_mount_destination_allowed(service_name: &str, mount: &str) -> Result<()> {
    let parsed = ParsedMount::parse(mount)?;
    if RESERVED_RUNTIME_MOUNT_DESTINATIONS.contains(&parsed.container) {
        bail!(
            "service '{service_name}' uses reserved runtime mount destination '{}'; that path is provided automatically for per-job shared state",
            parsed.container
        );
    }
    Ok(())
}

struct ParsedMount<'a> {
    host: &'a str,
    container: &'a str,
    mode: Option<&'a str>,
}

impl<'a> ParsedMount<'a> {
    fn parse(mount: &'a str) -> Result<Self> {
        let parsed = match split_mount_parts(mount) {
            MountParts::HostContainer {
                host,
                container,
                mode,
            } => Self {
                host,
                container,
                mode,
            },
            MountParts::UnsupportedMode(mode) => {
                bail!("mount '{mount}' uses unsupported mode '{mode}'; use ro or rw")
            }
            MountParts::InvalidShape => {
                bail!("mount '{mount}' must use host_path:container_path[:ro|rw] syntax")
            }
        };
        if parsed.host.trim().is_empty() || parsed.container.trim().is_empty() {
            bail!("mount '{mount}' must use non-empty host and container paths");
        }
        if !parsed.container.starts_with('/') {
            bail!("mount '{mount}' container path must be absolute");
        }
        Ok(parsed)
    }
}

fn resolve_cache_dir(
    slurm: &SlurmConfig,
    project_dir: &Path,
    default_cache_dir: Option<&Path>,
) -> Result<PathBuf> {
    let raw = match slurm.cache_dir.clone() {
        Some(cache_dir) => cache_dir,
        None => {
            if let Some(cache_dir) = default_cache_dir {
                return Ok(crate::path_util::normalize_path(cache_dir.to_path_buf()));
            }
            let home = match env::var("HOME") {
                Ok(home) => home,
                Err(_) => "~".to_string(),
            };
            format!("{home}/.cache/hpc-compose")
        }
    };
    resolve_path(&raw, project_dir)
}

/// Returns a user-facing issue for cache paths that violate cluster policy.
#[must_use]
pub fn cache_path_policy_issue(path: &Path) -> Option<String> {
    if crate::path_util::is_node_local_path(&path.to_string_lossy()) {
        return Some(format!(
            "x-slurm.cache_dir resolves to '{}', which is typically node-local and not shared; choose a shared filesystem path instead",
            path.display()
        ));
    }
    None
}

/// Returns a user-facing issue when a resolved `x-slurm.runtime_root` override
/// points at a node-local path, which would hide per-job logs and state from
/// compute nodes. Only explicit overrides are policed; the default
/// `<submit_dir>/.hpc-compose` layout mirrors the submit directory and is
/// governed by the submission environment.
#[must_use]
pub fn runtime_root_policy_issue(path: &Path) -> Option<String> {
    if crate::path_util::is_node_local_path(&path.to_string_lossy()) {
        return Some(format!(
            "x-slurm.runtime_root resolves to '{}', which is typically node-local and not shared; choose a shared filesystem path so per-job logs and state stay visible from compute nodes",
            path.display()
        ));
    }
    None
}

/// Extracts the registry hostname used by a remote image reference.
#[must_use]
pub fn registry_host_for_remote(remote: &str) -> String {
    let without_scheme = remote.split("://").nth(1).unwrap_or(remote);
    if let Some((host, _)) = without_scheme.split_once('#') {
        return host.to_string();
    }

    let has_path_component = without_scheme.contains('/');
    if !has_path_component {
        return "registry-1.docker.io".to_string();
    }

    let first = without_scheme.split('/').next().unwrap_or(without_scheme);
    if first == "localhost" || first.contains('.') || (first.contains(':') && has_path_component) {
        first.to_string()
    } else {
        "registry-1.docker.io".to_string()
    }
}

fn looks_like_local_sqsh(value: &str) -> bool {
    value.ends_with(".sqsh")
        || value.ends_with(".squashfs")
        || (looks_like_explicit_local_path(value)
            && (value.contains(".sqsh") || value.contains(".squashfs")))
}

fn looks_like_explicit_local_path(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with("~/")
}

fn resolve_path(value: &str, project_dir: &Path) -> Result<PathBuf> {
    let expanded = expand_home(value);
    let raw = PathBuf::from(expanded);
    let path = if raw.is_absolute() {
        raw
    } else {
        project_dir.join(raw)
    };
    Ok(crate::path_util::normalize_path(path))
}

fn expand_home(value: &str) -> String {
    if value == "~" {
        return match env::var("HOME") {
            Ok(home) => home,
            Err(_) => "~".to_string(),
        };
    }
    if let Some(rest) = value.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    value.to_string()
}

fn normalize_existing_path(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .context(format!("failed to canonicalize {}", path.display()))
}

fn build_execution(
    entrypoint: Option<&CommandSpec>,
    command: Option<&CommandSpec>,
    working_dir: Option<&str>,
    service_name: &str,
) -> Result<ExecutionSpec> {
    let execution = match (entrypoint, command) {
        (None, None) => ExecutionSpec::ImageDefault,
        (None, Some(CommandSpec::String(cmd))) => ExecutionSpec::Shell(cmd.clone()),
        (None, Some(CommandSpec::Vec(cmd))) => ExecutionSpec::Exec(cmd.clone()),
        (Some(CommandSpec::String(entry)), None) => ExecutionSpec::Shell(entry.clone()),
        (Some(CommandSpec::Vec(entry)), None) => ExecutionSpec::Exec(entry.clone()),
        (Some(CommandSpec::String(entry)), Some(CommandSpec::String(cmd))) => {
            ExecutionSpec::Shell(format!("{entry} {cmd}"))
        }
        (Some(CommandSpec::Vec(entry)), Some(CommandSpec::Vec(cmd))) => {
            let mut argv = entry.clone();
            argv.extend(cmd.clone());
            ExecutionSpec::Exec(argv)
        }
        (Some(_), Some(_)) => {
            bail!(
                "service '{service_name}' mixes string and array forms for entrypoint/command; use both strings or both arrays in v1"
            );
        }
    };

    if matches!(execution, ExecutionSpec::ImageDefault) && working_dir.is_some() {
        bail!(
            "service '{service_name}' sets working_dir without an explicit command or entrypoint; define one in v1"
        );
    }

    Ok(execution)
}

fn topo_sort(services: &BTreeMap<String, PlannedService>) -> Result<Vec<String>> {
    #[derive(Copy, Clone, Eq, PartialEq)]
    enum Mark {
        Temporary,
        Permanent,
    }

    fn visit(
        name: &str,
        services: &BTreeMap<String, PlannedService>,
        marks: &mut HashMap<String, Mark>,
        ordered: &mut Vec<String>,
    ) -> Result<()> {
        if let Some(Mark::Permanent) = marks.get(name) {
            return Ok(());
        }
        if let Some(Mark::Temporary) = marks.get(name) {
            bail!("dependency cycle detected around service '{name}'");
        }
        let Some(service) = services.get(name) else {
            bail!("service '{name}' references an unknown dependency");
        };
        marks.insert(name.to_string(), Mark::Temporary);
        for dep in &service.depends_on {
            if !services.contains_key(&dep.name) {
                bail!(
                    "service '{name}' depends on undefined service '{}'",
                    dep.name
                );
            }
            visit(&dep.name, services, marks, ordered)?;
        }
        marks.insert(name.to_string(), Mark::Permanent);
        ordered.push(name.to_string());
        Ok(())
    }

    let mut marks = HashMap::new();
    let mut ordered = Vec::with_capacity(services.len());
    for name in services.keys() {
        visit(name, services, &mut marks, &mut ordered)?;
    }
    Ok(ordered)
}

fn validate_dependency_conditions(services: &BTreeMap<String, PlannedService>) -> Result<()> {
    for (service_name, service) in services {
        for dep in &service.depends_on {
            let Some(dependency) = services.get(&dep.name) else {
                bail!(
                    "service '{service_name}' depends on undefined service '{}'",
                    dep.name
                );
            };
            if dep.condition == DependencyCondition::ServiceHealthy
                && dependency.readiness.is_none()
            {
                bail!(
                    "service '{service_name}' depends on '{}' with condition 'service_healthy', but '{}' does not define readiness",
                    dep.name,
                    dep.name
                );
            }
            if dependency.failure_policy.mode == ServiceFailureMode::Ignore {
                bail!(
                    "service '{service_name}' depends on '{}', but '{}' uses x-slurm.failure_policy.mode=ignore and cannot be depended on",
                    dep.name,
                    dep.name
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
