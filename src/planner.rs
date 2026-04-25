//! Normalization from parsed spec into an execution plan.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::{MountParts, resolve_node_index_expr, split_mount_parts};
use crate::readiness_util::readiness_uses_implicit_localhost;
use crate::spec::{
    CommandSpec, ComposeSpec, DependencyCondition, PrepareSpec, ReadinessSpec, RuntimeBackend,
    RuntimeConfig, ServiceDependency, ServiceFailureMode, ServiceFailurePolicy, ServiceSlurmConfig,
    SlurmConfig,
};

const RESERVED_RUNTIME_MOUNT_DESTINATIONS: &[&str] = &["/hpc-compose/job"];

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
    pub failure_policy: ServiceFailurePolicy,
    pub placement: ServicePlacement,
    pub slurm: ServiceSlurmConfig,
    pub prepare: Option<PreparedImageSpec>,
}

/// Service placement mode inside one Slurm allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    spec.slurm.validate()?;
    let spec_path = normalize_existing_path(spec_path)?;
    let project_dir = spec_path
        .parent()
        .context("compose file must have a parent directory")?
        .to_path_buf();

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

    let cache_dir = resolve_cache_dir(&spec.slurm, &project_dir)?;

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

fn resolve_cache_dir(slurm: &SlurmConfig, project_dir: &Path) -> Result<PathBuf> {
    let raw = match slurm.cache_dir.clone() {
        Some(cache_dir) => cache_dir,
        None => {
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
    let banned_prefixes = [
        Path::new("/tmp"),
        Path::new("/var/tmp"),
        Path::new("/private/tmp"),
        Path::new("/dev/shm"),
    ];
    if banned_prefixes
        .iter()
        .any(|prefix| path.starts_with(prefix))
    {
        return Some(format!(
            "x-slurm.cache_dir resolves to '{}', which is typically node-local and not shared; choose a shared filesystem path instead",
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
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use proptest::prelude::*;

    use super::*;
    use crate::spec::{
        ComposeSpec, DependsOnConditionSpec, DependsOnSpec, EnvironmentSpec, HostMpiConfig,
        MpiConfig, MpiLauncher, MpiType, ReadinessSpec, RuntimeConfig, ServiceDependency,
        ServiceEnrootConfig, ServiceFailureMode, ServiceFailurePolicy, ServiceFailurePolicySpec,
        ServiceHookContext, ServiceHookSpec, ServicePlacementSpec, ServiceRuntimeConfig,
        ServiceSlurmConfig, ServiceSpec,
    };

    fn service(image: &str) -> ServiceSpec {
        ServiceSpec {
            image: Some(image.to_string()),
            command: None,
            entrypoint: None,
            environment: EnvironmentSpec::None,
            volumes: Vec::new(),
            working_dir: None,
            depends_on: DependsOnSpec::None,
            readiness: None,
            healthcheck: None,
            software_env: crate::spec::SoftwareEnvConfig::default(),
            slurm: ServiceSlurmConfig::default(),
            runtime: ServiceRuntimeConfig::default(),
            enroot: ServiceEnrootConfig::default(),
        }
    }

    proptest! {
        #[test]
        fn property_node_ranges_resolve_to_sorted_in_range_sets(
            allocation_nodes in 1u32..32,
            raw_start in 0u32..64,
            raw_width in 0u32..64,
        ) {
            let start = raw_start % allocation_nodes;
            let width = raw_width % (allocation_nodes - start);
            let end = start + width;
            let expr = if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            };
            let indices = parse_node_index_expr(&expr, allocation_nodes, "placement").expect("indices");
            prop_assert!(!indices.is_empty());
            prop_assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
            prop_assert!(indices.iter().all(|index| *index < allocation_nodes));
            prop_assert_eq!(indices.first().copied(), Some(start));
            prop_assert_eq!(indices.last().copied(), Some(end));
        }

        #[test]
        fn property_topological_order_places_dependencies_first(service_count in 1usize..8) {
            let mut services = BTreeMap::new();
            for index in 0..service_count {
                let name = format!("s{index}");
                let mut spec = service("redis:7");
                if index > 0 {
                    spec.depends_on = DependsOnSpec::List(vec![format!("s{}", index - 1)]);
                }
                services.insert(name, spec);
            }
            let spec = ComposeSpec {
                runtime: RuntimeConfig::default(),
                name: Some("demo".into()),
                slurm: SlurmConfig::default(),
                software_env: crate::spec::SoftwareEnvConfig::default(),
                services,
            };
            let plan = build_plan(Path::new("."), spec).expect("plan");
            let positions = plan
                .ordered_services
                .iter()
                .enumerate()
                .map(|(index, service)| (service.name.clone(), index))
                .collect::<BTreeMap<_, _>>();
            for index in 1..service_count {
                let dependent = positions[&format!("s{index}")];
                let dependency = positions[&format!("s{}", index - 1)];
                prop_assert!(dependency < dependent);
            }
        }
    }

    #[test]
    fn bare_images_normalize_to_docker_uri() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([("redis".into(), service("redis:7"))]),
        };
        let plan = build_plan(Path::new("."), spec).expect("plan");
        assert_eq!(
            plan.ordered_services[0].image,
            ImageSource::Remote("docker://redis:7".into())
        );
    }

    #[test]
    fn host_backend_allows_command_without_image() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Host,
                ..RuntimeConfig::default()
            },
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    image: None,
                    command: Some(CommandSpec::String("module list".into())),
                    ..service("ignored:latest")
                },
            )]),
        };

        let plan = build_plan(Path::new("."), spec).expect("host plan");
        assert_eq!(plan.ordered_services[0].image, ImageSource::Host);
    }

    #[test]
    fn host_backend_rejects_service_volumes() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Host,
                ..RuntimeConfig::default()
            },
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    image: None,
                    command: Some(CommandSpec::String("/bin/true".into())),
                    volumes: vec!["./app:/workspace".into()],
                    ..service("ignored:latest")
                },
            )]),
        };

        let err = build_plan(Path::new("."), spec).expect_err("host volumes");
        assert!(err.to_string().contains("volumes"));
        assert!(err.to_string().contains("runtime.backend=host"));
    }

    #[test]
    fn host_backend_rejects_host_mpi_bind_paths() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Host,
                ..RuntimeConfig::default()
            },
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    image: None,
                    command: Some(CommandSpec::String("/bin/true".into())),
                    slurm: ServiceSlurmConfig {
                        mpi: Some(MpiConfig {
                            mpi_type: MpiType::new("pmix").expect("mpi type"),
                            profile: None,
                            implementation: None,
                            launcher: MpiLauncher::default(),
                            expected_ranks: None,
                            host_mpi: Some(HostMpiConfig {
                                bind_paths: vec!["/opt/mpi:/opt/mpi:ro".into()],
                                env: EnvironmentSpec::None,
                            }),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("ignored:latest")
                },
            )]),
        };

        let err = build_plan(Path::new("."), spec).expect_err("host mpi binds");
        assert!(err.to_string().contains("host_mpi.bind_paths"));
        assert!(err.to_string().contains("runtime.backend=host"));
    }

    #[test]
    fn non_pyxis_backends_accept_sif_and_reject_sqsh() {
        let project = Path::new("/tmp/project");
        let source = normalize_image(
            Some("./image.sif"),
            RuntimeBackend::Apptainer,
            project,
            "app",
        )
        .expect("sif image");
        assert!(matches!(source, ImageSource::LocalSif(path) if path.ends_with("image.sif")));

        let err = normalize_image(
            Some("./image.sqsh"),
            RuntimeBackend::Apptainer,
            project,
            "app",
        )
        .expect_err("sqsh rejected");
        assert!(
            err.to_string()
                .contains("expects a remote image or local .sif")
        );
    }

    #[test]
    fn read_only_volume_mode_is_preserved() {
        let mount = normalize_mount("./data:/data:ro", Path::new("/tmp/project")).expect("mount");
        assert_eq!(mount, "/tmp/project/data:/data:ro");
    }

    #[test]
    fn build_execution_rejects_ambiguous_mixed_forms() {
        let result = build_execution(
            Some(&CommandSpec::Vec(vec!["/bin/app".into()])),
            Some(&CommandSpec::String("serve".into())),
            None,
            "app",
        );
        assert!(result.is_err());
    }

    #[test]
    fn build_execution_allows_exec_form() {
        let execution = build_execution(
            Some(&CommandSpec::Vec(vec!["/bin/app".into()])),
            Some(&CommandSpec::Vec(vec![
                "serve".into(),
                "--port".into(),
                "8080".into(),
            ])),
            None,
            "app",
        )
        .expect("exec");

        assert_eq!(
            execution,
            ExecutionSpec::Exec(vec![
                "/bin/app".into(),
                "serve".into(),
                "--port".into(),
                "8080".into()
            ])
        );
    }

    #[test]
    fn working_dir_requires_explicit_command() {
        let result = build_execution(None, None, Some("/work"), "app");
        assert!(result.is_err());
    }

    #[test]
    fn container_hooks_require_explicit_command_or_entrypoint() {
        let mut app = service("redis:7");
        app.slurm.prologue = Some(ServiceHookSpec {
            context: ServiceHookContext::Container,
            script: "echo prepare".into(),
        });
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([("app".into(), app)]),
        };

        let err = build_plan(Path::new("."), spec).expect_err("image default cannot be wrapped");
        assert!(err.to_string().contains("container-context"));
        assert!(err.to_string().contains("command or entrypoint"));
    }

    #[test]
    fn prepare_mounts_force_rebuild() {
        let spec = PrepareSpec {
            commands: vec!["echo hello".into()],
            mounts: vec!["./data:/data".into()],
            env: EnvironmentSpec::None,
            root: true,
        };
        let prepare =
            build_prepare_plan(spec, Path::new("/tmp/project"), "svc", "x-runtime.prepare")
                .expect("prepare");
        assert!(prepare.force_rebuild);
        assert_eq!(prepare.mounts, vec!["/tmp/project/data:/data"]);
    }

    #[test]
    fn topo_sort_orders_dependencies() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "app".into(),
                    ServiceSpec {
                        depends_on: DependsOnSpec::List(vec!["redis".into()]),
                        ..service("redis:7")
                    },
                ),
                ("redis".into(), service("redis:7")),
            ]),
        };

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let plan = build_plan(&compose, spec).expect("plan");
        let names = plan
            .ordered_services
            .iter()
            .map(|svc| svc.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["redis", "app"]);
    }

    #[test]
    fn build_plan_rejects_reserved_runtime_mount_destination() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    volumes: vec!["./data:/hpc-compose/job".into()],
                    ..service("redis:7")
                },
            )]),
        };

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let err = build_plan(&compose, spec).expect_err("reserved mount");
        assert!(
            err.to_string()
                .contains("reserved runtime mount destination")
        );
        assert!(err.to_string().contains("/hpc-compose/job"));
    }

    #[test]
    fn cache_dir_policy_flags_tmp() {
        let issue = cache_path_policy_issue(Path::new("/tmp/hpc-compose")).expect("issue");
        assert!(issue.contains("not shared"));
    }

    #[test]
    fn registry_host_defaults_to_docker_hub_for_bare_refs() {
        assert_eq!(
            registry_host_for_remote("docker://redis:7"),
            "registry-1.docker.io"
        );
        assert_eq!(
            registry_host_for_remote("docker://python:3.11-slim"),
            "registry-1.docker.io"
        );
        assert_eq!(
            registry_host_for_remote("docker://library/redis:7"),
            "registry-1.docker.io"
        );
    }

    #[test]
    fn registry_host_extracts_explicit_registry_hosts() {
        assert_eq!(
            registry_host_for_remote("docker://ghcr.io/ggerganov/llama.cpp:server-cuda"),
            "ghcr.io"
        );
        assert_eq!(
            registry_host_for_remote("docker://registry.scc.kit.edu#proj/app:latest"),
            "registry.scc.kit.edu"
        );
        assert_eq!(
            registry_host_for_remote("docker://localhost:5000/app:latest"),
            "localhost:5000"
        );
    }

    #[test]
    fn readiness_is_cloned_into_plan() {
        let mut svc = service("redis:7");
        svc.readiness = Some(ReadinessSpec::Sleep { seconds: 5 });
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([("redis".into(), svc)]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let plan = build_plan(&compose, spec).expect("plan");
        assert_eq!(
            plan.ordered_services[0].readiness,
            Some(ReadinessSpec::Sleep { seconds: 5 })
        );
    }

    #[test]
    fn build_plan_rejects_empty_services_and_accepts_multi_node_single_service() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let err = build_plan(
            &compose,
            ComposeSpec {
                runtime: RuntimeConfig::default(),
                name: None,
                slurm: SlurmConfig::default(),
                software_env: crate::spec::SoftwareEnvConfig::default(),
                services: BTreeMap::new(),
            },
        )
        .expect_err("empty services");
        assert!(err.to_string().contains("at least one service"));

        let plan = build_plan(
            &compose,
            ComposeSpec {
                runtime: RuntimeConfig::default(),
                name: None,
                slurm: SlurmConfig {
                    nodes: Some(2),
                    ntasks_per_node: Some(4),
                    ..SlurmConfig::default()
                },
                software_env: crate::spec::SoftwareEnvConfig::default(),
                services: BTreeMap::from([("app".into(), service("redis:7"))]),
            },
        )
        .expect("multi node");
        assert_eq!(plan.slurm.allocation_nodes(), 2);
        assert_eq!(plan.ordered_services.len(), 1);
        assert_eq!(
            plan.ordered_services[0].placement.mode,
            ServicePlacementMode::Distributed
        );
        assert_eq!(plan.ordered_services[0].placement.nodes, 2);
        assert_eq!(plan.ordered_services[0].placement.ntasks_per_node, Some(4));
    }

    #[test]
    fn build_plan_rejects_overlapping_full_allocation_services() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "a".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            nodes: Some(2),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "b".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            nodes: Some(2),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };

        let err = build_plan(&compose, spec).expect_err("overlapping distributed services");
        assert!(err.to_string().contains("overlap"));
    }

    #[test]
    fn build_plan_accepts_disjoint_partitioned_services() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let plan = build_plan(
            &compose,
            ComposeSpec {
                runtime: RuntimeConfig::default(),
                name: Some("demo".into()),
                slurm: SlurmConfig {
                    nodes: Some(8),
                    ..SlurmConfig::default()
                },
                software_env: crate::spec::SoftwareEnvConfig::default(),
                services: BTreeMap::from([
                    (
                        "a".into(),
                        ServiceSpec {
                            slurm: ServiceSlurmConfig {
                                placement: Some(ServicePlacementSpec {
                                    node_range: Some("0-3".into()),
                                    ..ServicePlacementSpec::default()
                                }),
                                ..ServiceSlurmConfig::default()
                            },
                            ..service("redis:7")
                        },
                    ),
                    (
                        "b".into(),
                        ServiceSpec {
                            slurm: ServiceSlurmConfig {
                                placement: Some(ServicePlacementSpec {
                                    node_range: Some("4-7".into()),
                                    ..ServicePlacementSpec::default()
                                }),
                                ..ServiceSlurmConfig::default()
                            },
                            ..service("python:3.11-slim")
                        },
                    ),
                ]),
            },
        )
        .expect("partitioned plan");

        let a = plan
            .ordered_services
            .iter()
            .find(|service| service.name == "a")
            .expect("a");
        let b = plan
            .ordered_services
            .iter()
            .find(|service| service.name == "b")
            .expect("b");
        assert_eq!(a.placement.mode, ServicePlacementMode::Partitioned);
        assert_eq!(a.placement.nodes, 4);
        assert_eq!(a.placement.node_indices, Some(vec![0, 1, 2, 3]));
        assert_eq!(b.placement.node_indices, Some(vec![4, 5, 6, 7]));
    }

    #[test]
    fn build_plan_resolves_percent_with_ceil_minimum_one() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let plan = build_plan(
            &compose,
            ComposeSpec {
                runtime: RuntimeConfig::default(),
                name: Some("demo".into()),
                slurm: SlurmConfig {
                    nodes: Some(8),
                    ..SlurmConfig::default()
                },
                software_env: crate::spec::SoftwareEnvConfig::default(),
                services: BTreeMap::from([(
                    "workers".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_percent: Some(60),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                )]),
            },
        )
        .expect("percent plan");

        let workers = &plan.ordered_services[0];
        assert_eq!(workers.placement.nodes, 5);
        assert_eq!(workers.placement.node_indices, Some(vec![0, 1, 2, 3, 4]));
    }

    #[test]
    fn build_plan_rejects_accidental_overlap_unless_allowed_or_shared() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let overlapping = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "a".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-3".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "b".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("3-5".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };
        let err = build_plan(&compose, overlapping).expect_err("overlap");
        assert!(err.to_string().contains("overlap"));

        let allowed = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "a".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-3".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "b".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("3-5".into()),
                                allow_overlap: true,
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };
        build_plan(&compose, allowed).expect("overlap allowed");

        let shared = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(8),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "ps".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                share_with: Some("workers".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "workers".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("2-5".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };
        let plan = build_plan(&compose, shared).expect("share placement");
        let ps = plan
            .ordered_services
            .iter()
            .find(|service| service.name == "ps")
            .expect("ps");
        let workers = plan
            .ordered_services
            .iter()
            .find(|service| service.name == "workers")
            .expect("workers");
        assert_eq!(ps.placement.node_indices, workers.placement.node_indices);
    }

    #[test]
    fn build_plan_rejects_invalid_partitioned_placements() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let out_of_bounds = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-2".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, out_of_bounds).expect_err("out of bounds");
        assert!(err.to_string().contains("only has 2 node"));

        let empty_after_exclude = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "a".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("0-1".into()),
                            exclude: Some("0-1".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, empty_after_exclude).expect_err("empty placement");
        assert!(err.to_string().contains("empty node set"));

        let cycle = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "a".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                share_with: Some("b".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "b".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                share_with: Some("a".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };
        let err = build_plan(&compose, cycle).expect_err("share cycle");
        let err_text = format!("{err:#}");
        assert!(err_text.contains("cycle"), "{err_text}");
    }

    #[test]
    fn build_plan_rejects_distributed_readiness_with_localhost_semantics() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "trainer".into(),
                ServiceSpec {
                    readiness: Some(ReadinessSpec::Tcp {
                        host: None,
                        port: 29500,
                        timeout_seconds: None,
                    }),
                    ..service("python:3.11-slim")
                },
            )]),
        };

        let err = build_plan(&compose, spec).expect_err("distributed localhost readiness");
        assert!(err.to_string().contains("localhost semantics"));
    }

    #[test]
    fn build_plan_rejects_non_primary_placement_readiness_with_localhost_semantics() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let explicit_non_primary = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    readiness: Some(ReadinessSpec::Tcp {
                        host: None,
                        port: 6379,
                        timeout_seconds: None,
                    }),
                    slurm: ServiceSlurmConfig {
                        placement: Some(ServicePlacementSpec {
                            node_range: Some("1".into()),
                            ..ServicePlacementSpec::default()
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, explicit_non_primary)
            .expect_err("non-primary localhost readiness");
        assert!(err.to_string().contains("localhost semantics"));

        let shared_multi_node = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(4),
                ..SlurmConfig::default()
            },
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "ps".into(),
                    ServiceSpec {
                        readiness: Some(ReadinessSpec::Tcp {
                            host: None,
                            port: 6379,
                            timeout_seconds: None,
                        }),
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                share_with: Some("workers".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "workers".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            placement: Some(ServicePlacementSpec {
                                node_range: Some("0-1".into()),
                                ..ServicePlacementSpec::default()
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("python:3.11-slim")
                    },
                ),
            ]),
        };
        let err = build_plan(&compose, shared_multi_node).expect_err("shared localhost readiness");
        assert!(err.to_string().contains("localhost semantics"));
    }

    #[test]
    fn build_prepare_and_execution_cover_error_and_string_variants() {
        let err = build_prepare_plan(
            PrepareSpec {
                commands: Vec::new(),
                mounts: Vec::new(),
                env: EnvironmentSpec::None,
                root: true,
            },
            Path::new("/tmp/project"),
            "svc",
            "x-runtime.prepare",
        )
        .expect_err("missing commands");
        assert!(err.to_string().contains("prepare.commands"));

        let prepared = build_prepare_plan(
            PrepareSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: EnvironmentSpec::Map(BTreeMap::from([("A".into(), "B".into())])),
                root: false,
            },
            Path::new("/tmp/project"),
            "svc",
            "x-runtime.prepare",
        )
        .expect("prepared");
        assert!(!prepared.force_rebuild);
        assert_eq!(prepared.env, vec![("A".into(), "B".into())]);
        assert!(!prepared.root);

        assert_eq!(
            build_execution(
                None,
                Some(&CommandSpec::String("echo hi".into())),
                None,
                "svc"
            )
            .expect("shell"),
            ExecutionSpec::Shell("echo hi".into())
        );
        assert_eq!(
            build_execution(
                Some(&CommandSpec::String("python".into())),
                None,
                None,
                "svc"
            )
            .expect("entry shell"),
            ExecutionSpec::Shell("python".into())
        );
        assert_eq!(
            build_execution(
                Some(&CommandSpec::String("python".into())),
                Some(&CommandSpec::String("-m main".into())),
                None,
                "svc"
            )
            .expect("combined"),
            ExecutionSpec::Shell("python -m main".into())
        );
    }

    #[test]
    fn topo_sort_and_normalize_helpers_cover_error_branches() {
        let services = BTreeMap::from([(
            "app".into(),
            PlannedService {
                name: "app".into(),
                image: ImageSource::Remote("docker://redis:7".into()),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: vec![ServiceDependency {
                    name: "missing".into(),
                    condition: DependencyCondition::ServiceStarted,
                }],
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
            },
        )]);
        let err = topo_sort(&services).expect_err("missing dep");
        assert!(err.to_string().contains("undefined service"));

        let cycle = BTreeMap::from([
            (
                "a".into(),
                PlannedService {
                    name: "a".into(),
                    image: ImageSource::Remote("docker://redis:7".into()),
                    execution: ExecutionSpec::ImageDefault,
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: vec![ServiceDependency {
                        name: "b".into(),
                        condition: DependencyCondition::ServiceStarted,
                    }],
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                },
            ),
            (
                "b".into(),
                PlannedService {
                    name: "b".into(),
                    image: ImageSource::Remote("docker://redis:7".into()),
                    execution: ExecutionSpec::ImageDefault,
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: vec![ServiceDependency {
                        name: "a".into(),
                        condition: DependencyCondition::ServiceStarted,
                    }],
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                },
            ),
        ]);
        let err = topo_sort(&cycle).expect_err("cycle");
        assert!(err.to_string().contains("dependency cycle"));

        let err = normalize_mount("/host-only", Path::new("/tmp/project")).expect_err("mount");
        assert!(err.to_string().contains("host_path:container_path"));
    }

    #[test]
    fn image_and_path_normalizers_cover_remaining_variants() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_sqsh = tmpdir.path().join("image.sqsh");
        std::fs::write(&local_sqsh, "x").expect("sqsh");

        assert_eq!(
            normalize_image(
                Some(local_sqsh.to_str().expect("path")),
                RuntimeBackend::Pyxis,
                tmpdir.path(),
                "svc"
            )
            .expect("local"),
            ImageSource::LocalSqsh(local_sqsh.clone())
        );
        assert_eq!(
            normalize_image(
                Some("docker://redis:7"),
                RuntimeBackend::Pyxis,
                tmpdir.path(),
                "svc"
            )
            .expect("remote"),
            ImageSource::Remote("docker://redis:7".into())
        );
        assert_eq!(
            normalize_image(
                Some("docker://registry.example/app.sif"),
                RuntimeBackend::Pyxis,
                tmpdir.path(),
                "svc"
            )
            .expect("remote sif-like uri"),
            ImageSource::Remote("docker://registry.example/app.sif".into())
        );

        let err = normalize_image(
            Some("oci://redis:7"),
            RuntimeBackend::Pyxis,
            tmpdir.path(),
            "svc",
        )
        .expect_err("scheme");
        assert!(err.to_string().contains("unsupported image scheme"));
        let err = normalize_image(
            Some("./Dockerfile"),
            RuntimeBackend::Pyxis,
            tmpdir.path(),
            "svc",
        )
        .expect_err("local path");
        assert!(
            err.to_string()
                .contains("Dockerfiles and build contexts are not supported")
        );

        let mount = normalize_mount("./data:/data", tmpdir.path()).expect("mount");
        assert!(mount.contains("/data"));
        assert_eq!(
            resolve_path("relative/path", tmpdir.path()).expect("resolve"),
            tmpdir.path().join("relative/path")
        );
        assert_eq!(
            crate::path_util::normalize_path(PathBuf::from("/tmp/a/./b/../c")),
            PathBuf::from("/tmp/a/c")
        );
    }

    #[test]
    fn resolve_cache_dir_and_existing_path_cover_defaults_and_failures() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let resolved = resolve_cache_dir(&SlurmConfig::default(), tmpdir.path()).expect("cache");
        assert!(resolved.ends_with(".cache/hpc-compose"));

        let explicit = resolve_cache_dir(
            &SlurmConfig {
                cache_dir: Some("./cache".into()),
                ..SlurmConfig::default()
            },
            tmpdir.path(),
        )
        .expect("explicit");
        assert_eq!(explicit, tmpdir.path().join("cache"));

        assert_eq!(
            normalize_existing_path(&compose).expect("existing"),
            compose.canonicalize().expect("canon")
        );
        let err =
            normalize_existing_path(&tmpdir.path().join("missing.yaml")).expect_err("missing");
        assert!(err.to_string().contains("failed to canonicalize"));
    }

    #[test]
    fn build_plan_rejects_service_healthy_without_readiness() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "app".into(),
                    ServiceSpec {
                        depends_on: DependsOnSpec::Map(BTreeMap::from([(
                            "redis".into(),
                            DependsOnConditionSpec {
                                condition: Some("service_healthy".into()),
                            },
                        )])),
                        ..service("redis:7")
                    },
                ),
                ("redis".into(), service("redis:7")),
            ]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let err = build_plan(&compose, spec).expect_err("missing readiness");
        assert!(err.to_string().contains("service_healthy"));
        assert!(err.to_string().contains("does not define readiness"));
    }

    #[test]
    fn build_plan_preserves_dependency_conditions() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "app".into(),
                    ServiceSpec {
                        depends_on: DependsOnSpec::Map(BTreeMap::from([
                            (
                                "cache".into(),
                                DependsOnConditionSpec {
                                    condition: Some("service_started".into()),
                                },
                            ),
                            (
                                "redis".into(),
                                DependsOnConditionSpec {
                                    condition: Some("service_healthy".into()),
                                },
                            ),
                        ])),
                        ..service("redis:7")
                    },
                ),
                (
                    "redis".into(),
                    ServiceSpec {
                        readiness: Some(ReadinessSpec::Log {
                            pattern: "ready".into(),
                            timeout_seconds: Some(5),
                        }),
                        ..service("redis:7")
                    },
                ),
                ("cache".into(), service("redis:7")),
            ]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let plan = build_plan(&compose, spec).expect("plan");
        assert_eq!(
            plan.ordered_services
                .last()
                .expect("app")
                .depends_on
                .iter()
                .map(|dep| (&dep.name, dep.condition))
                .collect::<Vec<_>>(),
            vec![
                (&"cache".to_string(), DependencyCondition::ServiceStarted),
                (&"redis".to_string(), DependencyCondition::ServiceHealthy),
            ]
        );
    }

    #[test]
    fn build_plan_normalizes_failure_policy_defaults_and_overrides() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                ("default".into(), service("redis:7")),
                (
                    "restart-defaults".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::RestartOnFailure,
                                max_restarts: None,
                                backoff_seconds: None,
                                window_seconds: None,
                                max_restarts_in_window: None,
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "restart-custom".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::RestartOnFailure,
                                max_restarts: Some(7),
                                backoff_seconds: Some(9),
                                window_seconds: Some(11),
                                max_restarts_in_window: Some(4),
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "ignore".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::Ignore,
                                max_restarts: None,
                                backoff_seconds: None,
                                window_seconds: None,
                                max_restarts_in_window: None,
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
            ]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let plan = build_plan(&compose, spec).expect("plan");
        let by_name = plan
            .ordered_services
            .iter()
            .map(|service| (service.name.as_str(), service.failure_policy.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            by_name.get("default"),
            Some(&ServiceFailurePolicy::default())
        );
        assert_eq!(
            by_name.get("restart-defaults"),
            Some(&ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 3,
                backoff_seconds: 5,
                window_seconds: 60,
                max_restarts_in_window: 3,
            })
        );
        assert_eq!(
            by_name.get("restart-custom"),
            Some(&ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 7,
                backoff_seconds: 9,
                window_seconds: 11,
                max_restarts_in_window: 4,
            })
        );
        assert_eq!(
            by_name.get("ignore"),
            Some(&ServiceFailurePolicy {
                mode: ServiceFailureMode::Ignore,
                max_restarts: 0,
                backoff_seconds: 0,
                window_seconds: 0,
                max_restarts_in_window: 0,
            })
        );
    }

    #[test]
    fn build_plan_applies_partial_restart_window_overrides() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "window-seconds-only".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::RestartOnFailure,
                                max_restarts: Some(4),
                                backoff_seconds: Some(9),
                                window_seconds: Some(30),
                                max_restarts_in_window: None,
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
                (
                    "window-count-only".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::RestartOnFailure,
                                max_restarts: Some(6),
                                backoff_seconds: Some(7),
                                window_seconds: None,
                                max_restarts_in_window: Some(2),
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
            ]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let plan = build_plan(&compose, spec).expect("plan");
        let by_name = plan
            .ordered_services
            .iter()
            .map(|service| (service.name.as_str(), service.failure_policy.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            by_name.get("window-seconds-only"),
            Some(&ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 4,
                backoff_seconds: 9,
                window_seconds: 30,
                max_restarts_in_window: 4,
            })
        );
        assert_eq!(
            by_name.get("window-count-only"),
            Some(&ServiceFailurePolicy {
                mode: ServiceFailureMode::RestartOnFailure,
                max_restarts: 6,
                backoff_seconds: 7,
                window_seconds: 60,
                max_restarts_in_window: 2,
            })
        );
    }

    #[test]
    fn build_plan_rejects_invalid_failure_policy_combinations() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let invalid_non_restart = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::FailJob,
                            max_restarts: Some(2),
                            backoff_seconds: None,
                            window_seconds: None,
                            max_restarts_in_window: Some(1),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, invalid_non_restart).expect_err("invalid fail_job policy");
        assert!(
            err.to_string()
                .contains("only valid when mode is restart_on_failure")
        );

        let invalid_restart = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(0),
                            backoff_seconds: Some(5),
                            window_seconds: Some(10),
                            max_restarts_in_window: Some(2),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, invalid_restart).expect_err("invalid restart policy");
        assert!(err.to_string().contains("max_restarts"));

        let invalid_window = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(2),
                            backoff_seconds: Some(5),
                            window_seconds: Some(0),
                            max_restarts_in_window: Some(1),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err = build_plan(&compose, invalid_window).expect_err("invalid restart window");
        assert!(err.to_string().contains("window_seconds"));

        let invalid_window_count = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([(
                "app".into(),
                ServiceSpec {
                    slurm: ServiceSlurmConfig {
                        failure_policy: Some(ServiceFailurePolicySpec {
                            mode: ServiceFailureMode::RestartOnFailure,
                            max_restarts: Some(2),
                            backoff_seconds: Some(5),
                            window_seconds: Some(10),
                            max_restarts_in_window: Some(0),
                        }),
                        ..ServiceSlurmConfig::default()
                    },
                    ..service("redis:7")
                },
            )]),
        };
        let err =
            build_plan(&compose, invalid_window_count).expect_err("invalid restart window count");
        assert!(err.to_string().contains("max_restarts_in_window"));
    }

    #[test]
    fn build_plan_rejects_dependencies_on_ignore_services() {
        let spec = ComposeSpec {
            runtime: RuntimeConfig::default(),
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            software_env: crate::spec::SoftwareEnvConfig::default(),
            services: BTreeMap::from([
                (
                    "app".into(),
                    ServiceSpec {
                        depends_on: DependsOnSpec::List(vec!["sidecar".into()]),
                        ..service("redis:7")
                    },
                ),
                (
                    "sidecar".into(),
                    ServiceSpec {
                        slurm: ServiceSlurmConfig {
                            failure_policy: Some(ServiceFailurePolicySpec {
                                mode: ServiceFailureMode::Ignore,
                                max_restarts: None,
                                backoff_seconds: None,
                                window_seconds: None,
                                max_restarts_in_window: None,
                            }),
                            ..ServiceSlurmConfig::default()
                        },
                        ..service("redis:7")
                    },
                ),
            ]),
        };
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");
        let err = build_plan(&compose, spec).expect_err("ignore dependency");
        assert!(err.to_string().contains("cannot be depended on"));
    }
}
