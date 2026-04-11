//! Normalization from parsed spec into an execution plan.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::readiness_util::readiness_uses_implicit_localhost;
use crate::spec::{
    CommandSpec, ComposeSpec, DependencyCondition, PrepareSpec, ReadinessSpec, ServiceDependency,
    ServiceEnrootConfig, ServiceFailureMode, ServiceFailurePolicy, ServiceSlurmConfig, SlurmConfig,
};

const RESERVED_RUNTIME_MOUNT_DESTINATIONS: &[&str] = &["/hpc-compose/job"];

/// A normalized application plan derived from a compose file.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct Plan {
    pub name: String,
    pub project_dir: PathBuf,
    pub spec_path: PathBuf,
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
}

impl Default for ServicePlacement {
    fn default() -> Self {
        Self {
            mode: ServicePlacementMode::PrimaryNode,
            nodes: 1,
            ntasks: Some(1),
            ntasks_per_node: None,
            pin_to_primary_node: false,
        }
    }
}

/// Where a service image comes from after normalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ImageSource {
    /// A local `.sqsh` or `.squashfs` file used directly at runtime.
    LocalSqsh(PathBuf),
    /// A remote image reference imported through Enroot.
    Remote(String),
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

/// A normalized `x-enroot.prepare` block attached to a service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
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
        let mut volumes = Vec::with_capacity(service.volumes.len());
        for mount in &service.volumes {
            let mount = normalize_mount(mount, &project_dir)?;
            ensure_runtime_mount_destination_allowed(name, &mount)?;
            volumes.push(mount);
        }
        let working_dir = service.working_dir.clone();
        let execution = build_execution(
            service.entrypoint.as_ref(),
            service.command.as_ref(),
            working_dir.as_deref(),
            name,
        )?;
        let image = normalize_image(&service.image, &project_dir)?;
        let prepare = normalize_prepare(service.enroot.clone(), &project_dir, name)?;
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
    let default_distributed_service = allocation_nodes > 1 && services.len() == 1;
    let mut distributed_services = Vec::new();

    for service in services.values_mut() {
        let placement = resolve_service_placement(
            service,
            slurm,
            allocation_nodes,
            default_distributed_service,
        )?;
        if placement.mode == ServicePlacementMode::Distributed {
            distributed_services.push(service.name.clone());
        }
        service.placement = placement;
    }

    if distributed_services.len() > 1 {
        bail!(
            "multi-node allocations support at most one distributed service, but {} request full-allocation placement: {}",
            distributed_services.len(),
            distributed_services.join(", ")
        );
    }

    Ok(())
}

fn resolve_service_placement(
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
                    "service '{}' requests x-slurm.nodes={}, but multi-node v1 only supports helpers on 1 node or one distributed service spanning all {} nodes",
                    service.name,
                    nodes,
                    allocation_nodes
                );
            }
            None => default_distributed_service,
        }
    };

    if distributed && readiness_uses_implicit_localhost(service.readiness.as_ref()) {
        bail!(
            "service '{}' uses readiness that relies on localhost semantics, but distributed services must use sleep/log readiness or explicit non-local hosts",
            service.name
        );
    }

    let (nodes, ntasks, ntasks_per_node, pin_to_primary_node, mode) = if distributed {
        (
            allocation_nodes,
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
            false,
            ServicePlacementMode::Distributed,
        )
    } else {
        (
            1,
            service
                .slurm
                .ntasks
                .or_else(|| service.slurm.ntasks_per_node.is_none().then_some(1)),
            service.slurm.ntasks_per_node,
            allocation_nodes > 1,
            ServicePlacementMode::PrimaryNode,
        )
    };

    Ok(ServicePlacement {
        mode,
        nodes,
        ntasks,
        ntasks_per_node,
        pin_to_primary_node,
    })
}

fn normalize_prepare(
    cfg: ServiceEnrootConfig,
    project_dir: &Path,
    service_name: &str,
) -> Result<Option<PreparedImageSpec>> {
    let Some(prepare) = cfg.prepare else {
        return Ok(None);
    };
    let prepare = build_prepare_plan(prepare, project_dir, service_name)?;
    Ok(Some(prepare))
}

fn build_prepare_plan(
    prepare: PrepareSpec,
    project_dir: &Path,
    service_name: &str,
) -> Result<PreparedImageSpec> {
    if prepare.commands.is_empty() {
        bail!(
            "service '{service_name}' uses x-enroot.prepare but does not define any prepare.commands"
        );
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

fn normalize_image(image: &str, project_dir: &Path) -> Result<ImageSource> {
    if looks_like_local_sqsh(image) {
        return Ok(ImageSource::LocalSqsh(resolve_path(image, project_dir)?));
    }

    if image.contains("://") {
        if image.starts_with("docker://")
            || image.starts_with("dockerd://")
            || image.starts_with("podman://")
        {
            return Ok(ImageSource::Remote(image.to_string()));
        }
        bail!(
            "unsupported image scheme in '{image}'; use docker://, dockerd://, podman://, or a local .sqsh path"
        );
    }

    if looks_like_explicit_local_path(image) {
        bail!(
            "local image path '{image}' must point to a .sqsh or .squashfs file; Dockerfiles and build contexts are not supported in v1"
        );
    }

    Ok(ImageSource::Remote(format!("docker://{image}")))
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

fn normalize_mount(mount: &str, project_dir: &Path) -> Result<String> {
    let Some((host, rest)) = mount.split_once(':') else {
        bail!("mount '{mount}' must use host_path:container_path syntax");
    };
    let host_path = resolve_path(host, project_dir)?;
    Ok(format!("{}:{rest}", host_path.display()))
}

fn ensure_runtime_mount_destination_allowed(service_name: &str, mount: &str) -> Result<()> {
    let Some((_, container_path)) = mount.rsplit_once(':') else {
        bail!("mount '{mount}' must use host_path:container_path syntax");
    };
    if RESERVED_RUNTIME_MOUNT_DESTINATIONS.contains(&container_path) {
        bail!(
            "service '{service_name}' uses reserved runtime mount destination '{container_path}'; that path is provided automatically for per-job shared state"
        );
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use super::*;
    use crate::spec::{
        ComposeSpec, DependsOnConditionSpec, DependsOnSpec, EnvironmentSpec, ReadinessSpec,
        ServiceDependency, ServiceEnrootConfig, ServiceFailureMode, ServiceFailurePolicy,
        ServiceFailurePolicySpec, ServiceSlurmConfig, ServiceSpec,
    };

    fn service(image: &str) -> ServiceSpec {
        ServiceSpec {
            image: image.to_string(),
            command: None,
            entrypoint: None,
            environment: EnvironmentSpec::None,
            volumes: Vec::new(),
            working_dir: None,
            depends_on: DependsOnSpec::None,
            readiness: None,
            healthcheck: None,
            slurm: ServiceSlurmConfig::default(),
            enroot: ServiceEnrootConfig::default(),
        }
    }

    #[test]
    fn bare_images_normalize_to_docker_uri() {
        let spec = ComposeSpec {
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
            services: BTreeMap::from([("redis".into(), service("redis:7"))]),
        };
        let plan = build_plan(Path::new("."), spec).expect("plan");
        assert_eq!(
            plan.ordered_services[0].image,
            ImageSource::Remote("docker://redis:7".into())
        );
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
    fn prepare_mounts_force_rebuild() {
        let spec = PrepareSpec {
            commands: vec!["echo hello".into()],
            mounts: vec!["./data:/data".into()],
            env: EnvironmentSpec::None,
            root: true,
        };
        let prepare = build_prepare_plan(spec, Path::new("/tmp/project"), "svc").expect("prepare");
        assert!(prepare.force_rebuild);
        assert_eq!(prepare.mounts, vec!["/tmp/project/data:/data"]);
    }

    #[test]
    fn topo_sort_orders_dependencies() {
        let spec = ComposeSpec {
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
                name: None,
                slurm: SlurmConfig::default(),
                services: BTreeMap::new(),
            },
        )
        .expect_err("empty services");
        assert!(err.to_string().contains("at least one service"));

        let plan = build_plan(
            &compose,
            ComposeSpec {
                name: None,
                slurm: SlurmConfig {
                    nodes: Some(2),
                    ntasks_per_node: Some(4),
                    ..SlurmConfig::default()
                },
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
    fn build_plan_rejects_multiple_distributed_services_in_multi_node_allocations() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let spec = ComposeSpec {
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
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

        let err = build_plan(&compose, spec).expect_err("multiple distributed services");
        assert!(err.to_string().contains("at most one distributed service"));
    }

    #[test]
    fn build_plan_rejects_distributed_readiness_with_localhost_semantics() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        std::fs::write(&compose, "services: {}\n").expect("write");

        let spec = ComposeSpec {
            name: Some("demo".into()),
            slurm: SlurmConfig {
                nodes: Some(2),
                ..SlurmConfig::default()
            },
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
            normalize_image(local_sqsh.to_str().expect("path"), tmpdir.path()).expect("local"),
            ImageSource::LocalSqsh(local_sqsh.clone())
        );
        assert_eq!(
            normalize_image("docker://redis:7", tmpdir.path()).expect("remote"),
            ImageSource::Remote("docker://redis:7".into())
        );

        let err = normalize_image("oci://redis:7", tmpdir.path()).expect_err("scheme");
        assert!(err.to_string().contains("unsupported image scheme"));
        let err = normalize_image("./Dockerfile", tmpdir.path()).expect_err("local path");
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
            name: Some("demo".into()),
            slurm: SlurmConfig::default(),
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
