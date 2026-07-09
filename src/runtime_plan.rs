//! Runtime-ready plan model and deterministic artifact path derivation.

use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Serialize;

use crate::domain::{artifact_cache_key, short_digest_prefix};
use crate::planner::{
    ExecutionSpec, ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement,
};
use crate::spec::{
    ReadinessSpec, RuntimeBackend, RuntimeConfig, ServiceAssertSpec, ServiceDependency,
    ServiceFailurePolicy, ServiceSlurmConfig, SlurmConfig,
};

/// A plan with concrete runtime image paths for every service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RuntimePlan {
    pub name: String,
    pub cache_dir: PathBuf,
    pub runtime: RuntimeConfig,
    pub slurm: SlurmConfig,
    pub ordered_services: Vec<RuntimeService>,
}

/// A runtime-ready service entry with resolved image artifact paths.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RuntimeService {
    pub name: String,
    pub runtime_image: PathBuf,
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
    pub source: ImageSource,
}

/// Converts a normalized [`Plan`] into a runtime plan with cache artifact paths.
#[must_use]
pub fn build_runtime_plan(plan: &Plan) -> RuntimePlan {
    RuntimePlan {
        name: plan.name.clone(),
        cache_dir: plan.cache_dir.clone(),
        runtime: plan.runtime.clone(),
        slurm: plan.slurm.clone(),
        ordered_services: plan
            .ordered_services
            .iter()
            .map(|service| RuntimeService {
                name: service.name.clone(),
                runtime_image: runtime_image_path(plan, service),
                execution: service.execution.clone(),
                environment: service.environment.clone(),
                volumes: service.volumes.clone(),
                working_dir: service.working_dir.clone(),
                depends_on: service.depends_on.clone(),
                readiness: service.readiness.clone(),
                assertions: service.assertions.clone(),
                failure_policy: service.failure_policy.clone(),
                placement: service.placement.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
                source: service.image.clone(),
            })
            .collect(),
    }
}

/// Returns the cache location used for a service's imported base image.
#[must_use]
pub fn base_image_path(cache_dir: &Path, service: &RuntimeService) -> PathBuf {
    base_image_path_from_source_for_backend(cache_dir, &service.source, RuntimeBackend::Pyxis)
}

/// Returns the cache location used for a service's imported base image under a
/// specific runtime backend.
#[must_use]
pub fn base_image_path_for_backend(
    cache_dir: &Path,
    service: &RuntimeService,
    backend: RuntimeBackend,
) -> PathBuf {
    base_image_path_from_source_for_backend(cache_dir, &service.source, backend)
}

/// Returns the cache location for a base image given its source reference.
#[must_use]
pub fn base_image_path_from_source(cache_dir: &Path, source: &ImageSource) -> PathBuf {
    base_image_path_from_source_for_backend(cache_dir, source, RuntimeBackend::Pyxis)
}

fn base_image_path_from_source_for_backend(
    cache_dir: &Path,
    source: &ImageSource,
    backend: RuntimeBackend,
) -> PathBuf {
    let key = base_image_cache_key_from_source(source);
    let extension = image_artifact_extension(source, backend);
    cache_dir.join("base").join(format!(
        "{}-{}.{}",
        short_digest_prefix(&key),
        sanitize_name(&image_label(source)),
        extension
    ))
}

fn runtime_image_path(plan: &Plan, service: &PlannedService) -> PathBuf {
    let extension = image_artifact_extension(&service.image, plan.runtime.backend);
    match (&service.image, &service.prepare) {
        (ImageSource::LocalSqsh(path), None) => path.clone(),
        (ImageSource::LocalSif(path), None) => path.clone(),
        (ImageSource::Host, _) => PathBuf::new(),
        (ImageSource::Remote(_), None) => base_image_path_from_source_for_backend(
            &plan.cache_dir,
            &service.image,
            plan.runtime.backend,
        ),
        (_, Some(prepare)) => plan.cache_dir.join("prepared").join(format!(
            "{}-{}.{}",
            short_digest_prefix(&prepared_image_cache_key_from_plan(
                service,
                prepare,
                plan.runtime.backend
            )),
            sanitize_name(&service.name),
            extension
        )),
    }
}

pub(crate) fn prepared_image_cache_key_from_plan(
    service: &PlannedService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    prepared_image_cache_key_parts(&service.image, prepare, backend)
}

pub(crate) fn prepared_image_cache_key(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    prepared_image_cache_key_parts(&service.source, prepare, backend)
}

fn prepared_image_cache_key_parts(
    source: &ImageSource,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        backend.as_str().to_string(),
    ];
    match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => {
            parts.push(path.to_string_lossy().into_owned());
        }
        ImageSource::Remote(remote) => parts.push(remote.clone()),
        ImageSource::Host => parts.push("host".to_string()),
    }
    parts.extend(prepare.commands.iter().cloned());
    parts.extend(prepare.mounts.iter().cloned());
    parts.extend(
        prepare
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}")),
    );
    parts.push(format!("root={}", prepare.root));
    artifact_cache_key(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

pub(crate) fn base_image_cache_key(service: &RuntimeService) -> String {
    base_image_cache_key_from_source(&service.source)
}

fn base_image_cache_key_from_source(source: &ImageSource) -> String {
    let image_key = match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => {
            path.to_string_lossy().into_owned()
        }
        ImageSource::Remote(remote) => remote.clone(),
        ImageSource::Host => "host".to_string(),
    };
    artifact_cache_key(&["base", image_key.as_str(), env!("CARGO_PKG_VERSION")])
}

fn image_artifact_extension(source: &ImageSource, backend: RuntimeBackend) -> &'static str {
    match source {
        ImageSource::LocalSif(_) => "sif",
        ImageSource::Remote(_) if backend.uses_sif() => "sif",
        ImageSource::Remote(_) | ImageSource::LocalSqsh(_) => "sqsh",
        ImageSource::Host => "host",
    }
}

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn image_label(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("local-image")
            .to_string(),
        ImageSource::Remote(remote) => remote
            .rsplit('/')
            .next()
            .unwrap_or(remote.as_str())
            .replace(':', "-"),
        ImageSource::Host => "host".to_string(),
    }
}
