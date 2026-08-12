//! Runtime-ready plan model and deterministic artifact path derivation.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::Serialize;
use sha2::{Digest, Sha256};

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

/// Returns whether this service permits job-level scratch when it is configured.
pub(crate) fn service_allows_configured_scratch(service: &RuntimeService) -> bool {
    service
        .slurm
        .scratch
        .as_ref()
        .and_then(|scratch| scratch.enabled)
        .unwrap_or(true)
}

/// Converts a normalized [`Plan`] into a runtime plan with cache artifact paths.
///
/// Prepared local images are fingerprinted immediately before their service is
/// derived. The remaining service and cache-path derivation is pure.
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
            .map(|service| {
                let local_image_fingerprint = prepared_local_image_fingerprint(service);
                derive_runtime_service(plan, service, local_image_fingerprint.as_deref())
            })
            .collect(),
    }
}

fn derive_runtime_service(
    plan: &Plan,
    service: &PlannedService,
    local_image_fingerprint: Option<&str>,
) -> RuntimeService {
    RuntimeService {
        name: service.name.clone(),
        runtime_image: runtime_image_path(plan, service, local_image_fingerprint),
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

fn runtime_image_path(
    plan: &Plan,
    service: &PlannedService,
    local_image_fingerprint: Option<&str>,
) -> PathBuf {
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
            short_digest_prefix(&prepared_image_cache_key_parts(
                &service.image,
                prepare,
                plan.runtime.backend,
                local_image_fingerprint,
            )),
            sanitize_name(&service.name),
            extension
        )),
    }
}

#[cfg(test)]
pub(crate) fn prepared_image_cache_key_from_plan(
    service: &PlannedService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    let local_image_fingerprint = local_image_fingerprint_for_source(&service.image);
    prepared_image_cache_key_parts(
        &service.image,
        prepare,
        backend,
        local_image_fingerprint.as_deref(),
    )
}

pub(crate) fn prepared_image_cache_key(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    let local_image_fingerprint = local_image_fingerprint_for_source(&service.source);
    prepared_image_cache_key_parts(
        &service.source,
        prepare,
        backend,
        local_image_fingerprint.as_deref(),
    )
}

fn prepared_image_cache_key_parts(
    source: &ImageSource,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
    local_image_fingerprint: Option<&str>,
) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        backend.as_str().to_string(),
    ];
    match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => {
            parts.push(path.to_string_lossy().into_owned());
            parts.push(
                local_image_fingerprint
                    .expect("local image fingerprint must be resolved before derivation")
                    .to_string(),
            );
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

fn prepared_local_image_fingerprint(service: &PlannedService) -> Option<String> {
    service.prepare.as_ref()?;
    local_image_fingerprint_for_source(&service.image)
}

fn local_image_fingerprint_for_source(source: &ImageSource) -> Option<String> {
    match source {
        ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => {
            Some(local_image_content_fingerprint(path))
        }
        ImageSource::Remote(_) | ImageSource::Host => None,
    }
}

/// Returns an exact, streaming content fingerprint for a local image.
///
/// Prepared-image paths are derived before prepare performs its normal source
/// validation, so this helper stays infallible. A missing/unreadable path gets a
/// stable sentinel and prepare later reports the actionable filesystem error.
fn local_image_content_fingerprint(path: &Path) -> String {
    let Ok(mut file) = File::open(path) else {
        return "sha256:unavailable".to_string();
    };
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(_) => return "sha256:unavailable".to_string(),
        }
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::planner::{ExecutionSpec, ServicePlacement};
    use crate::spec::{
        RuntimeConfig, ServiceFailurePolicy, ServiceScratchConfig, ServiceSlurmConfig, SlurmConfig,
    };

    fn prepared_spec() -> PreparedImageSpec {
        PreparedImageSpec {
            commands: vec!["echo prepare".into()],
            mounts: vec!["/shared/input:/input:ro".into()],
            env: vec![("MODE".into(), "test".into())],
            root: true,
            force_rebuild: false,
        }
    }

    fn planned_service(
        name: &str,
        image: ImageSource,
        prepare: Option<PreparedImageSpec>,
    ) -> PlannedService {
        PlannedService {
            name: name.into(),
            image,
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare,
        }
    }

    fn plan(cache_dir: PathBuf, ordered_services: Vec<PlannedService>) -> Plan {
        Plan {
            name: "runtime-paths".into(),
            project_dir: cache_dir.parent().unwrap_or(&cache_dir).to_path_buf(),
            spec_path: cache_dir.join("compose.yaml"),
            runtime: RuntimeConfig::default(),
            cache_dir,
            slurm: SlurmConfig::default(),
            ordered_services,
        }
    }

    #[test]
    fn service_allows_configured_scratch_preserves_default_true_truth_table() {
        let plan = plan(
            PathBuf::from("/cache"),
            vec![planned_service("scratch", ImageSource::Host, None)],
        );
        let mut service = derive_runtime_service(&plan, &plan.ordered_services[0], None);

        for (scratch, expected) in [
            (None, true),
            (Some(ServiceScratchConfig { enabled: None }), true),
            (
                Some(ServiceScratchConfig {
                    enabled: Some(true),
                }),
                true,
            ),
            (
                Some(ServiceScratchConfig {
                    enabled: Some(false),
                }),
                false,
            ),
        ] {
            service.slurm.scratch = scratch;
            assert_eq!(service_allows_configured_scratch(&service), expected);
        }
    }

    #[test]
    fn prepared_local_image_key_and_path_are_stable_and_content_sensitive() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let image = tmpdir.path().join("base.sqsh");
        fs::write(&image, b"image bytes v1").expect("write local image");
        let prepared = prepared_spec();
        let plan = plan(
            tmpdir.path().join("cache"),
            vec![planned_service(
                "local-app",
                ImageSource::LocalSqsh(image.clone()),
                Some(prepared.clone()),
            )],
        );

        let service = &plan.ordered_services[0];
        let first_key = prepared_image_cache_key_from_plan(
            service,
            service.prepare.as_ref().expect("prepare"),
            plan.runtime.backend,
        );
        let first_path = build_runtime_plan(&plan).ordered_services[0]
            .runtime_image
            .clone();

        fs::write(&image, b"image bytes v1").expect("rewrite identical local image");
        let identical_key = prepared_image_cache_key_from_plan(
            service,
            service.prepare.as_ref().expect("prepare"),
            plan.runtime.backend,
        );
        let identical_path = build_runtime_plan(&plan).ordered_services[0]
            .runtime_image
            .clone();
        assert_eq!(identical_key, first_key);
        assert_eq!(identical_path, first_path);
        assert_eq!(
            first_path,
            plan.cache_dir.join("prepared").join(format!(
                "{}-local-app.sqsh",
                short_digest_prefix(&first_key)
            ))
        );

        fs::write(&image, b"image bytes v2").expect("change local image");
        let changed_key = prepared_image_cache_key_from_plan(
            service,
            service.prepare.as_ref().expect("prepare"),
            plan.runtime.backend,
        );
        let changed_path = build_runtime_plan(&plan).ordered_services[0]
            .runtime_image
            .clone();
        assert_ne!(changed_key, first_key);
        assert_ne!(changed_path, first_path);
        assert_eq!(
            changed_path,
            plan.cache_dir.join("prepared").join(format!(
                "{}-local-app.sqsh",
                short_digest_prefix(&changed_key)
            ))
        );
    }

    #[test]
    fn pure_service_derivation_uses_the_injected_local_image_fingerprint() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("not-read-by-pure-core.sqsh");
        let prepared = prepared_spec();
        let plan = plan(
            tmpdir.path().join("cache"),
            vec![planned_service(
                "local-app",
                ImageSource::LocalSqsh(missing.clone()),
                Some(prepared),
            )],
        );

        let runtime_service =
            derive_runtime_service(&plan, &plan.ordered_services[0], Some("sha256:injected"));
        let expected_key = artifact_cache_key(&[
            "prepared",
            env!("CARGO_PKG_VERSION"),
            RuntimeBackend::Pyxis.as_str(),
            &missing.to_string_lossy(),
            "sha256:injected",
            "echo prepare",
            "/shared/input:/input:ro",
            "MODE=test",
            "root=true",
        ]);

        assert_eq!(
            runtime_service.runtime_image,
            plan.cache_dir.join("prepared").join(format!(
                "{}-local-app.sqsh",
                short_digest_prefix(&expected_key)
            ))
        );
    }

    #[test]
    fn missing_prepared_local_image_uses_stable_unavailable_fingerprint() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("missing.sqsh");
        let prepared = prepared_spec();
        let plan = plan(
            tmpdir.path().join("cache"),
            vec![planned_service(
                "missing-app",
                ImageSource::LocalSqsh(missing.clone()),
                Some(prepared.clone()),
            )],
        );
        let service = &plan.ordered_services[0];

        assert_eq!(
            local_image_content_fingerprint(&missing),
            "sha256:unavailable"
        );
        let expected_key = artifact_cache_key(&[
            "prepared",
            env!("CARGO_PKG_VERSION"),
            RuntimeBackend::Pyxis.as_str(),
            &missing.to_string_lossy(),
            "sha256:unavailable",
            "echo prepare",
            "/shared/input:/input:ro",
            "MODE=test",
            "root=true",
        ]);
        let first_key = prepared_image_cache_key_from_plan(
            service,
            service.prepare.as_ref().expect("prepare"),
            plan.runtime.backend,
        );
        let second_key = prepared_image_cache_key_from_plan(
            service,
            service.prepare.as_ref().expect("prepare"),
            plan.runtime.backend,
        );
        assert_eq!(first_key, expected_key);
        assert_eq!(second_key, expected_key);

        let expected_path = plan.cache_dir.join("prepared").join(format!(
            "{}-missing-app.sqsh",
            short_digest_prefix(&expected_key)
        ));
        assert_eq!(
            build_runtime_plan(&plan).ordered_services[0].runtime_image,
            expected_path
        );
    }

    #[test]
    fn remote_and_host_runtime_image_paths_keep_their_existing_derivation() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let remote = ImageSource::Remote("docker://redis:7".into());
        let plan = plan(
            tmpdir.path().join("cache"),
            vec![
                planned_service("remote", remote.clone(), None),
                planned_service("remote-prepared", remote, Some(prepared_spec())),
                planned_service("host", ImageSource::Host, None),
            ],
        );
        let runtime = build_runtime_plan(&plan);

        let remote_base_key =
            artifact_cache_key(&["base", "docker://redis:7", env!("CARGO_PKG_VERSION")]);
        assert_eq!(
            runtime.ordered_services[0].runtime_image,
            plan.cache_dir.join("base").join(format!(
                "{}-redis-7.sqsh",
                short_digest_prefix(&remote_base_key)
            ))
        );

        let prepared = plan.ordered_services[1].prepare.as_ref().expect("prepare");
        let remote_prepared_key = artifact_cache_key(&[
            "prepared",
            env!("CARGO_PKG_VERSION"),
            RuntimeBackend::Pyxis.as_str(),
            "docker://redis:7",
            &prepared.commands[0],
            &prepared.mounts[0],
            "MODE=test",
            "root=true",
        ]);
        assert_eq!(
            runtime.ordered_services[1].runtime_image,
            plan.cache_dir.join("prepared").join(format!(
                "{}-remote-prepared.sqsh",
                short_digest_prefix(&remote_prepared_key)
            ))
        );
        assert_eq!(runtime.ordered_services[2].runtime_image, PathBuf::new());
    }
}
