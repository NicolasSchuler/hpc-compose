//! Runtime artifact preparation and runtime-plan derivation.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cache::{touch_manifest, upsert_base_manifest, upsert_prepared_manifest};
use crate::domain::{artifact_cache_key, short_digest_prefix};
use crate::planner::{
    ExecutionSpec, ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement,
};
use crate::spec::{
    ReadinessSpec, RuntimeBackend, RuntimeConfig, ServiceDependency, ServiceFailurePolicy,
    ServiceSlurmConfig, SlurmConfig,
};

/// A plan with concrete runtime image paths for every service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct RuntimePlan {
    pub name: String,
    pub cache_dir: PathBuf,
    pub runtime: RuntimeConfig,
    pub slurm: SlurmConfig,
    pub ordered_services: Vec<RuntimeService>,
}

/// A runtime-ready service entry with resolved image artifact paths.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeService {
    pub name: String,
    pub runtime_image: PathBuf,
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
    pub source: ImageSource,
}

/// Options that control image import and prepare behavior.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct PrepareOptions {
    pub enroot_bin: String,
    pub apptainer_bin: String,
    pub singularity_bin: String,
    pub keep_failed_prep: bool,
    pub force_rebuild: bool,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self {
            enroot_bin: "enroot".to_string(),
            apptainer_bin: "apptainer".to_string(),
            singularity_bin: "singularity".to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
        }
    }
}

/// How a runtime artifact was obtained during preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactAction {
    /// The artifact already existed and was used as-is.
    Present,
    /// The artifact existed in cache and was refreshed for tracking purposes.
    Reused,
    /// The artifact was built or imported during this run.
    Built,
}

/// Status for one concrete artifact path produced or reused during prepare.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactStatus {
    pub path: PathBuf,
    pub action: ArtifactAction,
    pub note: Option<String>,
}

/// Preparation results for one service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ServicePrepareResult {
    pub service_name: String,
    pub base_image: Option<ArtifactStatus>,
    pub runtime_image: ArtifactStatus,
}

/// Summary of all service preparations in a runtime plan.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize)]
pub struct PrepareSummary {
    pub services: Vec<ServicePrepareResult>,
}

/// Converts a normalized [`Plan`] into a runtime plan with cache artifact paths.
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
                failure_policy: service.failure_policy.clone(),
                placement: service.placement.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
                source: service.image.clone(),
            })
            .collect(),
    }
}

/// Imports and prepares any missing runtime artifacts for the given plan.
pub fn prepare_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
) -> Result<PrepareSummary> {
    match plan.runtime.backend {
        RuntimeBackend::Pyxis => prepare_pyxis_runtime_plan(plan, options),
        RuntimeBackend::Apptainer | RuntimeBackend::Singularity => {
            prepare_sif_runtime_plan(plan, options)
        }
        RuntimeBackend::Host => prepare_host_runtime_plan(plan),
    }
}

fn prepare_pyxis_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
) -> Result<PrepareSummary> {
    ensure_binary_available(
        &options.enroot_bin,
        "Enroot is required for submit; install it or pass a valid enroot binary path",
    )?;
    let envs = enroot_env(&plan.cache_dir);
    create_cache_dirs(plan)?;
    let mut summary = PrepareSummary::default();
    let mut refreshed_base_images = HashSet::new();

    for service in &plan.ordered_services {
        let mut result = ServicePrepareResult {
            service_name: service.name.clone(),
            base_image: None,
            runtime_image: ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Present,
                note: None,
            },
        };

        match &service.source {
            ImageSource::LocalSqsh(path) => {
                if !path.exists() {
                    bail!(
                        "service '{}' references local image '{}', but that file does not exist",
                        service.name,
                        path.display()
                    );
                }
                result.runtime_image = ArtifactStatus {
                    path: path.clone(),
                    action: ArtifactAction::Present,
                    note: Some("uses local .sqsh directly".to_string()),
                };
            }
            ImageSource::LocalSif(path) => {
                bail!(
                    "service '{}' references SIF image '{}', but runtime.backend=pyxis requires Enroot-compatible images",
                    service.name,
                    path.display()
                );
            }
            ImageSource::Remote(remote) => {
                let base_path = base_image_path(&plan.cache_dir, service);
                let base_cache_key = base_image_cache_key(service);
                let needs_import = !base_path.exists()
                    || (options.force_rebuild && !refreshed_base_images.contains(&base_path));
                let base_action = if needs_import {
                    ensure_parent_dir(&base_path)?;
                    run_enroot(
                        &options.enroot_bin,
                        &envs,
                        [
                            "import".to_string(),
                            "-o".to_string(),
                            base_path.display().to_string(),
                            remote.clone(),
                        ],
                        &format!("import base image for service '{}'", service.name),
                    )?;
                    refreshed_base_images.insert(base_path.clone());
                    ArtifactAction::Built
                } else {
                    ArtifactAction::Reused
                };
                upsert_base_manifest(&base_path, &service.name, &service.source, &base_cache_key)?;
                result.base_image = Some(ArtifactStatus {
                    path: base_path.clone(),
                    action: base_action,
                    note: None,
                });
                if service.prepare.is_none() {
                    result.runtime_image = ArtifactStatus {
                        path: base_path,
                        action: base_action,
                        note: Some("base cache artifact is used directly at runtime".to_string()),
                    };
                }
            }
            ImageSource::Host => unreachable!("host backend handled before Pyxis prepare"),
        }

        let Some(prepare) = &service.prepare else {
            if !matches!(service.source, ImageSource::LocalSqsh(_)) {
                touch_manifest(&service.runtime_image)?;
            }
            summary.services.push(result);
            continue;
        };

        let forced_by_mounts = prepare.force_rebuild;
        let should_rebuild =
            options.force_rebuild || forced_by_mounts || !service.runtime_image.exists();
        if should_rebuild {
            ensure_parent_dir(&service.runtime_image)?;
            prepare_service_image(service, prepare, &plan.cache_dir, options, &envs)?;
            let note = if options.force_rebuild {
                Some("rebuilt because --force/--force-rebuild was requested".to_string())
            } else if forced_by_mounts {
                Some("rebuilt because prepare.mounts are present".to_string())
            } else {
                None
            };
            upsert_prepared_manifest(
                &service.runtime_image,
                &service.name,
                &service.source,
                &prepared_image_cache_key(service, prepare, plan.runtime.backend),
                prepare,
            )?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Built,
                note,
            };
        } else {
            touch_manifest(&service.runtime_image)?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Reused,
                note: None,
            };
        }
        summary.services.push(result);
    }

    Ok(summary)
}

fn prepare_host_runtime_plan(plan: &RuntimePlan) -> Result<PrepareSummary> {
    let mut summary = PrepareSummary::default();
    for service in &plan.ordered_services {
        summary.services.push(ServicePrepareResult {
            service_name: service.name.clone(),
            base_image: None,
            runtime_image: ArtifactStatus {
                path: PathBuf::new(),
                action: ArtifactAction::Present,
                note: Some("host runtime does not use image artifacts".to_string()),
            },
        });
    }
    Ok(summary)
}

fn prepare_sif_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
) -> Result<PrepareSummary> {
    let runtime_bin = sif_runtime_bin(plan.runtime.backend, options);
    ensure_binary_available(
        runtime_bin,
        &format!(
            "{} is required for runtime.backend={}; install it or pass the matching binary path",
            runtime_bin,
            plan.runtime.backend.as_str()
        ),
    )?;
    create_cache_dirs(plan)?;
    let mut summary = PrepareSummary::default();
    let mut refreshed_base_images = HashSet::new();

    for service in &plan.ordered_services {
        let mut result = ServicePrepareResult {
            service_name: service.name.clone(),
            base_image: None,
            runtime_image: ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Present,
                note: None,
            },
        };

        match &service.source {
            ImageSource::LocalSif(path) => {
                if !path.exists() {
                    bail!(
                        "service '{}' references local SIF image '{}', but that file does not exist",
                        service.name,
                        path.display()
                    );
                }
                result.runtime_image = ArtifactStatus {
                    path: path.clone(),
                    action: ArtifactAction::Present,
                    note: Some("uses local .sif directly".to_string()),
                };
            }
            ImageSource::Remote(remote) => {
                let base_path =
                    base_image_path_for_backend(&plan.cache_dir, service, plan.runtime.backend);
                let base_cache_key = base_image_cache_key(service);
                let needs_build = !base_path.exists()
                    || (options.force_rebuild && !refreshed_base_images.contains(&base_path));
                let base_action = if needs_build {
                    ensure_parent_dir(&base_path)?;
                    run_container_runtime(
                        runtime_bin,
                        [
                            "build".to_string(),
                            "--force".to_string(),
                            base_path.display().to_string(),
                            remote.clone(),
                        ],
                        &format!("build base SIF for service '{}'", service.name),
                    )?;
                    refreshed_base_images.insert(base_path.clone());
                    ArtifactAction::Built
                } else {
                    ArtifactAction::Reused
                };
                upsert_base_manifest(&base_path, &service.name, &service.source, &base_cache_key)?;
                result.base_image = Some(ArtifactStatus {
                    path: base_path.clone(),
                    action: base_action,
                    note: None,
                });
                if service.prepare.is_none() {
                    result.runtime_image = ArtifactStatus {
                        path: base_path,
                        action: base_action,
                        note: Some(
                            "base SIF cache artifact is used directly at runtime".to_string(),
                        ),
                    };
                }
            }
            ImageSource::LocalSqsh(path) => {
                bail!(
                    "service '{}' references Enroot image '{}', but runtime.backend={} requires SIF images",
                    service.name,
                    path.display(),
                    plan.runtime.backend.as_str()
                );
            }
            ImageSource::Host => unreachable!("host backend handled before SIF prepare"),
        }

        let Some(prepare) = &service.prepare else {
            if !matches!(service.source, ImageSource::LocalSif(_)) {
                touch_manifest(&service.runtime_image)?;
            }
            summary.services.push(result);
            continue;
        };

        let forced_by_mounts = prepare.force_rebuild;
        let should_rebuild =
            options.force_rebuild || forced_by_mounts || !service.runtime_image.exists();
        if should_rebuild {
            ensure_parent_dir(&service.runtime_image)?;
            prepare_service_sif(
                service,
                prepare,
                &plan.cache_dir,
                plan.runtime.backend,
                runtime_bin,
                options,
            )?;
            let note = if options.force_rebuild {
                Some("rebuilt because --force/--force-rebuild was requested".to_string())
            } else if forced_by_mounts {
                Some("rebuilt because prepare.mounts are present".to_string())
            } else {
                None
            };
            upsert_prepared_manifest(
                &service.runtime_image,
                &service.name,
                &service.source,
                &prepared_image_cache_key(service, prepare, plan.runtime.backend),
                prepare,
            )?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Built,
                note,
            };
        } else {
            touch_manifest(&service.runtime_image)?;
            result.runtime_image = ArtifactStatus {
                path: service.runtime_image.clone(),
                action: ArtifactAction::Reused,
                note: None,
            };
        }
        summary.services.push(result);
    }

    Ok(summary)
}

fn prepare_service_image(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    cache_dir: &Path,
    options: &PrepareOptions,
    envs: &[(String, String)],
) -> Result<()> {
    let rootfs_name = temporary_rootfs_name(service);
    let base_image = match &service.source {
        ImageSource::LocalSqsh(path) => path.clone(),
        ImageSource::Remote(_) => base_image_path(cache_dir, service),
        ImageSource::LocalSif(_) | ImageSource::Host => unreachable!("validated by backend"),
    };

    let cleanup_result =
        run_prepare_sequence(service, prepare, &rootfs_name, &base_image, options, envs);

    match cleanup_result {
        Ok(()) => {
            remove_rootfs(&options.enroot_bin, envs, &rootfs_name)?;
            Ok(())
        }
        Err(err) => {
            if !options.keep_failed_prep {
                let _ = remove_rootfs(&options.enroot_bin, envs, &rootfs_name);
            }
            Err(err)
        }
    }
}

fn run_prepare_sequence(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    rootfs_name: &str,
    base_image: &Path,
    options: &PrepareOptions,
    envs: &[(String, String)],
) -> Result<()> {
    let _ = remove_rootfs(&options.enroot_bin, envs, rootfs_name);

    run_enroot(
        &options.enroot_bin,
        envs,
        vec![
            "create".to_string(),
            "--force".to_string(),
            "--name".to_string(),
            rootfs_name.to_string(),
            base_image.display().to_string(),
        ],
        &format!("create prepare rootfs for service '{}'", service.name),
    )?;

    for command in &prepare.commands {
        let mut args = vec!["start".to_string()];
        if prepare.root {
            args.push("--root".to_string());
        }
        args.push("--rw".to_string());
        for mount in &prepare.mounts {
            args.push("--mount".to_string());
            args.push(mount.clone());
        }
        for (key, value) in &prepare.env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(rootfs_name.to_string());
        args.push("/bin/sh".to_string());
        args.push("-lc".to_string());
        args.push(command.clone());

        run_enroot(
            &options.enroot_bin,
            envs,
            args,
            &format!("run prepare command for service '{}'", service.name),
        )?;
    }

    run_enroot(
        &options.enroot_bin,
        envs,
        vec![
            "export".to_string(),
            "--force".to_string(),
            "--output".to_string(),
            service.runtime_image.display().to_string(),
            rootfs_name.to_string(),
        ],
        &format!("export prepared image for service '{}'", service.name),
    )?;

    Ok(())
}

fn remove_rootfs(enroot_bin: &str, envs: &[(String, String)], rootfs_name: &str) -> Result<()> {
    run_enroot(
        enroot_bin,
        envs,
        vec![
            "remove".to_string(),
            "--force".to_string(),
            rootfs_name.to_string(),
        ],
        "remove temporary prepare rootfs",
    )
}

fn run_enroot<I>(enroot_bin: &str, envs: &[(String, String)], args: I, context: &str) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args_vec = args.into_iter().collect::<Vec<_>>();
    let mut command = Command::new(enroot_bin);
    command.args(&args_vec);
    command.envs(envs.iter().map(|(k, v)| (k, v)));
    let output = command.output().context(format!(
        "failed to execute '{}' while trying to {}",
        enroot_bin, context
    ))?;
    if !output.status.success() {
        bail!(
            "failed to {}: {}",
            context,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn prepare_service_sif(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    cache_dir: &Path,
    backend: RuntimeBackend,
    runtime_bin: &str,
    options: &PrepareOptions,
) -> Result<()> {
    let sandbox = temporary_sandbox_path(cache_dir, service);
    let base_image = match &service.source {
        ImageSource::LocalSif(path) => path.clone(),
        ImageSource::Remote(_) => base_image_path_for_backend(cache_dir, service, backend),
        ImageSource::LocalSqsh(_) | ImageSource::Host => unreachable!("validated by backend"),
    };

    let cleanup_result =
        run_sif_prepare_sequence(service, prepare, &sandbox, &base_image, runtime_bin);
    match cleanup_result {
        Ok(()) => {
            let _ = fs::remove_dir_all(&sandbox);
            Ok(())
        }
        Err(err) => {
            if !options.keep_failed_prep {
                let _ = fs::remove_dir_all(&sandbox);
            }
            Err(err)
        }
    }
}

fn run_sif_prepare_sequence(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    sandbox: &Path,
    base_image: &Path,
    runtime_bin: &str,
) -> Result<()> {
    let _ = fs::remove_dir_all(sandbox);
    let mut build_args = vec![
        "build".to_string(),
        "--force".to_string(),
        "--sandbox".to_string(),
    ];
    if prepare.root {
        build_args.push("--fakeroot".to_string());
    }
    build_args.push(sandbox.display().to_string());
    build_args.push(base_image.display().to_string());
    run_container_runtime(
        runtime_bin,
        build_args,
        &format!("create prepare sandbox for service '{}'", service.name),
    )?;

    for command in &prepare.commands {
        let mut args = vec!["exec".to_string(), "--writable".to_string()];
        if prepare.root {
            args.push("--fakeroot".to_string());
        }
        for mount in &prepare.mounts {
            args.push("--bind".to_string());
            args.push(mount.clone());
        }
        for (key, value) in &prepare.env {
            args.push("--env".to_string());
            args.push(format!("{key}={value}"));
        }
        args.push(sandbox.display().to_string());
        args.push("/bin/sh".to_string());
        args.push("-lc".to_string());
        args.push(command.clone());
        run_container_runtime(
            runtime_bin,
            args,
            &format!("run prepare command for service '{}'", service.name),
        )?;
    }

    let mut export_args = vec!["build".to_string(), "--force".to_string()];
    if prepare.root {
        export_args.push("--fakeroot".to_string());
    }
    export_args.push(service.runtime_image.display().to_string());
    export_args.push(sandbox.display().to_string());
    run_container_runtime(
        runtime_bin,
        export_args,
        &format!("export prepared SIF for service '{}'", service.name),
    )
}

fn run_container_runtime<I>(runtime_bin: &str, args: I, context: &str) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args_vec = args.into_iter().collect::<Vec<_>>();
    let output = Command::new(runtime_bin)
        .args(&args_vec)
        .output()
        .context(format!(
            "failed to execute '{}' while trying to {}",
            runtime_bin, context
        ))?;
    if !output.status.success() {
        bail!(
            "failed to {}: {}",
            context,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn sif_runtime_bin(backend: RuntimeBackend, options: &PrepareOptions) -> &str {
    match backend {
        RuntimeBackend::Apptainer => options.apptainer_bin.as_str(),
        RuntimeBackend::Singularity => options.singularity_bin.as_str(),
        RuntimeBackend::Pyxis | RuntimeBackend::Host => unreachable!("not a SIF backend"),
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
        short_hash(&key),
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
            short_hash(&prepared_image_cache_key_from_plan(
                service,
                prepare,
                plan.runtime.backend
            )),
            sanitize_name(&service.name),
            extension
        )),
    }
}

fn prepared_image_cache_key_from_plan(
    service: &PlannedService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        backend.as_str().to_string(),
    ];
    match &service.image {
        ImageSource::LocalSqsh(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::LocalSif(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::Remote(remote) => parts.push(remote.clone()),
        ImageSource::Host => parts.push("host".to_string()),
    }
    parts.extend(prepare.commands.iter().cloned());
    parts.extend(prepare.mounts.iter().cloned());
    parts.extend(prepare.env.iter().map(|(k, v)| format!("{k}={v}")));
    parts.push(format!("root={}", prepare.root));
    cache_key(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

fn prepared_image_cache_key(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    backend: RuntimeBackend,
) -> String {
    let mut parts = vec![
        "prepared".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        backend.as_str().to_string(),
    ];
    match &service.source {
        ImageSource::LocalSqsh(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::LocalSif(path) => parts.push(path.to_string_lossy().into_owned()),
        ImageSource::Remote(remote) => parts.push(remote.clone()),
        ImageSource::Host => parts.push("host".to_string()),
    }
    parts.extend(prepare.commands.iter().cloned());
    parts.extend(prepare.mounts.iter().cloned());
    parts.extend(prepare.env.iter().map(|(k, v)| format!("{k}={v}")));
    parts.push(format!("root={}", prepare.root));
    cache_key(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

fn base_image_cache_key(service: &RuntimeService) -> String {
    base_image_cache_key_from_source(&service.source)
}

fn base_image_cache_key_from_source(source: &ImageSource) -> String {
    let image_key = match source {
        ImageSource::LocalSqsh(path) => path.to_string_lossy().into_owned(),
        ImageSource::LocalSif(path) => path.to_string_lossy().into_owned(),
        ImageSource::Remote(remote) => remote.clone(),
        ImageSource::Host => "host".to_string(),
    };
    cache_key(&["base", image_key.as_str(), env!("CARGO_PKG_VERSION")])
}

fn temporary_rootfs_name(service: &RuntimeService) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("hpc-compose-{}-{}", sanitize_name(&service.name), ts)
}

fn temporary_sandbox_path(cache_dir: &Path, service: &RuntimeService) -> PathBuf {
    cache_dir
        .join("prepared")
        .join(format!("{}.sandbox", temporary_rootfs_name(service)))
}

fn cache_key(parts: &[&str]) -> String {
    artifact_cache_key(parts)
}

fn short_hash(hash: &str) -> &str {
    short_digest_prefix(hash)
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

fn image_label(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("local-image")
            .to_string(),
        ImageSource::LocalSif(path) => path
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

fn image_artifact_extension(source: &ImageSource, backend: RuntimeBackend) -> &'static str {
    match source {
        ImageSource::LocalSif(_) => "sif",
        ImageSource::Remote(_) if backend.uses_sif() => "sif",
        ImageSource::Remote(_) => "sqsh",
        ImageSource::LocalSqsh(_) => "sqsh",
        ImageSource::Host => "host",
    }
}

fn create_cache_dirs(plan: &RuntimePlan) -> Result<()> {
    for path in [
        plan.cache_dir.join("base"),
        plan.cache_dir.join("prepared"),
        plan.cache_dir.join("enroot/cache"),
        plan.cache_dir.join("enroot/data"),
        plan.cache_dir.join("enroot/tmp"),
    ] {
        fs::create_dir_all(&path).context(format!(
            "failed to create cache directory {}",
            path.display()
        ))?;
    }
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().context(format!(
        "path '{}' does not have a parent directory",
        path.display()
    ))?;
    fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    Ok(())
}

fn enroot_env(cache_dir: &Path) -> Vec<(String, String)> {
    vec![
        (
            "ENROOT_CACHE_PATH".to_string(),
            cache_dir.join("enroot/cache").display().to_string(),
        ),
        (
            "ENROOT_DATA_PATH".to_string(),
            cache_dir.join("enroot/data").display().to_string(),
        ),
    ]
}

/// Verifies that an external binary is available on the current machine.
///
/// # Errors
///
/// Returns an error when the provided binary path does not exist or the named
/// binary cannot be found on the current `PATH`.
pub fn ensure_binary_available(binary: &str, message: &str) -> Result<()> {
    if binary.contains(std::path::MAIN_SEPARATOR) {
        let path = Path::new(binary);
        if path.exists() {
            return Ok(());
        }
        bail!("{message}");
    }

    let path_var = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(binary);
        if candidate.exists() {
            return Ok(());
        }
    }
    bail!("{message}");
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use crate::planner::{ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement};
    use crate::spec::{ServiceFailurePolicy, SlurmConfig};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn fake_service(tmpdir: &Path) -> RuntimeService {
        RuntimeService {
            name: "svc".into(),
            runtime_image: tmpdir.join("prepared/svc.sqsh"),
            execution: ExecutionSpec::Shell("echo ready".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: Some(PreparedImageSpec {
                commands: vec!["echo setup".into()],
                mounts: Vec::new(),
                env: vec![("A".into(), "B".into())],
                root: true,
                force_rebuild: false,
            }),
            source: ImageSource::Remote("docker://redis:7".into()),
        }
    }

    #[test]
    fn runtime_plan_conversion_preserves_planned_service_contract() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let prepare = PreparedImageSpec {
            commands: vec!["echo setup".into()],
            mounts: vec!["/host/input:/input:ro".into()],
            env: vec![("A".into(), "B".into())],
            root: true,
            force_rebuild: true,
        };
        let planned = PlannedService {
            name: "app".into(),
            image: ImageSource::Remote("docker://python:3.11-slim".into()),
            execution: ExecutionSpec::Exec(vec!["python".into(), "-m".into(), "app".into()]),
            environment: vec![("ENV".into(), "prod".into())],
            volumes: vec!["/host/app:/app".into()],
            working_dir: Some("/app".into()),
            depends_on: vec![ServiceDependency {
                name: "db".into(),
                condition: crate::spec::DependencyCondition::ServiceStarted,
            }],
            readiness: Some(ReadinessSpec::Sleep { seconds: 1 }),
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement {
                nodes: 2,
                ntasks: Some(4),
                node_indices: Some(vec![0, 1]),
                ..ServicePlacement::default()
            },
            slurm: ServiceSlurmConfig {
                cpus_per_task: Some(2),
                ..ServiceSlurmConfig::default()
            },
            prepare: Some(prepare),
        };
        let plan = Plan {
            name: "demo".into(),
            project_dir: tmpdir.path().to_path_buf(),
            spec_path: tmpdir.path().join("compose.yaml"),
            runtime: RuntimeConfig::default(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig {
                time: Some("00:10:00".into()),
                ..SlurmConfig::default()
            },
            ordered_services: vec![planned.clone()],
        };

        let runtime_plan = build_runtime_plan(&plan);
        assert_eq!(runtime_plan.name, plan.name);
        assert_eq!(runtime_plan.cache_dir, plan.cache_dir);
        assert_eq!(runtime_plan.runtime.backend, plan.runtime.backend);
        assert_eq!(runtime_plan.slurm.time, plan.slurm.time);
        let runtime = runtime_plan.ordered_services.first().expect("service");
        assert_eq!(runtime.name, planned.name);
        assert_eq!(runtime.execution, planned.execution);
        assert_eq!(runtime.environment, planned.environment);
        assert_eq!(runtime.volumes, planned.volumes);
        assert_eq!(runtime.working_dir, planned.working_dir);
        assert_eq!(runtime.depends_on, planned.depends_on);
        assert_eq!(runtime.readiness, planned.readiness);
        assert_eq!(runtime.failure_policy, planned.failure_policy);
        assert_eq!(runtime.placement, planned.placement);
        assert_eq!(runtime.slurm.cpus_per_task, planned.slurm.cpus_per_task);
        assert_eq!(runtime.prepare, planned.prepare);
        assert_eq!(runtime.source, planned.image);
        assert!(
            runtime
                .runtime_image
                .starts_with(plan.cache_dir.join("prepared"))
        );
    }

    fn write_fake_enroot(tmpdir: &Path, log_path: &Path) -> PathBuf {
        let script = tmpdir.join("fake-enroot.sh");
        let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="$1"
shift
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name)
          name="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          image="$1"
          shift
          ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      exit 41
    fi
    ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output|--output=*)
          if [[ "$1" == *=* ]]; then
            output="${1#*=}"
            shift
          else
            output="$2"
            shift 2
          fi
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove)
    while (($#)); do
      case "$1" in
        -f|--force)
          shift
          ;;
        *)
          rm -rf "$ENROOT_DATA_PATH/$1"
          shift
          ;;
      esac
    done
    ;;
esac
"#;
        let content = template.replace(
            "__LOG_PATH__",
            &shell_quote_for_test(&log_path.display().to_string()),
        );
        fs::write(&script, content).expect("write fake enroot");
        let mut perms = fs::metadata(&script).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
        script
    }

    fn write_fake_sif_runtime(tmpdir: &Path, log_path: &Path) -> PathBuf {
        let script = tmpdir.join("fake-sif-runtime.sh");
        let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi
case "$cmd" in
  build)
    sandbox=0
    target=""
    while (($#)); do
      case "$1" in
        --sandbox)
          sandbox=1
          shift
          ;;
        --force|--fakeroot)
          shift
          ;;
        *)
          target="$1"
          break
          ;;
      esac
    done
    if [[ -z "$target" ]]; then
      echo "missing build target" >&2
      exit 64
    fi
    if (( sandbox )); then
      mkdir -p "$target"
    else
      mkdir -p "$(dirname "$target")"
      touch "$target"
    fi
    ;;
  exec)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      echo "prepare failed" >&2
      exit 41
    fi
    ;;
esac
"#;
        let content = template.replace(
            "__LOG_PATH__",
            &shell_quote_for_test(&log_path.display().to_string()),
        );
        fs::write(&script, content).expect("write fake sif runtime");
        let mut perms = fs::metadata(&script).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
        script
    }

    fn shell_quote_for_test(value: &str) -> String {
        let escaped = value.replace('\'', "'\"'\"'");
        format!("'{escaped}'")
    }

    #[test]
    fn prepare_pipeline_imports_and_exports() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![fake_service(tmpdir.path())],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        };

        let summary = prepare_runtime_plan(&plan, &options).expect("prepare");
        assert!(plan.ordered_services[0].runtime_image.exists());
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Built
        );
        let log_content = fs::read_to_string(log).expect("log");
        assert!(log_content.contains("import"));
        assert!(log_content.contains("create --force --name"));
        assert!(log_content.contains("export --force --output"));
        assert!(crate::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists());
    }

    #[test]
    fn cached_prepared_image_skips_rebuild_without_mounts() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let service = fake_service(tmpdir.path());
        let runtime_image = service.runtime_image.clone();
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        };

        prepare_runtime_plan(&plan, &options).expect("prepare once");
        fs::write(&log, "").expect("clear log");
        fs::write(&runtime_image, "cached").expect("seed");
        prepare_runtime_plan(&plan, &options).expect("prepare twice");
        let log_content = fs::read_to_string(log).expect("log");
        assert!(!log_content.contains("create --force"));
    }

    #[test]
    fn prepare_mounts_force_rebuild_even_with_existing_image() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let mut service = fake_service(tmpdir.path());
        service.prepare.as_mut().expect("prepare").mounts = vec!["/host:/mnt".into()];
        service.prepare.as_mut().expect("prepare").force_rebuild = true;
        fs::create_dir_all(service.runtime_image.parent().expect("parent")).expect("mkdir");
        fs::write(&service.runtime_image, "cached").expect("seed");

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        };

        prepare_runtime_plan(&plan, &options).expect("prepare");
        let log_content = fs::read_to_string(log).expect("log");
        assert!(log_content.contains("create --force --name"));
    }

    #[test]
    fn identical_remote_images_share_base_cache_path() {
        let service_a = RuntimeService {
            name: "a".into(),
            runtime_image: PathBuf::from("/tmp/a.sqsh"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::Remote("docker://redis:7".into()),
        };
        let service_b = RuntimeService {
            name: "b".into(),
            ..service_a.clone()
        };
        assert_eq!(
            base_image_path(Path::new("/shared/cache"), &service_a),
            base_image_path(Path::new("/shared/cache"), &service_b)
        );
    }

    #[test]
    fn sif_backends_use_sif_cache_paths_for_remote_images() {
        let service = RuntimeService {
            name: "app".into(),
            runtime_image: PathBuf::from("/tmp/app.sif"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::Remote("docker://ubuntu:24.04".into()),
        };

        let cache_dir = Path::new("/shared/cache");
        assert!(
            base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Apptainer)
                .display()
                .to_string()
                .ends_with(".sif")
        );
        assert!(
            base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Pyxis)
                .display()
                .to_string()
                .ends_with(".sqsh")
        );
    }

    #[test]
    fn failed_prepare_cleans_up_by_default() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let mut service = fake_service(tmpdir.path());
        service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let options = PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        };

        let err = prepare_runtime_plan(&plan, &options).expect_err("should fail");
        assert!(err.to_string().contains("prepare command"));
        let log_content = fs::read_to_string(log).expect("log");
        assert!(log_content.contains("remove --force"));
    }

    #[test]
    fn force_rebuild_option_rebuilds_prepared_images() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let service = fake_service(tmpdir.path());
        let runtime_image = service.runtime_image.clone();
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };

        prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect("prepare once");
        fs::write(&log, "").expect("clear log");
        fs::write(&runtime_image, "cached").expect("seed");

        let summary = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: true,
                ..PrepareOptions::default()
            },
        )
        .expect("prepare twice");
        let log_content = fs::read_to_string(log).expect("log");
        assert!(log_content.contains("create --force --name"));
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Built
        );
        assert_eq!(
            summary.services[0].runtime_image.note.as_deref(),
            Some("rebuilt because --force/--force-rebuild was requested")
        );
    }

    #[test]
    fn helper_defaults_and_paths_cover_remaining_prepare_helpers() {
        let defaults = PrepareOptions::default();
        assert_eq!(defaults.enroot_bin, "enroot");
        assert!(!defaults.keep_failed_prep);
        assert!(!defaults.force_rebuild);

        let cache_dir = Path::new("/shared/cache");
        let service = RuntimeService {
            name: "svc/name".into(),
            runtime_image: PathBuf::from("/tmp/runtime.sqsh"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::LocalSqsh(PathBuf::from("/tmp/local-image.sqsh")),
        };
        assert_eq!(base_image_cache_key(&service).len(), 64);
        assert!(temporary_rootfs_name(&service).starts_with("hpc-compose-svc_name-"));
        assert_eq!(short_hash("1234567890abcdef1234"), "1234567890abcdef");
        assert_eq!(sanitize_name("svc/name"), "svc_name");
        assert_eq!(image_label(&service.source), "local-image");
        let envs = enroot_env(cache_dir);
        assert_eq!(envs.len(), 2);
        assert!(envs[0].1.contains("enroot/cache"));
        assert!(!envs.iter().any(|(key, _)| key == "ENROOT_TEMP_PATH"));
    }

    #[test]
    fn prepare_runtime_plan_covers_local_missing_and_remote_without_prepare() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let local_present_path = tmpdir.path().join("present.sqsh");
        fs::write(&local_present_path, "x").expect("present local");
        let local_present = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache-local"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local-present".into(),
                runtime_image: local_present_path.clone(),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::LocalSqsh(local_present_path.clone()),
            }],
        };
        let local_summary = prepare_runtime_plan(
            &local_present,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect("local present");
        assert_eq!(
            local_summary.services[0].runtime_image.action,
            ArtifactAction::Present
        );
        assert_eq!(
            local_summary.services[0].runtime_image.note.as_deref(),
            Some("uses local .sqsh directly")
        );

        let local_missing = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local".into(),
                runtime_image: tmpdir.path().join("local.sqsh"),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
            }],
        };
        let err = prepare_runtime_plan(
            &local_missing,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect_err("local missing");
        assert!(err.to_string().contains("does not exist"));

        let remote_no_prepare = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache2"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "redis".into(),
                runtime_image: tmpdir.path().join("cache2/base/redis.sqsh"),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::Remote("docker://redis:7".into()),
            }],
        };
        let summary = prepare_runtime_plan(
            &remote_no_prepare,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect("remote no prepare");
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Built
        );
        assert_eq!(
            summary.services[0].runtime_image.note.as_deref(),
            Some("base cache artifact is used directly at runtime")
        );
    }

    #[test]
    fn local_sqsh_prepare_and_helper_failures_cover_remaining_branches() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);
        let local_base = tmpdir.path().join("local-base.sqsh");
        fs::write(&local_base, "x").expect("local base");

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local-prepared".into(),
                runtime_image: tmpdir.path().join("cache/prepared/local-prepared.sqsh"),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(PreparedImageSpec {
                    commands: vec!["echo local".into()],
                    mounts: vec!["/host:/mnt".into()],
                    env: vec![("KEY".into(), "VALUE".into())],
                    root: false,
                    force_rebuild: false,
                }),
                source: ImageSource::LocalSqsh(local_base),
            }],
        };
        let summary = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: false,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect("local prepare");
        assert!(summary.services[0].base_image.is_none());
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Built
        );
        let log_content = fs::read_to_string(&log).expect("log");
        assert!(!log_content.contains("import"));
        assert!(log_content.contains("--mount /host:/mnt"));
        assert!(!log_content.contains("start --root --rw"));

        let err = ensure_parent_dir(Path::new("/")).expect_err("root has no parent");
        assert!(err.to_string().contains("does not have a parent directory"));

        let err = run_enroot(
            "/definitely/missing/enroot",
            &[],
            vec!["version".to_string()],
            "probe missing binary",
        )
        .expect_err("missing binary execution");
        assert!(err.to_string().contains("failed to execute"));
    }

    #[test]
    fn keep_failed_prep_and_binary_errors_cover_failure_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("enroot.log");
        let fake = write_fake_enroot(tmpdir.path(), &log);

        let mut service = fake_service(tmpdir.path());
        service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service],
        };
        let err = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                enroot_bin: fake.display().to_string(),
                keep_failed_prep: true,
                force_rebuild: false,
                ..PrepareOptions::default()
            },
        )
        .expect_err("should fail");
        assert!(err.to_string().contains("prepare command"));
        let log_content = fs::read_to_string(log).expect("log");
        let remove_count = log_content.matches("remove --force").count();
        assert_eq!(remove_count, 1);

        let err = ensure_binary_available("/definitely/missing/enroot", "missing")
            .expect_err("missing binary");
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn helper_paths_binary_search_and_run_failures_are_reported() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache_dir = tmpdir.path().join("cache");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: cache_dir.clone(),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: Vec::new(),
        };
        create_cache_dirs(&plan).expect("create cache dirs");
        for suffix in [
            "base",
            "prepared",
            "enroot/cache",
            "enroot/data",
            "enroot/tmp",
        ] {
            assert!(cache_dir.join(suffix).exists(), "{suffix} missing");
        }

        let _guard = env_lock().lock().expect("env lock");
        let bin_dir = tmpdir.path().join("bin");
        fs::create_dir_all(&bin_dir).expect("bin dir");
        let helper = bin_dir.join("enroot-ok");
        fs::write(
            &helper,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == fail ]]; then echo boom >&2; exit 7; fi\nexit 0\n",
        )
        .expect("helper");
        let mut perms = fs::metadata(&helper).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&helper, perms).expect("chmod");
        let old_path = env::var_os("PATH");
        let joined = env::join_paths(
            std::iter::once(bin_dir.clone())
                .chain(old_path.as_ref().into_iter().flat_map(env::split_paths)),
        )
        .expect("join path");
        unsafe {
            env::set_var("PATH", joined);
        }
        ensure_binary_available("enroot-ok", "missing in path").expect("binary on path");
        let err = run_enroot(
            helper.to_str().expect("helper"),
            &[],
            vec!["fail".to_string()],
            "run failing command",
        )
        .expect_err("failing helper");
        assert!(
            err.to_string()
                .contains("failed to run failing command: boom")
        );
        match old_path {
            Some(value) => unsafe {
                env::set_var("PATH", value);
            },
            None => unsafe {
                env::remove_var("PATH");
            },
        }
    }

    #[test]
    fn sif_remote_base_builds_reuses_and_writes_manifest() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("sif-runtime.log");
        let fake = write_fake_sif_runtime(tmpdir.path(), &log);
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let plan = Plan {
            name: "demo".into(),
            project_dir: tmpdir.path().to_path_buf(),
            spec_path: compose,
            cache_dir: tmpdir.path().join("cache"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Apptainer,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![PlannedService {
                name: "app".into(),
                image: ImageSource::Remote("docker://example.com/app:1".into()),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
            }],
        };
        let runtime_plan = build_runtime_plan(&plan);
        let options = PrepareOptions {
            apptainer_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        };

        let first = prepare_runtime_plan(&runtime_plan, &options).expect("first prepare");
        assert_eq!(
            first.services[0].base_image.as_ref().expect("base").action,
            ArtifactAction::Built
        );
        assert_eq!(
            first.services[0].runtime_image.note.as_deref(),
            Some("base SIF cache artifact is used directly at runtime")
        );
        assert!(runtime_plan.ordered_services[0].runtime_image.exists());
        let manifest = crate::cache::read_manifest(&runtime_plan.ordered_services[0].runtime_image)
            .expect("base manifest");
        assert_eq!(manifest.kind, crate::cache::CacheEntryKind::Base);
        assert!(
            fs::read_to_string(&log)
                .expect("log")
                .contains("docker://example.com/app:1")
        );

        fs::write(&log, "").expect("clear log");
        let second = prepare_runtime_plan(&runtime_plan, &options).expect("second prepare");
        assert_eq!(
            second.services[0].base_image.as_ref().expect("base").action,
            ArtifactAction::Reused
        );
        assert!(
            !fs::read_to_string(&log)
                .expect("log")
                .contains("build --force")
        );
    }

    #[test]
    fn sif_local_images_are_validated_for_sif_backends() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("sif-runtime.log");
        let fake = write_fake_sif_runtime(tmpdir.path(), &log);
        let local_sif = tmpdir.path().join("local.sif");
        fs::write(&local_sif, "sif").expect("local sif");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        fs::write(&local_sqsh, "sqsh").expect("local sqsh");

        let present = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache-present"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Singularity,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "local-sif".into(),
                runtime_image: local_sif.clone(),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::LocalSif(local_sif.clone()),
            }],
        };
        let summary = prepare_runtime_plan(
            &present,
            &PrepareOptions {
                singularity_bin: fake.display().to_string(),
                ..PrepareOptions::default()
            },
        )
        .expect("local sif present");
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Present
        );
        assert_eq!(
            summary.services[0].runtime_image.note.as_deref(),
            Some("uses local .sif directly")
        );

        let missing = RuntimePlan {
            cache_dir: tmpdir.path().join("cache-missing"),
            ordered_services: vec![RuntimeService {
                name: "missing-sif".into(),
                runtime_image: tmpdir.path().join("missing.sif"),
                source: ImageSource::LocalSif(tmpdir.path().join("missing.sif")),
                ..present.ordered_services[0].clone()
            }],
            ..present.clone()
        };
        let err = prepare_runtime_plan(
            &missing,
            &PrepareOptions {
                singularity_bin: fake.display().to_string(),
                ..PrepareOptions::default()
            },
        )
        .expect_err("missing local sif");
        assert!(err.to_string().contains("does not exist"));

        let wrong_format = RuntimePlan {
            cache_dir: tmpdir.path().join("cache-sqsh"),
            ordered_services: vec![RuntimeService {
                name: "local-sqsh".into(),
                runtime_image: local_sqsh.clone(),
                source: ImageSource::LocalSqsh(local_sqsh),
                ..present.ordered_services[0].clone()
            }],
            ..present
        };
        let err = prepare_runtime_plan(
            &wrong_format,
            &PrepareOptions {
                singularity_bin: fake.display().to_string(),
                ..PrepareOptions::default()
            },
        )
        .expect_err("sqsh rejected by sif backend");
        assert!(err.to_string().contains("requires SIF images"));
    }

    #[test]
    fn sif_prepare_sequence_uses_sandbox_flags_and_backend_cache_key() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("sif-runtime.log");
        let fake = write_fake_sif_runtime(tmpdir.path(), &log);
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let prepare = PreparedImageSpec {
            commands: vec!["echo setup".into()],
            mounts: vec!["/host:/mnt".into()],
            env: vec![("KEY".into(), "VALUE".into())],
            root: true,
            force_rebuild: false,
        };
        let plan = Plan {
            name: "demo".into(),
            project_dir: tmpdir.path().to_path_buf(),
            spec_path: compose,
            cache_dir: tmpdir.path().join("cache"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Apptainer,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![PlannedService {
                name: "prepared-sif".into(),
                image: ImageSource::Remote("docker://example.com/prepared:1".into()),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(prepare.clone()),
            }],
        };
        let runtime_plan = build_runtime_plan(&plan);
        let service = &runtime_plan.ordered_services[0];

        let summary = prepare_runtime_plan(
            &runtime_plan,
            &PrepareOptions {
                apptainer_bin: fake.display().to_string(),
                ..PrepareOptions::default()
            },
        )
        .expect("sif prepare");
        assert_eq!(
            summary.services[0].runtime_image.action,
            ArtifactAction::Built
        );
        assert!(service.runtime_image.exists());

        let log_content = fs::read_to_string(&log).expect("log");
        assert!(log_content.contains("build --force --sandbox --fakeroot"));
        assert!(log_content.contains("exec --writable --fakeroot"));
        assert!(log_content.contains("--bind /host:/mnt"));
        assert!(log_content.contains("--env KEY=VALUE"));
        assert!(log_content.contains(&service.runtime_image.display().to_string()));
        assert!(
            !fs::read_dir(runtime_plan.cache_dir.join("prepared"))
                .expect("prepared dir")
                .any(|entry| entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".sandbox"))
        );

        let manifest = crate::cache::read_manifest(&service.runtime_image).expect("manifest");
        let expected_from_plan = prepared_image_cache_key_from_plan(
            &plan.ordered_services[0],
            &prepare,
            RuntimeBackend::Apptainer,
        );
        assert_eq!(manifest.cache_key, expected_from_plan);
        assert_eq!(
            manifest.cache_key,
            prepared_image_cache_key(service, &prepare, RuntimeBackend::Apptainer)
        );
    }

    #[test]
    fn failed_sif_prepare_cleanup_respects_keep_failed_prep() {
        for (keep_failed_prep, should_keep_sandbox) in [(false, false), (true, true)] {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let log = tmpdir.path().join("sif-runtime.log");
            let fake = write_fake_sif_runtime(tmpdir.path(), &log);
            let local_sif = tmpdir.path().join("base.sif");
            fs::write(&local_sif, "sif").expect("local sif");
            let plan = RuntimePlan {
                name: "demo".into(),
                cache_dir: tmpdir.path().join("cache"),
                runtime: RuntimeConfig {
                    backend: RuntimeBackend::Apptainer,
                    ..RuntimeConfig::default()
                },
                slurm: SlurmConfig::default(),
                ordered_services: vec![RuntimeService {
                    name: "bad-prepare".into(),
                    runtime_image: tmpdir.path().join("cache/prepared/bad-prepare.sif"),
                    execution: ExecutionSpec::ImageDefault,
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: Some(PreparedImageSpec {
                        commands: vec!["fail-me".into()],
                        mounts: Vec::new(),
                        env: Vec::new(),
                        root: false,
                        force_rebuild: false,
                    }),
                    source: ImageSource::LocalSif(local_sif),
                }],
            };
            let err = prepare_runtime_plan(
                &plan,
                &PrepareOptions {
                    apptainer_bin: fake.display().to_string(),
                    keep_failed_prep,
                    ..PrepareOptions::default()
                },
            )
            .expect_err("prepare failure");
            assert!(err.to_string().contains("run prepare command"));
            let sandbox_left = fs::read_dir(plan.cache_dir.join("prepared"))
                .expect("prepared dir")
                .any(|entry| {
                    entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".sandbox")
                });
            assert_eq!(sandbox_left, should_keep_sandbox);
        }
    }

    #[test]
    fn runtime_path_and_command_helpers_cover_remaining_branches() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let plan = Plan {
            name: "demo".into(),
            project_dir: tmpdir.path().to_path_buf(),
            spec_path: compose,
            cache_dir: tmpdir.path().join("cache"),
            runtime: RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: vec![
                PlannedService {
                    name: "local".into(),
                    image: ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh")),
                    execution: ExecutionSpec::ImageDefault,
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: None,
                },
                PlannedService {
                    name: "prepared".into(),
                    image: ImageSource::LocalSqsh(PathBuf::from("/tmp/base.sqsh")),
                    execution: ExecutionSpec::ImageDefault,
                    environment: Vec::new(),
                    volumes: Vec::new(),
                    working_dir: None,
                    depends_on: Vec::new(),
                    readiness: None,
                    failure_policy: ServiceFailurePolicy::default(),
                    placement: ServicePlacement::default(),
                    slurm: ServiceSlurmConfig::default(),
                    prepare: Some(PreparedImageSpec {
                        commands: vec!["echo hi".into()],
                        mounts: Vec::new(),
                        env: Vec::new(),
                        root: true,
                        force_rebuild: false,
                    }),
                },
            ],
        };
        let runtime = build_runtime_plan(&plan);
        assert_eq!(
            runtime.ordered_services[0].runtime_image,
            PathBuf::from("/tmp/local.sqsh")
        );
        assert!(
            runtime.ordered_services[1]
                .runtime_image
                .display()
                .to_string()
                .contains("/prepared/")
        );
        assert!(
            prepared_image_cache_key_from_plan(
                &plan.ordered_services[1],
                plan.ordered_services[1].prepare.as_ref().expect("prepare"),
                plan.runtime.backend
            )
            .len()
                > 10
        );
        assert!(
            prepared_image_cache_key(
                &runtime.ordered_services[1],
                runtime.ordered_services[1]
                    .prepare
                    .as_ref()
                    .expect("prepare"),
                plan.runtime.backend
            )
            .len()
                > 10
        );
    }
}
