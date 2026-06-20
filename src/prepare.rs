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
    ReadinessSpec, RuntimeBackend, RuntimeConfig, ServiceAssertSpec, ServiceDependency,
    ServiceFailurePolicy, ServiceSlurmConfig, SlurmConfig,
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
    pub assertions: Option<ServiceAssertSpec>,
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
        "Enroot is required for up/run; install it or pass a valid enroot binary path",
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
        // `create_cache_dirs` already provisions `enroot/tmp`; point enroot at it
        // so prepare-time scratch lands on the (shared, quota-managed) cache
        // filesystem instead of the node's default `/tmp`.
        (
            "ENROOT_TEMP_PATH".to_string(),
            cache_dir.join("enroot/tmp").display().to_string(),
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
mod tests;
