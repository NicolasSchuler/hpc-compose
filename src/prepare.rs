//! Runtime artifact preparation and runtime-plan derivation.

use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    /// `huggingface-cli` used by `hf://` stage-in. Prepare does not download hf
    /// artifacts (that is a cluster-side render step); the name only flows on to
    /// render and doctor advisories.
    pub huggingface_cli_bin: String,
    pub keep_failed_prep: bool,
    pub force_rebuild: bool,
    /// Override for enroot's temporary extraction scratch directory
    /// (`ENROOT_TEMP_PATH`) sourced from settings (`cache.enroot_temp_dir`).
    /// Lower precedence than the compose `x-slurm.enroot_temp_dir` and the
    /// `HPC_COMPOSE_ENROOT_TEMP_DIR` env override. `None` keeps the default.
    pub enroot_temp_dir: Option<String>,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self {
            enroot_bin: "enroot".to_string(),
            apptainer_bin: "apptainer".to_string(),
            singularity_bin: "singularity".to_string(),
            huggingface_cli_bin: "huggingface-cli".to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            enroot_temp_dir: None,
        }
    }
}

/// Environment variable that overrides enroot's prepare-time temporary
/// extraction scratch directory. Highest precedence (above the compose spec and
/// settings). Useful when running prepare directly on a login node whose shared
/// filesystem is prone to `Stale file handle` errors during squashfs creation.
pub const ENROOT_TEMP_DIR_ENV: &str = "HPC_COMPOSE_ENROOT_TEMP_DIR";

/// Environment variable that opts prepare-time image building back into the
/// enroot NVIDIA GPU hook. By default the hook is disabled during prepare
/// (`NVIDIA_VISIBLE_DEVICES=void`) because prepare runs on a login node that
/// usually has no NVIDIA driver, so a CUDA image whose baked
/// `NVIDIA_VISIBLE_DEVICES=all` would otherwise make the hook fail. GPUs are
/// injected at Slurm/Pyxis runtime instead. Set to `1`/`true`/`yes` to keep the
/// hook enabled during prepare (rarely needed).
pub const PREPARE_GPU_ENV: &str = "HPC_COMPOSE_PREPARE_GPU";

/// Whether the enroot NVIDIA hook should run during prepare. Default `false`
/// (hook disabled) unless [`PREPARE_GPU_ENV`] opts in.
fn prepare_gpu_enabled() -> bool {
    gpu_flag_enabled(std::env::var(PREPARE_GPU_ENV).ok().as_deref())
}

/// Pure truthiness parse for [`PREPARE_GPU_ENV`], split out so the
/// accepted-values contract can be unit-tested without touching process env.
pub(crate) fn gpu_flag_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// The value forced into `NVIDIA_VISIBLE_DEVICES` to make the enroot NVIDIA hook
/// a no-op (CPU-only prepare on the login node).
const NVIDIA_HOOK_DISABLED: &str = "void";

/// Environment variable that streams the underlying tool's raw output (enroot,
/// apptainer) straight through to this process's stdout/stderr during prepare,
/// instead of summarizing the latest line in a progress bar. Useful for
/// debugging slow or stuck image imports. Set to `1`/`true`/`yes`/`on`.
///
/// Because the tool's output is inherited (not captured) in this mode, the
/// stale-file-handle auto-retry and its remediation hint are skipped — the raw
/// error is shown directly instead. Use the default (non-verbose) mode to keep
/// the automatic ESTALE retry.
pub const PREPARE_VERBOSE_ENV: &str = "HPC_COMPOSE_PREPARE_VERBOSE";

/// Whether to pass subprocess output through verbatim (see [`PREPARE_VERBOSE_ENV`]).
pub fn prepare_verbose_enabled() -> bool {
    matches!(
        std::env::var(PREPARE_VERBOSE_ENV)
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Observer for prepare-time sub-progress. The library emits coarse phase
/// transitions and forwards the underlying tool's live output; the binary
/// renders them (e.g. as spinner bars). All methods default to no-ops so
/// non-interactive callers need no implementation. Reporters are invoked on the
/// calling thread, so implementors need not be `Send`/`Sync`.
#[allow(unused_variables)]
pub trait PrepareReporter {
    /// A new prepare phase started for `service` (e.g. `importing docker://...`).
    fn step_started(&self, service: &str, phase: &str) {}
    /// A live output line from the underlying tool (enroot/apptainer) for
    /// `service`. Lines are already trimmed of trailing whitespace.
    fn step_output(&self, service: &str, line: &str) {}
    /// Best-effort byte count written to the current target artifact, polled
    /// while the tool runs. May stay at 0 for tools that write via temp+rename.
    fn step_bytes(&self, service: &str, bytes: u64) {}
}

/// A [`PrepareReporter`] that ignores every event. Use for non-interactive
/// callers (smoke tests, `--format json`, CI).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPrepareReporter;

impl PrepareReporter for NoopPrepareReporter {}

/// Live-progress context for a single subprocess invocation.
struct StreamCtx<'a> {
    reporter: &'a dyn PrepareReporter,
    service: &'a str,
    phase: &'a str,
    /// Artifact whose growing size is polled for best-effort byte progress.
    target: Option<&'a Path>,
}

impl<'a> StreamCtx<'a> {
    /// A context that suppresses live progress (for fast/cleanup steps).
    fn quiet(reporter: &'a dyn PrepareReporter, service: &'a str) -> Self {
        Self {
            reporter,
            service,
            phase: "",
            target: None,
        }
    }
}

/// Resolves enroot's prepare-time temporary scratch directory.
///
/// Precedence (highest first): the `HPC_COMPOSE_ENROOT_TEMP_DIR` env value, the
/// compose `x-slurm.enroot_temp_dir`, the settings `cache.enroot_temp_dir`, then
/// the historical default `<cache_dir>/enroot/tmp`. Picked values may contain
/// `~` and `$VAR` references, which are expanded.
#[must_use]
pub fn resolve_enroot_temp_dir(
    env_value: Option<&str>,
    spec_value: Option<&str>,
    settings_value: Option<&str>,
    cache_dir: &Path,
) -> PathBuf {
    let chosen = [env_value, spec_value, settings_value]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty());
    match chosen {
        Some(value) => PathBuf::from(
            shellexpand::full(value)
                .map(|expanded| expanded.into_owned())
                .unwrap_or_else(|_| value.to_string()),
        ),
        None => cache_dir.join("enroot/tmp"),
    }
}

/// The hpc-compose-owned, per-process scratch subdirectory inside the resolved
/// temp dir, used as the actual `ENROOT_TEMP_PATH`. Keeping enroot's scratch in a
/// directory we exclusively own means the stale-handle retry's cleanup never
/// deletes the user's other files (the resolved dir may be a shared location
/// such as `/tmp`) or a concurrent run's in-flight extraction.
fn enroot_scratch_dir(resolved_temp_dir: &Path) -> PathBuf {
    resolved_temp_dir.join(format!("hpc-compose-enroot-{}", std::process::id()))
}

/// The prepare-time `ENROOT_DATA_PATH` — where `enroot create` unsquashes the
/// transient prepare rootfs. When the extraction scratch has been redirected off
/// the shared cache (the default `<cache_dir>/enroot/tmp`), the user opted into
/// node-local storage to avoid slow/ESTALE-prone shared-FS extraction, so the
/// rootfs is placed node-local alongside the scratch (in an hpc-compose-owned
/// per-process subdir) and the `unsquashfs` is fast. Otherwise it stays on the
/// persistent shared cache (`<cache_dir>/enroot/data`). The layer cache
/// (`ENROOT_CACHE_PATH`) and the exported `.sqsh` always remain on the shared cache.
fn enroot_data_dir(resolved_temp_dir: &Path, cache_dir: &Path) -> PathBuf {
    if resolved_temp_dir == cache_dir.join("enroot/tmp") {
        cache_dir.join("enroot/data")
    } else {
        resolved_temp_dir.join(format!("hpc-compose-enroot-data-{}", std::process::id()))
    }
}

/// True when an enroot/mksquashfs failure looks like a stale-NFS-handle or
/// squashfs-read error on the temporary extraction filesystem — the signature
/// of a shared filesystem that cannot sustain the extract-then-scan workload.
fn is_stale_handle_error(err: &anyhow::Error) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("stale file handle")
        || text.contains("read failed because")
        || (text.contains("squashfs") && text.contains("read failed"))
}

/// True when an import failure looks like the registry rejecting the reference —
/// the image tag does not exist or the pull is unauthorized, rather than a
/// filesystem problem. This is the confusing case where a typo'd or non-existent
/// tag only surfaces deep inside `enroot import`.
fn is_missing_image_error(err: &anyhow::Error) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    (text.contains("manifest") && (text.contains("unknown") || text.contains("not found")))
        || text.contains("401 unauthorized")
        || text.contains("access to the resource is denied")
}

/// Appends targeted remediation to an import failure: either a stale-handle issue
/// on the extraction filesystem, or a registry reference that could not be pulled.
fn enrich_import_error(err: anyhow::Error, temp_dir: &Path) -> anyhow::Error {
    if is_stale_handle_error(&err) {
        return err.context(format!(
            "enroot's temporary extraction directory ({}) is on a filesystem that cannot sustain \
             image extraction (stale file handle / squashfs read error). Point it at fast \
             node-local storage: set x-slurm.enroot_temp_dir (compose), cache.enroot_temp_dir \
             (settings), or {ENROOT_TEMP_DIR_ENV} to e.g. /tmp/$USER-hpc-compose-enroot. The \
             final image and layer cache stay on the shared cache. Also remove any stale temp \
             dirs left by interrupted imports.",
            temp_dir.display()
        ));
    }
    if is_missing_image_error(&err) {
        return err.context(
            "the container image could not be pulled — the tag may not exist or the registry \
             requires authentication. Verify the reference exists before submitting, e.g. \
             `skopeo inspect docker://<image>` or `docker manifest inspect <image>`. \
             `hpc-compose lint` (HPC007) flags mutable/`latest` tags but cannot confirm a tag \
             actually exists on the registry.",
        );
    }
    err
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
///
/// Equivalent to [`prepare_runtime_plan_with_reporter`] with a
/// [`NoopPrepareReporter`]; kept for callers that do not render sub-progress.
pub fn prepare_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
) -> Result<PrepareSummary> {
    prepare_runtime_plan_with_reporter(plan, options, &NoopPrepareReporter)
}

/// Imports and prepares any missing runtime artifacts, forwarding live
/// sub-progress (image import phases, tool output, byte counts) to `reporter`.
pub fn prepare_runtime_plan_with_reporter(
    plan: &RuntimePlan,
    options: &PrepareOptions,
    reporter: &dyn PrepareReporter,
) -> Result<PrepareSummary> {
    match plan.runtime.backend {
        RuntimeBackend::Pyxis => prepare_pyxis_runtime_plan(plan, options, reporter),
        RuntimeBackend::Apptainer | RuntimeBackend::Singularity => {
            prepare_sif_runtime_plan(plan, options, reporter)
        }
        RuntimeBackend::Host => prepare_host_runtime_plan(plan),
    }
}

fn prepare_pyxis_runtime_plan(
    plan: &RuntimePlan,
    options: &PrepareOptions,
    reporter: &dyn PrepareReporter,
) -> Result<PrepareSummary> {
    ensure_binary_available(
        &options.enroot_bin,
        "Enroot is required for up/run; install it or pass a valid enroot binary path",
    )?;
    // Use an hpc-compose-owned, per-process subdirectory of the resolved temp
    // dir as ENROOT_TEMP_PATH so the stale-handle retry can clean it without ever
    // touching the user's other files (the resolved dir may be a shared location
    // like /tmp) or a concurrent run's in-flight extraction.
    let resolved_temp = resolved_enroot_temp_dir(plan, options);
    let temp_dir = enroot_scratch_dir(&resolved_temp);
    // When the extraction scratch is redirected node-local (to dodge slow/ESTALE
    // shared-FS extraction), put the transient prepare rootfs (ENROOT_DATA_PATH,
    // where `enroot create` unsquashes the image) node-local alongside it so the
    // `unsquashfs` is fast too. The persistent layer cache stays on the shared cache.
    let data_dir = enroot_data_dir(&resolved_temp, &plan.cache_dir);
    let envs = enroot_env(&plan.cache_dir, &data_dir, &temp_dir, !prepare_gpu_enabled());
    create_cache_dirs(plan)?;
    ensure_dir(&temp_dir).with_context(|| {
        format!(
            "failed to create enroot temporary scratch directory {}",
            temp_dir.display()
        )
    })?;
    ensure_dir(&data_dir).with_context(|| {
        format!(
            "failed to create enroot prepare rootfs directory {}",
            data_dir.display()
        )
    })?;
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
                    import_base_image(
                        &options.enroot_bin,
                        &envs,
                        remote,
                        &base_path,
                        &temp_dir,
                        &service.name,
                        reporter,
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
            prepare_service_image(service, prepare, &plan.cache_dir, options, &envs, reporter)?;
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
    reporter: &dyn PrepareReporter,
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
                        &StreamCtx {
                            reporter,
                            service: &service.name,
                            phase: &format!(
                                "building {remote} (first build may take several minutes)"
                            ),
                            target: Some(&base_path),
                        },
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
                reporter,
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
    reporter: &dyn PrepareReporter,
) -> Result<()> {
    let rootfs_name = temporary_rootfs_name(service);
    let base_image = match &service.source {
        ImageSource::LocalSqsh(path) => path.clone(),
        ImageSource::Remote(_) => base_image_path(cache_dir, service),
        ImageSource::LocalSif(_) | ImageSource::Host => unreachable!("validated by backend"),
    };

    let cleanup_result = run_prepare_sequence(
        service,
        prepare,
        &rootfs_name,
        &base_image,
        options,
        envs,
        reporter,
    );

    match cleanup_result {
        Ok(()) => {
            remove_rootfs(
                &options.enroot_bin,
                envs,
                &rootfs_name,
                reporter,
                &service.name,
            )?;
            Ok(())
        }
        Err(err) => {
            if !options.keep_failed_prep {
                let _ = remove_rootfs(
                    &options.enroot_bin,
                    envs,
                    &rootfs_name,
                    reporter,
                    &service.name,
                );
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
    reporter: &dyn PrepareReporter,
) -> Result<()> {
    let _ = remove_rootfs(
        &options.enroot_bin,
        envs,
        rootfs_name,
        reporter,
        &service.name,
    );

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
        &StreamCtx {
            reporter,
            service: &service.name,
            phase: "creating prepare rootfs (unsquashing the image may take a minute)",
            target: None,
        },
    )?;

    let disable_nvidia_hook = !prepare_gpu_enabled();
    for (index, command) in prepare.commands.iter().enumerate() {
        let mut args = vec!["start".to_string()];
        if prepare.root {
            args.push("--root".to_string());
        }
        args.push("--rw".to_string());
        for mount in &prepare.mounts {
            args.push("--mount".to_string());
            args.push(mount.clone());
        }
        // Disable the NVIDIA hook for CPU-only prepare on the login node (before
        // the user's prepare.env, so an explicit override there still wins).
        if disable_nvidia_hook {
            args.push("--env".to_string());
            args.push(format!("NVIDIA_VISIBLE_DEVICES={NVIDIA_HOOK_DISABLED}"));
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
            &StreamCtx {
                reporter,
                service: &service.name,
                phase: &format!(
                    "running prepare step {}/{}",
                    index + 1,
                    prepare.commands.len()
                ),
                target: None,
            },
        )
        .map_err(|err| enrich_prepare_mount_error(err, prepare, &service.name))?;
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
        &StreamCtx {
            reporter,
            service: &service.name,
            phase: "exporting prepared image",
            target: Some(&service.runtime_image),
        },
    )?;

    Ok(())
}

fn remove_rootfs(
    enroot_bin: &str,
    envs: &[(String, String)],
    rootfs_name: &str,
    reporter: &dyn PrepareReporter,
    service: &str,
) -> Result<()> {
    run_enroot(
        enroot_bin,
        envs,
        vec![
            "remove".to_string(),
            "--force".to_string(),
            rootfs_name.to_string(),
        ],
        "remove temporary prepare rootfs",
        &StreamCtx::quiet(reporter, service),
    )
}

fn run_enroot<I>(
    enroot_bin: &str,
    envs: &[(String, String)],
    args: I,
    context: &str,
    stream: &StreamCtx<'_>,
) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args_vec = args.into_iter().collect::<Vec<_>>();
    let mut command = Command::new(enroot_bin);
    command.args(&args_vec);
    command.envs(envs.iter().map(|(k, v)| (k, v)));
    run_streamed_command(command, enroot_bin, context, stream)
}

fn prepare_service_sif(
    service: &RuntimeService,
    prepare: &PreparedImageSpec,
    cache_dir: &Path,
    backend: RuntimeBackend,
    runtime_bin: &str,
    options: &PrepareOptions,
    reporter: &dyn PrepareReporter,
) -> Result<()> {
    let sandbox = temporary_sandbox_path(cache_dir, service);
    let base_image = match &service.source {
        ImageSource::LocalSif(path) => path.clone(),
        ImageSource::Remote(_) => base_image_path_for_backend(cache_dir, service, backend),
        ImageSource::LocalSqsh(_) | ImageSource::Host => unreachable!("validated by backend"),
    };

    let cleanup_result = run_sif_prepare_sequence(
        service,
        prepare,
        &sandbox,
        &base_image,
        runtime_bin,
        reporter,
    );
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
    reporter: &dyn PrepareReporter,
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
        &StreamCtx {
            reporter,
            service: &service.name,
            phase: "creating prepare sandbox",
            target: None,
        },
    )?;

    for (index, command) in prepare.commands.iter().enumerate() {
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
            &StreamCtx {
                reporter,
                service: &service.name,
                phase: &format!(
                    "running prepare step {}/{}",
                    index + 1,
                    prepare.commands.len()
                ),
                target: None,
            },
        )
        .map_err(|err| enrich_prepare_mount_error(err, prepare, &service.name))?;
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
        &StreamCtx {
            reporter,
            service: &service.name,
            phase: "exporting prepared SIF",
            target: Some(&service.runtime_image),
        },
    )
}

fn run_container_runtime<I>(
    runtime_bin: &str,
    args: I,
    context: &str,
    stream: &StreamCtx<'_>,
) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args_vec = args.into_iter().collect::<Vec<_>>();
    let mut command = Command::new(runtime_bin);
    command.args(&args_vec);
    run_streamed_command(command, runtime_bin, context, stream)
}

/// Drains a byte stream line by line, decoding lossily so non-UTF-8 output never
/// terminates the reader early. Reading the pipe to EOF regardless of byte
/// content keeps the child from getting EPIPE/SIGPIPE on its next write and
/// preserves the complete stderr tail. Each line is yielded with its trailing
/// newline (and `\r`) stripped.
fn for_each_line_lossy<R: BufRead>(mut reader: R, mut on_line: impl FnMut(String)) {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => break,
            Ok(_) => {
                while matches!(buf.last(), Some(b'\n' | b'\r')) {
                    buf.pop();
                }
                on_line(String::from_utf8_lossy(&buf).into_owned());
            }
            Err(_) => break,
        }
    }
}

/// Runs a prepare subprocess, forwarding its live stdout/stderr to the
/// reporter and best-effort byte progress of the target artifact, while
/// preserving the buffered stderr tail for the failure message.
///
/// Both pipes are drained on dedicated threads to avoid pipe-buffer deadlock on
/// chatty tools (enroot import prints a lot); the reporter is only ever called
/// on this (the calling) thread, so it need not be `Send`.
fn run_streamed_command(
    mut command: Command,
    bin: &str,
    context: &str,
    stream: &StreamCtx<'_>,
) -> Result<()> {
    if !stream.phase.is_empty() {
        stream.reporter.step_started(stream.service, stream.phase);
    }
    // Verbose mode: hand the tool this process's stdout/stderr so its raw output
    // (including carriage-return progress bars) streams straight through.
    if prepare_verbose_enabled() {
        command.stdin(Stdio::null());
        let status = command
            .status()
            .with_context(|| format!("failed to execute '{bin}' while trying to {context}"))?;
        if !status.success() {
            bail!("failed to {} (see the streamed output above)", context);
        }
        return Ok(());
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to execute '{bin}' while trying to {context}"))?;

    let (tx, rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let tx_out = tx.clone();
    let stdout_handle = stdout.map(|pipe| {
        thread::spawn(move || {
            for_each_line_lossy(BufReader::new(pipe), |line| {
                let _ = tx_out.send(line);
            });
        })
    });
    let stderr_handle = stderr.map(|pipe| {
        thread::spawn(move || {
            let mut buffer = Vec::new();
            for_each_line_lossy(BufReader::new(pipe), |line| {
                buffer.push(line.clone());
                let _ = tx.send(line);
            });
            buffer
        })
    });
    // If a pipe was unexpectedly absent, make sure the sender side is dropped so
    // the channel can close (the moved `tx`/`tx_out` are otherwise owned by the
    // threads). Both pipes are piped above, so in practice both threads run.
    if stdout_handle.is_none() && stderr_handle.is_none() {
        // Nothing streams; fall through and just wait.
    }

    let forward = |line: &str, stream: &StreamCtx<'_>| {
        let trimmed = line.trim_end();
        if !trimmed.is_empty() && !stream.phase.is_empty() {
            stream.reporter.step_output(stream.service, trimmed);
        }
    };

    let mut last_bytes = 0u64;
    let status = loop {
        while let Ok(line) = rx.try_recv() {
            forward(&line, stream);
        }
        if let Some(target) = stream.target
            && let Ok(meta) = fs::metadata(target)
        {
            let len = meta.len();
            if len != last_bytes {
                last_bytes = len;
                stream.reporter.step_bytes(stream.service, len);
            }
        }
        match child
            .try_wait()
            .context("failed to poll prepare subprocess")?
        {
            Some(status) => break status,
            None => thread::sleep(Duration::from_millis(200)),
        }
    };

    // Child exited; let the reader threads reach EOF, then forward any tail.
    if let Some(handle) = stdout_handle {
        let _ = handle.join();
    }
    let stderr_buffer = stderr_handle
        .and_then(|handle| handle.join().ok())
        .unwrap_or_default();
    while let Ok(line) = rx.try_recv() {
        forward(&line, stream);
    }

    if !status.success() {
        bail!("failed to {}: {}", context, stderr_buffer.join("\n").trim());
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
        plan.cache_dir.join("datasets"),
        plan.cache_dir.join("models"),
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

fn enroot_env(
    cache_dir: &Path,
    data_dir: &Path,
    temp_dir: &Path,
    disable_nvidia_hook: bool,
) -> Vec<(String, String)> {
    let mut env = vec![
        // The persistent layer cache stays on the (shared, quota-managed) cache
        // filesystem so repeated imports are fast.
        (
            "ENROOT_CACHE_PATH".to_string(),
            cache_dir.join("enroot/cache").display().to_string(),
        ),
        // The container data path (where `enroot create` unsquashes the transient
        // prepare rootfs) follows the extraction scratch: shared cache by default,
        // node-local when the scratch is redirected (see `enroot_data_dir`).
        (
            "ENROOT_DATA_PATH".to_string(),
            data_dir.display().to_string(),
        ),
        // Temporary extraction scratch. Defaults to `<cache_dir>/enroot/tmp`, but
        // is overridable (compose `x-slurm.enroot_temp_dir`, settings
        // `cache.enroot_temp_dir`, or `HPC_COMPOSE_ENROOT_TEMP_DIR`) so it can be
        // moved to fast node-local storage: shared network filesystems are prone
        // to `Stale file handle` errors during squashfs creation.
        (
            "ENROOT_TEMP_PATH".to_string(),
            temp_dir.display().to_string(),
        ),
    ];
    if disable_nvidia_hook {
        // Prepare runs on a login node with no NVIDIA driver; disable enroot's
        // GPU hook so a CUDA image's baked NVIDIA_VISIBLE_DEVICES does not make
        // it try (and fail) to inject driver libraries. GPUs are wired in at
        // Slurm/Pyxis runtime instead.
        env.push((
            "NVIDIA_VISIBLE_DEVICES".to_string(),
            NVIDIA_HOOK_DISABLED.to_string(),
        ));
    }
    env
}

/// Resolves the effective enroot temporary scratch directory for a plan,
/// applying the `HPC_COMPOSE_ENROOT_TEMP_DIR` env override, the compose
/// `x-slurm.enroot_temp_dir`, the settings `cache.enroot_temp_dir`, then the
/// `<cache_dir>/enroot/tmp` default.
fn resolved_enroot_temp_dir(plan: &RuntimePlan, options: &PrepareOptions) -> PathBuf {
    let env_value = std::env::var(ENROOT_TEMP_DIR_ENV).ok();
    resolve_enroot_temp_dir(
        env_value.as_deref(),
        plan.slurm.enroot_temp_dir.as_deref(),
        options.enroot_temp_dir.as_deref(),
        &plan.cache_dir,
    )
}

/// Imports a base image, streaming live progress, with a single retry on a
/// clean temp directory when the failure looks like a stale-NFS-handle error,
/// and a targeted remediation appended on final failure.
fn import_base_image(
    enroot_bin: &str,
    envs: &[(String, String)],
    remote: &str,
    base_path: &Path,
    temp_dir: &Path,
    service: &str,
    reporter: &dyn PrepareReporter,
) -> Result<()> {
    let import = |reporter: &dyn PrepareReporter| {
        run_enroot(
            enroot_bin,
            envs,
            [
                "import".to_string(),
                "-o".to_string(),
                base_path.display().to_string(),
                remote.to_string(),
            ],
            &format!("import base image for service '{service}'"),
            &StreamCtx {
                reporter,
                service,
                phase: &format!("importing {remote} (first import may take several minutes)"),
                target: Some(base_path),
            },
        )
    };

    match import(reporter) {
        Ok(()) => Ok(()),
        Err(err) if is_stale_handle_error(&err) => {
            // Likely a transient stale handle or a stale partial temp dir from an
            // interrupted import; clean the scratch tree and try once more.
            reporter.step_output(
                service,
                "stale file handle during import; cleaning temp dir and retrying once",
            );
            clean_enroot_temp_dir(temp_dir);
            import(reporter).map_err(|err| enrich_import_error(err, temp_dir))
        }
        Err(err) => Err(enrich_import_error(err, temp_dir)),
    }
}

/// Appends a diagnostic listing the active prepare bind mounts when a prepare
/// command fails. Surfaces the common failure mode where a mount source on a
/// network/shared filesystem breaks at prepare time (made more likely by a
/// node-local enroot scratch dir), and points at the dependency-only pattern.
fn enrich_prepare_mount_error(
    err: anyhow::Error,
    prepare: &PreparedImageSpec,
    service: &str,
) -> anyhow::Error {
    if prepare.mounts.is_empty() {
        return err;
    }
    err.context(format!(
        "prepare command for service '{service}' failed with bind mounts active ({}). If a mount \
         source is on a network/shared filesystem it can fail at prepare time (especially with a \
         node-local enroot scratch dir). Prefer a dependency-only prepare — install dependencies \
         into the image and mount your source as a runtime volume instead of a prepare.mounts entry \
         — or ensure the mount source is reachable and stable on the prepare host.",
        prepare.mounts.join(", ")
    ))
}

/// Best-effort removal of leftover enroot extraction artifacts (e.g. `*/rootfs`
/// trees from an interrupted import) inside hpc-compose's own per-process scratch
/// subdir. Callers MUST pass an hpc-compose-owned directory (see
/// [`enroot_scratch_dir`]), never a user-shared path, because every entry is
/// removed. Never fails the caller; the directory itself is preserved.
fn clean_enroot_temp_dir(temp_dir: &Path) {
    let Ok(entries) = fs::read_dir(temp_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let _ = fs::remove_dir_all(&path);
        } else {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Creates a directory (and parents), used for an overridable enroot temp dir
/// that may live outside the cache tree.
fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).context(format!("failed to create {}", path.display()))
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
