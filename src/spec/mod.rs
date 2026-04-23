//! Compose-like spec parsing, interpolation, and validation.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

mod interpolate;
mod parse;
mod validation;

pub use interpolate::missing_defaulted_variables;

use interpolate::{
    InterpolationVars, interpolate_optional_string, interpolate_string, interpolate_vec_strings,
    interpolation_vars,
};
use parse::load_raw_spec;
use validation::{
    parse_duration_seconds, parse_healthcheck_argv, parse_http_probe, parse_nc_probe,
    validate_artifact_bundle_name, validate_artifact_path, validate_positive_u32,
    validate_resume_path, validate_sbatch_safe_string, validate_sbatch_safe_strings,
    validate_shell_hook_script,
};

/// Top-level compose file accepted by `hpc-compose`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
pub struct ComposeSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(rename = "x-slurm", default)]
    pub slurm: SlurmConfig,
    pub services: BTreeMap<String, ServiceSpec>,
}

/// Top-level runtime backend configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub backend: RuntimeBackend,
    #[serde(default)]
    pub gpu: RuntimeGpuPolicy,
}

/// Runtime backend used to launch each service inside a Slurm step.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBackend {
    /// Pyxis/Enroot through Slurm `srun --container-*` flags.
    #[default]
    Pyxis,
    /// Apptainer SIF images launched through `apptainer exec/run`.
    Apptainer,
    /// Singularity SIF images launched through `singularity exec/run`.
    Singularity,
    /// Host runtime without a container image.
    Host,
}

impl RuntimeBackend {
    /// Returns the config spelling for this backend.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pyxis => "pyxis",
            Self::Apptainer => "apptainer",
            Self::Singularity => "singularity",
            Self::Host => "host",
        }
    }

    /// Returns true when this backend launches a container image.
    #[must_use]
    pub fn is_containerized(self) -> bool {
        !matches!(self, Self::Host)
    }

    /// Returns true when this backend uses Enroot/Pyxis image artifacts.
    #[must_use]
    pub fn uses_pyxis(self) -> bool {
        matches!(self, Self::Pyxis)
    }

    /// Returns true when this backend uses SIF image artifacts.
    #[must_use]
    pub fn uses_sif(self) -> bool {
        matches!(self, Self::Apptainer | Self::Singularity)
    }
}

/// GPU passthrough policy for container backends that need an explicit flag.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeGpuPolicy {
    /// Enable backend GPU passthrough when the job or service requests GPUs.
    #[default]
    Auto,
    /// Do not add backend GPU passthrough flags.
    None,
    /// Always use NVIDIA GPU passthrough flags for supported backends.
    Nvidia,
}

/// Top-level `x-slurm` configuration shared by all services.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmConfig {
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub qos: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub ntasks: Option<u32>,
    #[serde(default)]
    pub ntasks_per_node: Option<u32>,
    #[serde(default)]
    pub cpus_per_task: Option<u32>,
    #[serde(default)]
    pub mem: Option<String>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub gpus: Option<u32>,
    #[serde(default)]
    pub gpus_per_node: Option<u32>,
    #[serde(default)]
    pub gpus_per_task: Option<u32>,
    #[serde(default)]
    pub cpus_per_gpu: Option<u32>,
    #[serde(default)]
    pub mem_per_gpu: Option<String>,
    #[serde(default)]
    pub gpu_bind: Option<String>,
    #[serde(default)]
    pub cpu_bind: Option<String>,
    #[serde(default)]
    pub mem_bind: Option<String>,
    #[serde(default)]
    pub distribution: Option<String>,
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub constraint: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub chdir: Option<String>,
    #[serde(default)]
    pub cache_dir: Option<String>,
    #[serde(default)]
    pub scratch: Option<ScratchConfig>,
    #[serde(default)]
    pub stage_in: Vec<StageInConfig>,
    #[serde(default)]
    pub stage_out: Vec<StageOutConfig>,
    #[serde(default)]
    pub burst_buffer: Option<BurstBufferConfig>,
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
    #[serde(default)]
    pub artifacts: Option<ArtifactsConfig>,
    #[serde(default)]
    pub resume: Option<ResumeConfig>,
    #[serde(default)]
    pub notify: Option<NotifyConfig>,
    #[serde(default)]
    pub setup: Vec<String>,
    #[serde(default)]
    pub submit_args: Vec<String>,
}

/// Scratch storage scope requested for a job.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScratchScope {
    /// Shared scratch path visible across all allocation nodes.
    Shared,
    /// Node-local scratch path on each allocated node.
    #[default]
    NodeLocal,
}

/// Scratch cleanup policy during batch teardown.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScratchCleanupPolicy {
    /// Remove scratch on every exit path after stage-out.
    #[default]
    Always,
    /// Remove scratch only when the job exits successfully.
    OnSuccess,
    /// Leave scratch behind for manual inspection or site cleanup.
    Never,
}

/// Top-level scratch configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScratchConfig {
    #[serde(default)]
    pub scope: ScratchScope,
    pub base: String,
    pub mount: String,
    #[serde(default)]
    pub cleanup: ScratchCleanupPolicy,
}

/// File transfer implementation for stage-in and stage-out.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StageMode {
    /// Use `rsync -a`.
    #[default]
    Rsync,
    /// Use portable `cp -R`/`cp`.
    Copy,
}

/// Stage-in path mapping run before service launch.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StageInConfig {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub mode: StageMode,
}

/// Stage-out policy for one path mapping.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StageOutWhen {
    /// Stage out on every exit path.
    #[default]
    Always,
    /// Stage out only when the job exits successfully.
    OnSuccess,
    /// Stage out only when the job fails.
    OnFailure,
}

/// Stage-out path mapping run during batch teardown.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StageOutConfig {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub when: StageOutWhen,
    #[serde(default)]
    pub mode: StageMode,
}

/// Raw site-specific burst-buffer directives emitted into the batch script.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BurstBufferConfig {
    #[serde(default)]
    pub directives: Vec<String>,
}

/// Artifact collection policy applied during batch teardown.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactCollectPolicy {
    /// Export artifacts after every job.
    #[default]
    Always,
    /// Export artifacts only for successful jobs.
    OnSuccess,
    /// Export artifacts only for failed jobs.
    OnFailure,
}

/// Top-level `x-slurm.artifacts` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactsConfig {
    #[serde(default)]
    pub collect: ArtifactCollectPolicy,
    #[serde(default)]
    pub export_dir: Option<String>,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub bundles: BTreeMap<String, ArtifactBundleSpec>,
}

/// Top-level `x-slurm.resume` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResumeConfig {
    pub path: String,
}

/// Top-level `x-slurm.notify` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    #[serde(default)]
    pub email: Option<EmailNotifyConfig>,
}

/// First-class Slurm email notification configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EmailNotifyConfig {
    pub to: String,
    #[serde(default)]
    pub on: Vec<NotifyEvent>,
}

/// Email lifecycle events supported by Slurm mail hooks.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotifyEvent {
    /// Send mail when the job begins executing.
    Start,
    /// Send mail when the job completes.
    End,
    /// Send mail when the job fails.
    Fail,
    /// Use Slurm's `ALL` shorthand.
    All,
}

/// Named artifact bundle under `x-slurm.artifacts.bundles`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactBundleSpec {
    #[serde(default)]
    pub paths: Vec<String>,
}

/// Runtime metrics collector supported by the job sampler.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricsCollector {
    /// Collect GPU telemetry through `nvidia-smi`.
    Gpu,
    /// Collect Slurm step metrics through `sstat`.
    Slurm,
}

/// Top-level `x-slurm.metrics` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub interval_seconds: Option<u64>,
    #[serde(default)]
    pub collectors: Vec<MetricsCollector>,
}

/// One service entry from the compose file.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceSpec {
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub command: Option<CommandSpec>,
    #[serde(default)]
    pub entrypoint: Option<CommandSpec>,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(rename = "working_dir", default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub depends_on: DependsOnSpec,
    #[serde(default)]
    pub readiness: Option<ReadinessSpec>,
    #[serde(default)]
    pub healthcheck: Option<HealthcheckSpec>,
    #[serde(rename = "x-slurm", default)]
    pub slurm: ServiceSlurmConfig,
    #[serde(rename = "x-runtime", default)]
    pub runtime: ServiceRuntimeConfig,
    #[serde(rename = "x-enroot", default)]
    pub enroot: ServiceEnrootConfig,
}

/// Per-service `x-slurm` overrides.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSlurmConfig {
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub placement: Option<ServicePlacementSpec>,
    #[serde(default)]
    pub ntasks: Option<u32>,
    #[serde(default)]
    pub ntasks_per_node: Option<u32>,
    #[serde(default)]
    pub cpus_per_task: Option<u32>,
    #[serde(default)]
    pub gpus: Option<u32>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub gpus_per_node: Option<u32>,
    #[serde(default)]
    pub gpus_per_task: Option<u32>,
    #[serde(default)]
    pub cpus_per_gpu: Option<u32>,
    #[serde(default)]
    pub mem_per_gpu: Option<String>,
    #[serde(default)]
    pub gpu_bind: Option<String>,
    #[serde(default)]
    pub cpu_bind: Option<String>,
    #[serde(default)]
    pub mem_bind: Option<String>,
    #[serde(default)]
    pub distribution: Option<String>,
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub time_limit: Option<String>,
    #[serde(default)]
    pub extra_srun_args: Vec<String>,
    #[serde(default)]
    pub mpi: Option<MpiConfig>,
    #[serde(default)]
    pub failure_policy: Option<ServiceFailurePolicySpec>,
    #[serde(default)]
    pub prologue: Option<ServiceHookSpec>,
    #[serde(default)]
    pub epilogue: Option<ServiceHookSpec>,
    #[serde(default)]
    pub scratch: Option<ServiceScratchConfig>,
}

/// Per-service scratch mount override.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceScratchConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// Where a per-service hook runs.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHookContext {
    /// Run the hook in the generated batch-script supervisor on the host.
    #[default]
    Host,
    /// Run the hook inside the service container.
    Container,
}

/// Per-service prologue or epilogue hook.
///
/// YAML accepts either a string shorthand, which defaults to host execution, or
/// an object with explicit `context` and `script` fields.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ServiceHookSpec {
    /// Execution context for this hook.
    pub context: ServiceHookContext,
    /// Shell script body to run for this hook.
    pub script: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawServiceHookSpec {
    Script(String),
    Object(RawServiceHookObject),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServiceHookObject {
    #[serde(default)]
    context: ServiceHookContext,
    script: String,
}

impl<'de> Deserialize<'de> for ServiceHookSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match RawServiceHookSpec::deserialize(deserializer)? {
            RawServiceHookSpec::Script(script) => Ok(Self {
                context: ServiceHookContext::Host,
                script,
            }),
            RawServiceHookSpec::Object(raw) => Ok(Self {
                context: raw.context,
                script: raw.script,
            }),
        }
    }
}

/// First-class service placement selector inside one Slurm allocation.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServicePlacementSpec {
    #[serde(default)]
    pub node_range: Option<String>,
    #[serde(default)]
    pub node_count: Option<u32>,
    #[serde(default)]
    pub node_percent: Option<u32>,
    #[serde(default)]
    pub share_with: Option<String>,
    #[serde(default)]
    pub start_index: Option<u32>,
    #[serde(default)]
    pub exclude: Option<String>,
    #[serde(default)]
    pub allow_overlap: bool,
}

/// First-class MPI launch configuration for one service step.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MpiConfig {
    #[serde(rename = "type")]
    pub mpi_type: MpiType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation: Option<MpiImplementation>,
    #[serde(default, skip_serializing_if = "MpiLauncher::is_default")]
    pub launcher: MpiLauncher,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_ranks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_mpi: Option<HostMpiConfig>,
}

/// Slurm MPI plugin type used for `srun --mpi=<type>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpiType(String);

impl MpiType {
    /// Builds an MPI type from an exact `srun --mpi` plugin token.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is empty or not a safe single CLI token.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_mpi_type_token(&value)?;
        Ok(Self(value))
    }

    /// Returns the exact value passed to `srun --mpi`.
    #[must_use]
    pub fn as_srun_value(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for MpiType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_srun_value())
    }
}

impl<'de> Deserialize<'de> for MpiType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl MpiLauncher {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// MPI implementation family used by a service image or host bind path.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MpiImplementation {
    /// Open MPI.
    Openmpi,
    /// MPICH.
    Mpich,
    /// Intel MPI.
    IntelMpi,
    /// MVAPICH2.
    Mvapich2,
    /// Cray MPI.
    CrayMpi,
    /// HPE MPI.
    HpeMpi,
    /// Implementation is intentionally unspecified.
    Unknown,
}

/// MPI process launcher selected for the service.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MpiLauncher {
    /// Slurm launches ranks directly with `srun --mpi=...`.
    #[default]
    Srun,
}

/// Host MPI installation bindings injected into a containerized MPI service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostMpiConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bind_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "EnvironmentSpec::is_none")]
    pub env: EnvironmentSpec,
}

/// Per-service failure mode inside a single batch job.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceFailureMode {
    /// Any non-zero service exit fails the whole job.
    #[default]
    FailJob,
    /// Non-zero exits are recorded but do not fail the whole job.
    Ignore,
    /// Non-zero exits trigger bounded restarts before failing the job.
    RestartOnFailure,
}

/// Raw per-service failure policy declaration under `x-slurm.failure_policy`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceFailurePolicySpec {
    #[serde(default)]
    pub mode: ServiceFailureMode,
    #[serde(default)]
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub backoff_seconds: Option<u64>,
    #[serde(default)]
    pub window_seconds: Option<u64>,
    #[serde(default)]
    pub max_restarts_in_window: Option<u32>,
}

impl Default for ServiceFailurePolicySpec {
    fn default() -> Self {
        Self {
            mode: ServiceFailureMode::FailJob,
            max_restarts: None,
            backoff_seconds: None,
            window_seconds: None,
            max_restarts_in_window: None,
        }
    }
}

/// Normalized per-service failure policy with defaults resolved.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceFailurePolicy {
    pub mode: ServiceFailureMode,
    pub max_restarts: u32,
    pub backoff_seconds: u64,
    pub window_seconds: u64,
    pub max_restarts_in_window: u32,
}

impl Default for ServiceFailurePolicy {
    fn default() -> Self {
        Self {
            mode: ServiceFailureMode::FailJob,
            max_restarts: 0,
            backoff_seconds: 0,
            window_seconds: 0,
            max_restarts_in_window: 0,
        }
    }
}

/// Stable, fully interpolated config surface used by `hpc-compose config`
/// and persisted for resume-diff checks.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveComposeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub runtime: RuntimeConfig,
    #[serde(rename = "x-slurm")]
    pub slurm: EffectiveSlurmConfig,
    pub services: BTreeMap<String, EffectiveServiceConfig>,
}

/// Stable top-level `x-slurm` config with defaults materialized.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveSlurmConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ntasks: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ntasks_per_node: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus_per_task: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gres: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus_per_node: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus_per_task: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus_per_gpu: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_per_gpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chdir: Option<String>,
    pub cache_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scratch: Option<ScratchConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub stage_in: Vec<StageInConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub stage_out: Vec<StageOutConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst_buffer: Option<BurstBufferConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<EffectiveMetricsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<EffectiveArtifactsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<EffectiveNotifyConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub setup: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub submit_args: Vec<String>,
}

/// Stable effective metrics config with defaults applied.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveMetricsConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub collectors: Vec<MetricsCollector>,
}

/// Stable effective artifacts config with defaults applied.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveArtifactsConfig {
    pub collect: ArtifactCollectPolicy,
    pub export_dir: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub paths: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub bundles: BTreeMap<String, ArtifactBundleSpec>,
}

/// Stable effective notify config with defaults applied.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveNotifyConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<EffectiveEmailNotifyConfig>,
}

/// Stable effective email notify config with normalized event order.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveEmailNotifyConfig {
    pub to: String,
    pub on: Vec<NotifyEvent>,
}

/// Stable service config surface with defaults applied where needed.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveServiceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<CommandSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<CommandSpec>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub environment: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub depends_on: BTreeMap<String, EffectiveDependsOnCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessSpec>,
    #[serde(rename = "x-slurm")]
    pub slurm: EffectiveServiceSlurmConfig,
    #[serde(rename = "x-runtime", skip_serializing_if = "Option::is_none")]
    pub runtime: Option<EffectiveServiceRuntimeConfig>,
    #[serde(rename = "x-enroot", skip_serializing_if = "Option::is_none")]
    pub enroot: Option<EffectiveServiceEnrootConfig>,
}

/// Stable dependency representation used by effective config output.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveDependsOnCondition {
    pub condition: DependencyCondition,
}

/// Stable per-service `x-slurm` config with advisory defaults applied.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveServiceSlurmConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<ServicePlacementSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ntasks: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ntasks_per_node: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus_per_task: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gres: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus_per_node: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus_per_task: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus_per_gpu: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_per_gpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_bind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_limit: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub extra_srun_args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mpi: Option<MpiConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prologue: Option<ServiceHookSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epilogue: Option<ServiceHookSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scratch: Option<ServiceScratchConfig>,
    pub failure_policy: EffectiveFailurePolicyConfig,
}

/// Stable effective `x-runtime` service config.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveServiceRuntimeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepare: Option<EffectivePrepareSpec>,
}

/// Stable effective per-service failure policy.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveFailurePolicyConfig {
    pub mode: ServiceFailureMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_restarts_in_window: Option<u32>,
}

/// Stable effective `x-enroot` config.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveServiceEnrootConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepare: Option<EffectivePrepareSpec>,
}

/// Stable effective prepare config with defaults applied.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectivePrepareSpec {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub commands: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub mounts: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub env: BTreeMap<String, String>,
    pub root: bool,
}

/// Per-service backend-neutral runtime configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceRuntimeConfig {
    #[serde(default)]
    pub prepare: Option<PrepareSpec>,
}

/// Per-service `x-enroot` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrootConfig {
    #[serde(default)]
    pub prepare: Option<PrepareSpec>,
}

/// Image customization for rebuilding a runtime artifact on the login node.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareSpec {
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub mounts: Vec<String>,
    #[serde(default)]
    pub env: EnvironmentSpec,
    #[serde(default = "default_true")]
    pub root: bool,
}

/// Accepted `depends_on` shapes.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
pub enum DependsOnSpec {
    /// No dependencies were declared.
    #[default]
    None,
    /// Compose list shorthand, which implies `service_started`.
    List(Vec<String>),
    /// Mapping form with explicit dependency conditions.
    Map(BTreeMap<String, DependsOnConditionSpec>),
}

/// Dependency condition declared for one dependency edge.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependsOnConditionSpec {
    #[serde(default)]
    pub condition: Option<String>,
}

/// Normalized dependency conditions understood by the planner.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DependencyCondition {
    /// Wait only until the upstream service is started.
    ServiceStarted,
    /// Wait until the upstream service reports readiness.
    ServiceHealthy,
    /// Wait until the upstream service exits successfully.
    ServiceCompletedSuccessfully,
}

/// A normalized service dependency edge.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDependency {
    pub name: String,
    pub condition: DependencyCondition,
}

/// Accepted environment syntaxes for service or prepare environments.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum EnvironmentSpec {
    /// No environment variables were declared.
    #[default]
    None,
    /// Mapping form such as `{ FOO: bar }`.
    Map(BTreeMap<String, String>),
    /// List form such as `["FOO=bar"]`.
    List(Vec<String>),
}

/// Accepted command or entrypoint syntaxes.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CommandSpec {
    /// Shell form command.
    String(String),
    /// Exec form argv vector.
    Vec(Vec<String>),
}

/// Readiness checks supported by `hpc-compose`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReadinessSpec {
    /// Wait for a fixed amount of time.
    Sleep {
        /// Number of seconds to sleep.
        seconds: u64,
    },
    /// Poll a TCP port.
    Tcp {
        /// Port to connect to.
        port: u16,
        /// Optional host; defaults to localhost inside the job.
        #[serde(default)]
        host: Option<String>,
        /// Optional readiness timeout.
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
    /// Wait until a pattern appears in the service log.
    Log {
        /// Literal pattern to look for in the log.
        pattern: String,
        /// Optional readiness timeout.
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
    /// Poll an HTTP endpoint.
    Http {
        /// URL to request.
        url: String,
        /// Expected success status code.
        #[serde(default = "default_http_status_code")]
        status_code: u16,
        /// Optional readiness timeout.
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
}

/// Compose-compatible healthcheck block accepted as sugar for `readiness`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthcheckSpec {
    #[serde(default)]
    pub test: Option<HealthcheckTest>,
    #[serde(default)]
    pub timeout: Option<HealthcheckDuration>,
    #[serde(default)]
    pub disable: Option<bool>,
    #[serde(default)]
    pub interval: Option<HealthcheckDuration>,
    #[serde(default)]
    pub retries: Option<u32>,
    #[serde(default)]
    pub start_period: Option<HealthcheckDuration>,
}

/// Supported healthcheck `test` syntaxes.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum HealthcheckTest {
    /// Compose exec-array form.
    Vec(Vec<String>),
    /// String form, treated like a shell probe.
    String(String),
}

/// Supported healthcheck duration syntaxes.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum HealthcheckDuration {
    /// Numeric seconds.
    Seconds(u64),
    /// Compose duration string such as `30s` or `1m30s`.
    String(String),
}

fn default_http_status_code() -> u16 {
    200
}

impl ComposeSpec {
    /// Loads, interpolates, and validates a compose file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, the YAML cannot be
    /// parsed, interpolation fails, or semantic validation rejects the spec.
    pub fn load(path: &Path) -> Result<Self> {
        let vars = interpolation_vars(path)?;
        Self::load_with_interpolation_vars(path, &vars)
    }

    /// Loads, interpolates, and validates a compose file using explicit
    /// interpolation variables.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, the YAML cannot be
    /// parsed, interpolation fails, or semantic validation rejects the spec.
    pub fn load_with_interpolation_vars(
        path: &Path,
        vars: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut spec = load_raw_spec(path)?;
        spec.interpolate_with_vars(vars)?;
        spec.validate()?;
        Ok(spec)
    }

    fn interpolate_with_vars(&mut self, vars: &BTreeMap<String, String>) -> Result<()> {
        interpolate_optional_string(&mut self.name, vars)?;
        self.slurm.interpolate(vars)?;
        for service in self.services.values_mut() {
            service.interpolate(vars)?;
        }
        Ok(())
    }

    fn validate(&mut self) -> Result<()> {
        self.slurm.validate()?;
        for (name, service) in &mut self.services {
            service.normalize_healthcheck()?;
            if service.runtime.prepare.is_some() && service.enroot.prepare.is_some() {
                bail!(
                    "service '{name}' sets both x-runtime.prepare and x-enroot.prepare; use only x-runtime.prepare for new specs"
                );
            }
            if self.runtime.backend != RuntimeBackend::Pyxis && service.enroot.prepare.is_some() {
                bail!(
                    "service '{name}' uses x-enroot.prepare with runtime.backend={}; use x-runtime.prepare for non-Pyxis backends",
                    self.runtime.backend.as_str()
                );
            }
            service.slurm.validate(name)?;
        }
        Ok(())
    }

    /// Builds a stable, fully interpolated effective config snapshot suitable
    /// for `hpc-compose config` output and resume-diff persistence.
    ///
    /// # Errors
    ///
    /// Returns an error when dependency or environment normalization fails.
    pub fn effective_config(
        &self,
        cache_dir: &Path,
        normalized_policies: &BTreeMap<String, ServiceFailurePolicy>,
    ) -> Result<EffectiveComposeConfig> {
        let mut services = BTreeMap::new();
        for (name, service) in &self.services {
            let environment = service
                .environment
                .to_pairs()?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            let depends_on = service
                .depends_on
                .entries()?
                .into_iter()
                .map(|dependency| {
                    (
                        dependency.name,
                        EffectiveDependsOnCondition {
                            condition: dependency.condition,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let normalized_policy = normalized_policies.get(name).cloned().unwrap_or_default();
            let enroot = match service.enroot.prepare.as_ref() {
                Some(prepare) => Some(EffectiveServiceEnrootConfig {
                    prepare: Some(EffectivePrepareSpec {
                        commands: prepare.commands.clone(),
                        mounts: prepare.mounts.clone(),
                        env: prepare
                            .env
                            .to_pairs()?
                            .into_iter()
                            .collect::<BTreeMap<_, _>>(),
                        root: prepare.root,
                    }),
                }),
                None => None,
            };
            let runtime = match service.runtime.prepare.as_ref() {
                Some(prepare) => Some(EffectiveServiceRuntimeConfig {
                    prepare: Some(EffectivePrepareSpec {
                        commands: prepare.commands.clone(),
                        mounts: prepare.mounts.clone(),
                        env: prepare
                            .env
                            .to_pairs()?
                            .into_iter()
                            .collect::<BTreeMap<_, _>>(),
                        root: prepare.root,
                    }),
                }),
                None => None,
            };
            services.insert(
                name.clone(),
                EffectiveServiceConfig {
                    image: service.image.clone(),
                    command: service.command.clone(),
                    entrypoint: service.entrypoint.clone(),
                    environment,
                    volumes: service.volumes.clone(),
                    working_dir: service.working_dir.clone(),
                    depends_on,
                    readiness: service.readiness.clone(),
                    slurm: EffectiveServiceSlurmConfig {
                        nodes: service.slurm.nodes,
                        placement: service.slurm.placement.clone(),
                        ntasks: service.slurm.ntasks,
                        ntasks_per_node: service.slurm.ntasks_per_node,
                        cpus_per_task: service.slurm.cpus_per_task,
                        gpus: service.slurm.gpus,
                        gres: service.slurm.gres.clone(),
                        gpus_per_node: service.slurm.gpus_per_node,
                        gpus_per_task: service.slurm.gpus_per_task,
                        cpus_per_gpu: service.slurm.cpus_per_gpu,
                        mem_per_gpu: service.slurm.mem_per_gpu.clone(),
                        gpu_bind: service.slurm.gpu_bind.clone(),
                        cpu_bind: service.slurm.cpu_bind.clone(),
                        mem_bind: service.slurm.mem_bind.clone(),
                        distribution: service.slurm.distribution.clone(),
                        hint: service.slurm.hint.clone(),
                        time_limit: service.slurm.time_limit.clone(),
                        extra_srun_args: service.slurm.extra_srun_args.clone(),
                        mpi: service.slurm.mpi.clone(),
                        prologue: service.slurm.prologue.clone(),
                        epilogue: service.slurm.epilogue.clone(),
                        scratch: service.slurm.scratch.clone(),
                        failure_policy: EffectiveFailurePolicyConfig::from_policy(
                            &normalized_policy,
                        ),
                    },
                    runtime,
                    enroot,
                },
            );
        }

        Ok(EffectiveComposeConfig {
            name: self.name.clone(),
            runtime: self.runtime.clone(),
            slurm: EffectiveSlurmConfig {
                job_name: self.slurm.job_name.clone(),
                partition: self.slurm.partition.clone(),
                account: self.slurm.account.clone(),
                qos: self.slurm.qos.clone(),
                time: self.slurm.time.clone(),
                nodes: self.slurm.nodes,
                ntasks: self.slurm.ntasks,
                ntasks_per_node: self.slurm.ntasks_per_node,
                cpus_per_task: self.slurm.cpus_per_task,
                mem: self.slurm.mem.clone(),
                gres: self.slurm.gres.clone(),
                gpus: self.slurm.gpus,
                gpus_per_node: self.slurm.gpus_per_node,
                gpus_per_task: self.slurm.gpus_per_task,
                cpus_per_gpu: self.slurm.cpus_per_gpu,
                mem_per_gpu: self.slurm.mem_per_gpu.clone(),
                gpu_bind: self.slurm.gpu_bind.clone(),
                cpu_bind: self.slurm.cpu_bind.clone(),
                mem_bind: self.slurm.mem_bind.clone(),
                distribution: self.slurm.distribution.clone(),
                hint: self.slurm.hint.clone(),
                constraint: self.slurm.constraint.clone(),
                output: self.slurm.output.clone(),
                error: self.slurm.error.clone(),
                chdir: self.slurm.chdir.clone(),
                cache_dir: cache_dir.display().to_string(),
                scratch: self.slurm.scratch.clone(),
                stage_in: self.slurm.stage_in.clone(),
                stage_out: self.slurm.stage_out.clone(),
                burst_buffer: self.slurm.burst_buffer.clone(),
                metrics: self.slurm.metrics.as_ref().map(|_| EffectiveMetricsConfig {
                    enabled: self.slurm.metrics_enabled(),
                    interval_seconds: self.slurm.metrics_interval_seconds(),
                    collectors: self.slurm.metrics_collectors(),
                }),
                artifacts: self.slurm.artifacts.as_ref().map(|artifacts| {
                    EffectiveArtifactsConfig {
                        collect: self.slurm.artifacts_collect_policy(),
                        export_dir: artifacts.export_dir.clone().unwrap_or_default(),
                        paths: artifacts.paths.clone(),
                        bundles: artifacts.bundles.clone(),
                    }
                }),
                resume: self.slurm.resume.clone(),
                notify: self
                    .slurm
                    .notify
                    .as_ref()
                    .map(|notify| EffectiveNotifyConfig {
                        email: notify
                            .email
                            .as_ref()
                            .map(|email| EffectiveEmailNotifyConfig {
                                to: email.to.clone(),
                                on: normalize_notify_events(&email.on),
                            }),
                    }),
                setup: self.slurm.setup.clone(),
                submit_args: self.slurm.submit_args.clone(),
            },
            services,
        })
    }
}

impl DependsOnSpec {
    /// Normalizes the dependency declaration into explicit dependency edges.
    ///
    /// # Errors
    ///
    /// Returns an error when a dependency condition uses an unsupported value.
    pub fn entries(&self) -> Result<Vec<ServiceDependency>> {
        match self {
            DependsOnSpec::None => Ok(Vec::new()),
            DependsOnSpec::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for name in items {
                    out.push(ServiceDependency {
                        name: name.clone(),
                        condition: DependencyCondition::ServiceStarted,
                    });
                }
                Ok(out)
            }
            DependsOnSpec::Map(items) => {
                let mut out = Vec::with_capacity(items.len());
                for (name, cfg) in items {
                    let condition = match cfg.condition.as_deref() {
                        None | Some("service_started") => DependencyCondition::ServiceStarted,
                        Some("service_healthy") => DependencyCondition::ServiceHealthy,
                        Some("service_completed_successfully") => {
                            DependencyCondition::ServiceCompletedSuccessfully
                        }
                        Some(other) => {
                            bail!(
                                "depends_on condition for service '{name}' must be 'service_started', 'service_healthy', or 'service_completed_successfully', got '{other}'"
                            );
                        }
                    };
                    out.push(ServiceDependency {
                        name: name.clone(),
                        condition,
                    });
                }
                Ok(out)
            }
        }
    }

    /// Returns only the dependency names, discarding their conditions.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`DependsOnSpec::entries`] when the dependency
    /// declaration uses unsupported conditions.
    pub fn names(&self) -> Result<Vec<String>> {
        let entries = self.entries()?;
        Ok(entries.into_iter().map(|entry| entry.name).collect())
    }
}

impl EnvironmentSpec {
    fn is_none(&self) -> bool {
        matches!(self, EnvironmentSpec::None)
    }

    /// Normalizes the environment declaration into key/value pairs.
    ///
    /// # Errors
    ///
    /// Returns an error when list-form environment entries do not use
    /// `KEY=VALUE` syntax.
    pub fn to_pairs(&self) -> Result<Vec<(String, String)>> {
        match self {
            EnvironmentSpec::None => Ok(Vec::new()),
            EnvironmentSpec::Map(map) => Ok(map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()),
            EnvironmentSpec::List(items) => {
                let mut pairs = Vec::with_capacity(items.len());
                for item in items {
                    let Some((key, value)) = item.split_once('=') else {
                        bail!("environment list items must use KEY=VALUE syntax");
                    };
                    pairs.push((key.to_string(), value.to_string()));
                }
                Ok(pairs)
            }
        }
    }

    fn interpolate_values(&mut self, vars: &InterpolationVars) -> Result<()> {
        match self {
            EnvironmentSpec::None => Ok(()),
            EnvironmentSpec::Map(map) => {
                for value in map.values_mut() {
                    *value = interpolate_string(value, vars)?;
                }
                Ok(())
            }
            EnvironmentSpec::List(items) => {
                for item in items.iter_mut() {
                    let Some((key, value)) = item.split_once('=') else {
                        bail!("environment list items must use KEY=VALUE syntax");
                    };
                    *item = format!("{key}={}", interpolate_string(value, vars)?);
                }
                Ok(())
            }
        }
    }
}

impl CommandSpec {
    /// Returns `true` when this command uses shell-string form.
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, CommandSpec::String(_))
    }

    /// Returns the shell-form string when this command uses string form.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            CommandSpec::String(value) => Some(value),
            CommandSpec::Vec(_) => None,
        }
    }

    /// Returns the exec-form argv when this command uses vector form.
    #[must_use]
    pub fn as_vec(&self) -> Option<&[String]> {
        match self {
            CommandSpec::String(_) => None,
            CommandSpec::Vec(value) => Some(value),
        }
    }

    fn interpolate_if_vec(&mut self, vars: &InterpolationVars) -> Result<()> {
        match self {
            CommandSpec::String(_) => Ok(()),
            CommandSpec::Vec(items) => {
                for item in items.iter_mut() {
                    *item = interpolate_string(item, vars)?;
                }
                Ok(())
            }
        }
    }
}

fn default_true() -> bool {
    true
}

impl SlurmConfig {
    /// Returns the effective Slurm allocation node count.
    #[must_use]
    pub fn allocation_nodes(&self) -> u32 {
        self.nodes.unwrap_or(1)
    }

    /// Returns whether the allocation spans multiple nodes.
    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.allocation_nodes() > 1
    }

    /// Returns whether runtime metrics sampling is enabled.
    #[must_use]
    pub fn metrics_enabled(&self) -> bool {
        self.metrics
            .as_ref()
            .is_some_and(|metrics| metrics.enabled.unwrap_or(true))
    }

    /// Returns the runtime metrics sampling interval in seconds.
    #[must_use]
    pub fn metrics_interval_seconds(&self) -> u64 {
        self.metrics
            .as_ref()
            .and_then(|metrics| metrics.interval_seconds)
            .unwrap_or(5)
    }

    /// Returns the configured runtime metrics collectors with defaults applied.
    #[must_use]
    pub fn metrics_collectors(&self) -> Vec<MetricsCollector> {
        let Some(metrics) = &self.metrics else {
            return Vec::new();
        };
        if metrics.collectors.is_empty() {
            vec![MetricsCollector::Gpu, MetricsCollector::Slurm]
        } else {
            metrics.collectors.clone()
        }
    }

    /// Returns whether teardown artifact collection is enabled.
    #[must_use]
    pub fn artifacts_enabled(&self) -> bool {
        self.artifacts.is_some()
    }

    /// Returns the configured artifact collection policy or the default.
    #[must_use]
    pub fn artifacts_collect_policy(&self) -> ArtifactCollectPolicy {
        self.artifacts
            .as_ref()
            .map(|artifacts| artifacts.collect)
            .unwrap_or_default()
    }

    /// Returns the configured shared resume directory when resume semantics are enabled.
    #[must_use]
    pub fn resume_dir(&self) -> Option<&str> {
        self.resume.as_ref().map(|resume| resume.path.as_str())
    }

    /// Returns normalized email events in stable order.
    #[must_use]
    pub fn notify_email_events(&self) -> Vec<NotifyEvent> {
        let Some(email) = self
            .notify
            .as_ref()
            .and_then(|notify| notify.email.as_ref())
        else {
            return Vec::new();
        };
        normalize_notify_events(&email.on)
    }

    /// Returns the configured notification email recipient, if any.
    #[must_use]
    pub fn notify_email_recipient(&self) -> Option<&str> {
        self.notify
            .as_ref()
            .and_then(|notify| notify.email.as_ref())
            .map(|email| email.to.as_str())
    }

    /// Returns the normalized Slurm mail-type value when first-class
    /// notifications are configured.
    #[must_use]
    pub fn notify_mail_type_value(&self) -> Option<String> {
        let events = self.notify_email_events();
        if events.is_empty() {
            return None;
        }
        Some(
            events
                .into_iter()
                .map(notify_event_mail_type)
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    /// Validates semantic rules that serde alone cannot express.
    ///
    /// # Errors
    ///
    /// Returns an error when allocation, metrics, artifact, or resume settings
    /// violate `hpc-compose`'s supported Slurm model.
    pub fn validate(&self) -> Result<()> {
        validate_positive_u32(self.nodes, "x-slurm.nodes")?;
        validate_positive_u32(self.ntasks, "x-slurm.ntasks")?;
        validate_positive_u32(self.ntasks_per_node, "x-slurm.ntasks_per_node")?;
        validate_positive_u32(self.gpus_per_node, "x-slurm.gpus_per_node")?;
        validate_positive_u32(self.gpus_per_task, "x-slurm.gpus_per_task")?;
        validate_positive_u32(self.cpus_per_gpu, "x-slurm.cpus_per_gpu")?;
        validate_sbatch_safe_string(self.job_name.as_deref(), "x-slurm.job-name")?;
        validate_sbatch_safe_string(self.partition.as_deref(), "x-slurm.partition")?;
        validate_sbatch_safe_string(self.account.as_deref(), "x-slurm.account")?;
        validate_sbatch_safe_string(self.qos.as_deref(), "x-slurm.qos")?;
        validate_sbatch_safe_string(self.constraint.as_deref(), "x-slurm.constraint")?;
        validate_sbatch_safe_string(self.time.as_deref(), "x-slurm.time")?;
        validate_sbatch_safe_string(self.mem.as_deref(), "x-slurm.mem")?;
        validate_sbatch_safe_string(self.gres.as_deref(), "x-slurm.gres")?;
        validate_sbatch_safe_string(self.mem_per_gpu.as_deref(), "x-slurm.mem_per_gpu")?;
        validate_sbatch_safe_string(self.gpu_bind.as_deref(), "x-slurm.gpu_bind")?;
        validate_sbatch_safe_string(self.cpu_bind.as_deref(), "x-slurm.cpu_bind")?;
        validate_sbatch_safe_string(self.mem_bind.as_deref(), "x-slurm.mem_bind")?;
        validate_sbatch_safe_string(self.distribution.as_deref(), "x-slurm.distribution")?;
        validate_sbatch_safe_string(self.hint.as_deref(), "x-slurm.hint")?;
        validate_sbatch_safe_string(self.output.as_deref(), "x-slurm.output")?;
        validate_sbatch_safe_string(self.error.as_deref(), "x-slurm.error")?;
        validate_sbatch_safe_string(self.chdir.as_deref(), "x-slurm.chdir")?;
        validate_sbatch_safe_strings(
            self.submit_args.iter().map(String::as_str),
            "x-slurm.submit_args",
        )?;
        validate_submit_arg_conflicts(self)?;
        if let Some(scratch) = &self.scratch {
            scratch.validate()?;
        }
        for (index, entry) in self.stage_in.iter().enumerate() {
            entry.validate(index)?;
        }
        for (index, entry) in self.stage_out.iter().enumerate() {
            entry.validate(index)?;
        }
        if let Some(burst_buffer) = &self.burst_buffer {
            burst_buffer.validate()?;
        }
        if let Some(metrics) = &self.metrics
            && matches!(metrics.interval_seconds, Some(0))
        {
            bail!("x-slurm.metrics.interval_seconds must be at least 1");
        }
        if let Some(artifacts) = &self.artifacts {
            let Some(export_dir) = artifacts.export_dir.as_deref() else {
                bail!("x-slurm.artifacts.export_dir is required when x-slurm.artifacts is present");
            };
            if export_dir.trim().is_empty() {
                bail!("x-slurm.artifacts.export_dir must not be empty");
            }
            if artifacts.paths.is_empty() && artifacts.bundles.is_empty() {
                bail!(
                    "x-slurm.artifacts must contain at least one source path in paths or bundles"
                );
            }
            for path in &artifacts.paths {
                validate_artifact_path(path)?;
            }
            for (name, bundle) in &artifacts.bundles {
                validate_artifact_bundle_name(name)?;
                if bundle.paths.is_empty() {
                    bail!(
                        "x-slurm.artifacts.bundles.{name}.paths must contain at least one source path"
                    );
                }
                for path in &bundle.paths {
                    validate_artifact_path(path)?;
                }
            }
        }
        if let Some(resume) = &self.resume {
            validate_resume_path(&resume.path)?;
        }
        if let Some(email) = self
            .notify
            .as_ref()
            .and_then(|notify| notify.email.as_ref())
        {
            if email.to.trim().is_empty() {
                bail!("x-slurm.notify.email.to must not be empty");
            }
            validate_sbatch_safe_string(Some(email.to.as_str()), "x-slurm.notify.email.to")?;
            if submit_args_contain_mail_settings(&self.submit_args) {
                bail!(
                    "x-slurm.notify.email cannot be combined with raw --mail-type/--mail-user submit args"
                );
            }
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_optional_string(&mut self.job_name, vars)?;
        interpolate_optional_string(&mut self.partition, vars)?;
        interpolate_optional_string(&mut self.account, vars)?;
        interpolate_optional_string(&mut self.qos, vars)?;
        interpolate_optional_string(&mut self.time, vars)?;
        interpolate_optional_string(&mut self.mem, vars)?;
        interpolate_optional_string(&mut self.gres, vars)?;
        interpolate_optional_string(&mut self.mem_per_gpu, vars)?;
        interpolate_optional_string(&mut self.gpu_bind, vars)?;
        interpolate_optional_string(&mut self.cpu_bind, vars)?;
        interpolate_optional_string(&mut self.mem_bind, vars)?;
        interpolate_optional_string(&mut self.distribution, vars)?;
        interpolate_optional_string(&mut self.hint, vars)?;
        interpolate_optional_string(&mut self.constraint, vars)?;
        interpolate_optional_string(&mut self.output, vars)?;
        interpolate_optional_string(&mut self.error, vars)?;
        interpolate_optional_string(&mut self.chdir, vars)?;
        interpolate_optional_string(&mut self.cache_dir, vars)?;
        if let Some(scratch) = &mut self.scratch {
            scratch.interpolate(vars)?;
        }
        for entry in &mut self.stage_in {
            entry.interpolate(vars)?;
        }
        for entry in &mut self.stage_out {
            entry.interpolate(vars)?;
        }
        if let Some(artifacts) = &mut self.artifacts {
            artifacts.interpolate(vars)?;
        }
        if let Some(resume) = &mut self.resume {
            resume.interpolate(vars)?;
        }
        if let Some(notify) = &mut self.notify {
            notify.interpolate(vars)?;
        }
        interpolate_vec_strings(&mut self.submit_args, vars)?;
        Ok(())
    }
}

impl ScratchConfig {
    fn validate(&self) -> Result<()> {
        if self.base.trim().is_empty() {
            bail!("x-slurm.scratch.base must not be empty");
        }
        if self.base.contains('\0') {
            bail!("x-slurm.scratch.base must not contain null bytes");
        }
        if self.mount.trim().is_empty() {
            bail!("x-slurm.scratch.mount must not be empty");
        }
        let mount = Path::new(self.mount.trim());
        if !mount.is_absolute() {
            bail!(
                "x-slurm.scratch.mount must be an absolute container path, got '{}'",
                self.mount
            );
        }
        if self.mount.contains('\0') {
            bail!("x-slurm.scratch.mount must not contain null bytes");
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        self.base = interpolate_string_preserving_slurm_job_id(&self.base, vars)?;
        self.mount = interpolate_string(&self.mount, vars)?;
        Ok(())
    }
}

impl StageInConfig {
    fn validate(&self, index: usize) -> Result<()> {
        validate_stage_path(&self.from, &format!("x-slurm.stage_in[{index}].from"))?;
        validate_stage_path(&self.to, &format!("x-slurm.stage_in[{index}].to"))?;
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        self.from = interpolate_string_preserving_slurm_job_id(&self.from, vars)?;
        self.to = interpolate_string_preserving_slurm_job_id(&self.to, vars)?;
        Ok(())
    }
}

impl StageOutConfig {
    fn validate(&self, index: usize) -> Result<()> {
        validate_stage_path(&self.from, &format!("x-slurm.stage_out[{index}].from"))?;
        validate_stage_path(&self.to, &format!("x-slurm.stage_out[{index}].to"))?;
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        self.from = interpolate_string_preserving_slurm_job_id(&self.from, vars)?;
        self.to = interpolate_string_preserving_slurm_job_id(&self.to, vars)?;
        Ok(())
    }
}

impl BurstBufferConfig {
    fn validate(&self) -> Result<()> {
        for (index, directive) in self.directives.iter().enumerate() {
            if !directive.starts_with("#BB ") && !directive.starts_with("#DW ") {
                bail!("x-slurm.burst_buffer.directives[{index}] must start with '#BB ' or '#DW '");
            }
            validate_sbatch_safe_string(
                Some(directive.as_str()),
                &format!("x-slurm.burst_buffer.directives[{index}]"),
            )?;
        }
        Ok(())
    }
}

fn validate_stage_path(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    Ok(())
}

fn validate_mount_syntax(value: &str, field: &str) -> Result<()> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [host, container] | [host, container, "ro" | "rw"] => {
            if host.trim().is_empty() {
                bail!("{field} host path must not be empty");
            }
            if container.trim().is_empty() {
                bail!("{field} container path must not be empty");
            }
            let container_path = Path::new(container.trim());
            if !container_path.is_absolute() {
                bail!("{field} container path must be absolute");
            }
            if value.contains('\0') {
                bail!("{field} must not contain null bytes");
            }
            Ok(())
        }
        [_, _, mode] => bail!("{field} uses unsupported mode '{mode}'; use ro or rw"),
        _ => bail!("{field} must use host_path:container_path[:ro|rw] syntax"),
    }
}

fn interpolate_string_preserving_slurm_job_id(
    raw: &str,
    vars: &InterpolationVars,
) -> Result<String> {
    const JOB_ID_SENTINEL: &str = "__HPC_COMPOSE_SLURM_JOB_ID__";
    let mut vars_with_job_id = vars.clone();
    vars_with_job_id.insert("SLURM_JOB_ID".to_string(), JOB_ID_SENTINEL.to_string());
    Ok(interpolate_string(raw, &vars_with_job_id)?.replace(JOB_ID_SENTINEL, "${SLURM_JOB_ID}"))
}

impl ArtifactsConfig {
    /// Returns artifact bundles with the legacy top-level `paths` exposed as `default`.
    #[must_use]
    pub fn normalized_bundles(&self) -> BTreeMap<String, Vec<String>> {
        let mut bundles = self
            .bundles
            .iter()
            .map(|(name, bundle)| (name.clone(), bundle.paths.clone()))
            .collect::<BTreeMap<_, _>>();
        if !self.paths.is_empty() {
            bundles.insert("default".to_string(), self.paths.clone());
        }
        bundles
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(export_dir) = &mut self.export_dir {
            let mut vars_with_job_id = vars.clone();
            const JOB_ID_SENTINEL: &str = "__HPC_COMPOSE_SLURM_JOB_ID__";
            vars_with_job_id.insert("SLURM_JOB_ID".to_string(), JOB_ID_SENTINEL.to_string());
            *export_dir = interpolate_string(export_dir, &vars_with_job_id)?
                .replace(JOB_ID_SENTINEL, "${SLURM_JOB_ID}");
        }
        interpolate_vec_strings(&mut self.paths, vars)?;
        for bundle in self.bundles.values_mut() {
            interpolate_vec_strings(&mut bundle.paths, vars)?;
        }
        Ok(())
    }
}

impl ResumeConfig {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        self.path = interpolate_string(&self.path, vars)?;
        Ok(())
    }
}

impl NotifyConfig {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(email) = &mut self.email {
            email.to = interpolate_string(&email.to, vars)?;
        }
        Ok(())
    }
}

impl ServiceSpec {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_optional_string(&mut self.image, vars)?;
        if let Some(command) = &mut self.command {
            command.interpolate_if_vec(vars)?;
        }
        if let Some(entrypoint) = &mut self.entrypoint {
            entrypoint.interpolate_if_vec(vars)?;
        }
        self.environment.interpolate_values(vars)?;
        interpolate_vec_strings(&mut self.volumes, vars)?;
        interpolate_optional_string(&mut self.working_dir, vars)?;
        if let Some(healthcheck) = &mut self.healthcheck {
            healthcheck.interpolate(vars)?;
        }
        self.slurm.interpolate(vars)?;
        self.runtime.interpolate(vars)?;
        self.enroot.interpolate(vars)?;
        Ok(())
    }

    fn normalize_healthcheck(&mut self) -> Result<()> {
        if self.readiness.is_some() && self.healthcheck.is_some() {
            bail!("readiness and healthcheck are mutually exclusive; use only one");
        }

        let Some(healthcheck) = self.healthcheck.take() else {
            return Ok(());
        };
        if healthcheck.disable.unwrap_or(false) {
            self.readiness = None;
            return Ok(());
        }
        if healthcheck.interval.is_some() {
            bail!(
                "healthcheck.interval is not supported; use healthcheck.timeout or explicit readiness instead"
            );
        }
        if healthcheck.retries.is_some() {
            bail!(
                "healthcheck.retries is not supported; use healthcheck.timeout or explicit readiness instead"
            );
        }
        if healthcheck.start_period.is_some() {
            bail!(
                "healthcheck.start_period is not supported; use healthcheck.timeout or explicit readiness instead"
            );
        }
        let timeout_seconds = healthcheck
            .timeout
            .as_ref()
            .map(HealthcheckDuration::to_seconds)
            .transpose()?;
        let test = healthcheck
            .test
            .context("healthcheck.test is required unless healthcheck.disable is true")?;
        self.readiness = Some(test.to_readiness(timeout_seconds)?);
        Ok(())
    }
}

impl ServiceSlurmConfig {
    /// Validates semantic rules on service-level Slurm options.
    ///
    /// # Errors
    ///
    /// Returns an error when service-level node or task counts are invalid.
    pub fn validate(&self, service_name: &str) -> Result<()> {
        validate_positive_u32(
            self.nodes,
            &format!("service '{service_name}' x-slurm.nodes"),
        )?;
        validate_positive_u32(
            self.ntasks,
            &format!("service '{service_name}' x-slurm.ntasks"),
        )?;
        validate_positive_u32(
            self.ntasks_per_node,
            &format!("service '{service_name}' x-slurm.ntasks_per_node"),
        )?;
        validate_positive_u32(
            self.gpus_per_node,
            &format!("service '{service_name}' x-slurm.gpus_per_node"),
        )?;
        validate_positive_u32(
            self.gpus_per_task,
            &format!("service '{service_name}' x-slurm.gpus_per_task"),
        )?;
        validate_positive_u32(
            self.cpus_per_gpu,
            &format!("service '{service_name}' x-slurm.cpus_per_gpu"),
        )?;
        validate_sbatch_safe_string(
            self.mem_per_gpu.as_deref(),
            &format!("service '{service_name}' x-slurm.mem_per_gpu"),
        )?;
        validate_sbatch_safe_string(
            self.gpu_bind.as_deref(),
            &format!("service '{service_name}' x-slurm.gpu_bind"),
        )?;
        validate_sbatch_safe_string(
            self.cpu_bind.as_deref(),
            &format!("service '{service_name}' x-slurm.cpu_bind"),
        )?;
        validate_sbatch_safe_string(
            self.mem_bind.as_deref(),
            &format!("service '{service_name}' x-slurm.mem_bind"),
        )?;
        validate_sbatch_safe_string(
            self.distribution.as_deref(),
            &format!("service '{service_name}' x-slurm.distribution"),
        )?;
        validate_sbatch_safe_string(
            self.hint.as_deref(),
            &format!("service '{service_name}' x-slurm.hint"),
        )?;
        validate_extra_srun_arg_conflicts(self, service_name)?;
        if let Some(placement) = &self.placement {
            placement.validate(service_name)?;
        }
        if let Some(limit) = &self.time_limit {
            parse_slurm_time_limit(limit).with_context(|| {
                format!("service '{service_name}' x-slurm.time_limit is invalid")
            })?;
        }
        if self.mpi.is_some()
            && self
                .extra_srun_args
                .iter()
                .any(|arg| arg.trim_start().starts_with("--mpi"))
        {
            bail!(
                "service '{service_name}' sets both x-slurm.mpi and x-slurm.extra_srun_args with --mpi; use one service-level MPI source"
            );
        }
        if let Some(mpi) = &self.mpi {
            mpi.validate(service_name)?;
        }
        if let Some(prologue) = &self.prologue {
            prologue.validate(&format!("service '{service_name}' x-slurm.prologue"))?;
        }
        if let Some(epilogue) = &self.epilogue {
            epilogue.validate(&format!("service '{service_name}' x-slurm.epilogue"))?;
        }
        Ok(())
    }

    /// Returns true when either service hook requests container execution.
    #[must_use]
    pub fn has_container_hook(&self) -> bool {
        self.prologue
            .as_ref()
            .is_some_and(ServiceHookSpec::is_container)
            || self
                .epilogue
                .as_ref()
                .is_some_and(ServiceHookSpec::is_container)
    }

    /// Returns the validated per-service failure policy with defaults resolved.
    ///
    /// # Errors
    ///
    /// Returns an error when failure-policy fields are used with an
    /// incompatible mode or contain invalid values.
    pub fn normalized_failure_policy(&self, service_name: &str) -> Result<ServiceFailurePolicy> {
        const DEFAULT_MAX_RESTARTS: u32 = 3;
        const DEFAULT_BACKOFF_SECONDS: u64 = 5;
        const DEFAULT_WINDOW_SECONDS: u64 = 60;

        let Some(policy) = &self.failure_policy else {
            return Ok(ServiceFailurePolicy::default());
        };

        match policy.mode {
            ServiceFailureMode::FailJob | ServiceFailureMode::Ignore => {
                if policy.max_restarts.is_some()
                    || policy.backoff_seconds.is_some()
                    || policy.window_seconds.is_some()
                    || policy.max_restarts_in_window.is_some()
                {
                    bail!(
                        "service '{service_name}' sets x-slurm.failure_policy.max_restarts/backoff_seconds/window_seconds/max_restarts_in_window, but those fields are only valid when mode is restart_on_failure"
                    );
                }
                Ok(ServiceFailurePolicy {
                    mode: policy.mode,
                    max_restarts: 0,
                    backoff_seconds: 0,
                    window_seconds: 0,
                    max_restarts_in_window: 0,
                })
            }
            ServiceFailureMode::RestartOnFailure => {
                let max_restarts = policy.max_restarts.unwrap_or(DEFAULT_MAX_RESTARTS);
                let backoff_seconds = policy.backoff_seconds.unwrap_or(DEFAULT_BACKOFF_SECONDS);
                let window_seconds = policy.window_seconds.unwrap_or(DEFAULT_WINDOW_SECONDS);
                let max_restarts_in_window = policy.max_restarts_in_window.unwrap_or(max_restarts);
                if max_restarts == 0 {
                    bail!(
                        "service '{service_name}' sets x-slurm.failure_policy.max_restarts to 0; use a value of at least 1"
                    );
                }
                if backoff_seconds == 0 {
                    bail!(
                        "service '{service_name}' sets x-slurm.failure_policy.backoff_seconds to 0; use a value of at least 1"
                    );
                }
                if window_seconds == 0 {
                    bail!(
                        "service '{service_name}' sets x-slurm.failure_policy.window_seconds to 0; use a value of at least 1"
                    );
                }
                if max_restarts_in_window == 0 {
                    bail!(
                        "service '{service_name}' sets x-slurm.failure_policy.max_restarts_in_window to 0; use a value of at least 1"
                    );
                }
                Ok(ServiceFailurePolicy {
                    mode: policy.mode,
                    max_restarts,
                    backoff_seconds,
                    window_seconds,
                    max_restarts_in_window,
                })
            }
        }
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(placement) = &mut self.placement {
            placement.interpolate(vars)?;
        }
        interpolate_optional_string(&mut self.gres, vars)?;
        interpolate_optional_string(&mut self.mem_per_gpu, vars)?;
        interpolate_optional_string(&mut self.gpu_bind, vars)?;
        interpolate_optional_string(&mut self.cpu_bind, vars)?;
        interpolate_optional_string(&mut self.mem_bind, vars)?;
        interpolate_optional_string(&mut self.distribution, vars)?;
        interpolate_optional_string(&mut self.hint, vars)?;
        interpolate_optional_string(&mut self.time_limit, vars)?;
        interpolate_vec_strings(&mut self.extra_srun_args, vars)?;
        if let Some(mpi) = &mut self.mpi {
            mpi.interpolate(vars)?;
        }
        Ok(())
    }
}

impl MpiConfig {
    fn validate(&self, service_name: &str) -> Result<()> {
        validate_positive_u32(
            self.expected_ranks,
            &format!("service '{service_name}' x-slurm.mpi.expected_ranks"),
        )?;
        if let Some(host_mpi) = &self.host_mpi {
            host_mpi.validate(service_name)?;
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(host_mpi) = &mut self.host_mpi {
            host_mpi.interpolate(vars)?;
        }
        Ok(())
    }
}

impl HostMpiConfig {
    fn validate(&self, service_name: &str) -> Result<()> {
        for (index, mount) in self.bind_paths.iter().enumerate() {
            validate_mount_syntax(
                mount,
                &format!("service '{service_name}' x-slurm.mpi.host_mpi.bind_paths[{index}]"),
            )?;
        }
        self.env.to_pairs().with_context(|| {
            format!("service '{service_name}' x-slurm.mpi.host_mpi.env is invalid")
        })?;
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_vec_strings(&mut self.bind_paths, vars)?;
        self.env.interpolate_values(vars)?;
        Ok(())
    }
}

impl ServiceHookSpec {
    fn validate(&self, field: &str) -> Result<()> {
        validate_shell_hook_script(&self.script, field)
    }

    /// Returns true when this hook runs inside the service container.
    #[must_use]
    pub fn is_container(&self) -> bool {
        self.context == ServiceHookContext::Container
    }

    /// Returns true when this hook runs in the batch-script supervisor.
    #[must_use]
    pub fn is_host(&self) -> bool {
        self.context == ServiceHookContext::Host
    }
}

impl ServicePlacementSpec {
    fn validate(&self, service_name: &str) -> Result<()> {
        let selector_count = usize::from(self.node_range.is_some())
            + usize::from(self.node_count.is_some())
            + usize::from(self.node_percent.is_some())
            + usize::from(self.share_with.is_some());
        if selector_count != 1 {
            bail!(
                "service '{service_name}' x-slurm.placement must set exactly one of node_range, node_count, node_percent, or share_with"
            );
        }

        validate_node_index_expr(
            self.node_range.as_deref(),
            &format!("service '{service_name}' x-slurm.placement.node_range"),
        )?;
        validate_node_index_expr(
            self.exclude.as_deref(),
            &format!("service '{service_name}' x-slurm.placement.exclude"),
        )?;

        if let Some(count) = self.node_count
            && count == 0
        {
            bail!("service '{service_name}' x-slurm.placement.node_count must be at least 1");
        }
        if let Some(percent) = self.node_percent
            && !(1..=100).contains(&percent)
        {
            bail!(
                "service '{service_name}' x-slurm.placement.node_percent must be between 1 and 100"
            );
        }
        if let Some(target) = self.share_with.as_deref() {
            if target.trim().is_empty() {
                bail!("service '{service_name}' x-slurm.placement.share_with must not be empty");
            }
            if self.start_index.is_some() || self.exclude.is_some() {
                bail!(
                    "service '{service_name}' x-slurm.placement.share_with cannot be combined with start_index or exclude"
                );
            }
        } else if self.start_index.is_some()
            && self.node_count.is_none()
            && self.node_percent.is_none()
        {
            bail!(
                "service '{service_name}' x-slurm.placement.start_index is only valid with node_count or node_percent"
            );
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_optional_string(&mut self.node_range, vars)?;
        interpolate_optional_string(&mut self.share_with, vars)?;
        interpolate_optional_string(&mut self.exclude, vars)?;
        Ok(())
    }
}

fn validate_node_index_expr(value: Option<&str>, label: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        bail!("{label} must not be empty");
    }
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            bail!("{label} contains an empty range segment");
        }
        let (start, end) = match part.split_once('-') {
            Some((start, end)) => (start.trim(), end.trim()),
            None => (part, part),
        };
        if start.is_empty() || end.is_empty() {
            bail!("{label} contains an incomplete range '{part}'");
        }
        let start = start
            .parse::<u32>()
            .with_context(|| format!("{label} contains invalid node index '{start}'"))?;
        let end = end
            .parse::<u32>()
            .with_context(|| format!("{label} contains invalid node index '{end}'"))?;
        if end < start {
            bail!("{label} contains descending range '{part}'");
        }
    }
    Ok(())
}

impl EffectiveFailurePolicyConfig {
    fn from_policy(policy: &ServiceFailurePolicy) -> Self {
        let restart_mode = policy.mode == ServiceFailureMode::RestartOnFailure;
        Self {
            mode: policy.mode,
            max_restarts: restart_mode.then_some(policy.max_restarts),
            backoff_seconds: restart_mode.then_some(policy.backoff_seconds),
            window_seconds: restart_mode.then_some(policy.window_seconds),
            max_restarts_in_window: restart_mode.then_some(policy.max_restarts_in_window),
        }
    }
}

impl ServiceEnrootConfig {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(prepare) = &mut self.prepare {
            prepare.interpolate(vars)?;
        }
        Ok(())
    }
}

impl ServiceRuntimeConfig {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(prepare) = &mut self.prepare {
            prepare.interpolate(vars)?;
        }
        Ok(())
    }
}

impl PrepareSpec {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_vec_strings(&mut self.mounts, vars)?;
        self.env.interpolate_values(vars)?;
        Ok(())
    }
}

impl HealthcheckSpec {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(test) = &mut self.test {
            test.interpolate(vars)?;
        }
        Ok(())
    }
}

impl HealthcheckTest {
    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        match self {
            HealthcheckTest::Vec(items) => interpolate_vec_strings(items, vars),
            HealthcheckTest::String(command) => {
                *command = interpolate_string(command, vars)?;
                Ok(())
            }
        }
    }

    fn to_readiness(&self, timeout_seconds: Option<u64>) -> Result<ReadinessSpec> {
        let argv = match self {
            HealthcheckTest::Vec(items) => parse_healthcheck_argv(items)?,
            HealthcheckTest::String(command) => command
                .split_whitespace()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        };
        if let Some((host, port)) = parse_nc_probe(&argv)? {
            return Ok(ReadinessSpec::Tcp {
                host: Some(host),
                port,
                timeout_seconds,
            });
        }
        if let Some(url) = parse_http_probe(&argv) {
            return Ok(ReadinessSpec::Http {
                url,
                status_code: 200,
                timeout_seconds,
            });
        }
        bail!(
            "healthcheck.test must use a recognized nc, curl, or wget --spider probe; use explicit readiness for other checks"
        )
    }
}

impl HealthcheckDuration {
    fn to_seconds(&self) -> Result<u64> {
        match self {
            HealthcheckDuration::Seconds(seconds) => Ok(*seconds),
            HealthcheckDuration::String(raw) => parse_duration_seconds(raw),
        }
    }
}

/// Parses a Slurm-style walltime string into seconds.
///
/// Supports `MM`, `MM:SS`, `HH:MM:SS`, `D-HH`, `D-HH:MM`, and
/// `D-HH:MM:SS`.
///
/// # Errors
///
/// Returns an error when the input is empty or does not match a supported
/// Slurm walltime format.
pub fn parse_slurm_time_limit(input: &str) -> Result<u64> {
    let input = input.trim();
    if input.is_empty() {
        bail!("time limit must not be empty");
    }

    let (days, rest) = if let Some((days, rest)) = input.split_once('-') {
        (
            parse_walltime_component(days, "days")?,
            Some(rest.trim().to_string()),
        )
    } else {
        (0, None)
    };
    let rest = rest.as_deref().unwrap_or(input);
    let parts = rest
        .split(':')
        .map(|part| parse_walltime_component(part, "time component"))
        .collect::<Result<Vec<_>>>()?;
    let seconds = match parts.as_slice() {
        [minutes] => minutes.saturating_mul(60),
        [minutes, seconds] => minutes.saturating_mul(60).saturating_add(*seconds),
        [hours, minutes, seconds] => hours
            .saturating_mul(3_600)
            .saturating_add(minutes.saturating_mul(60))
            .saturating_add(*seconds),
        _ => bail!("unsupported Slurm time limit format '{input}'"),
    };

    if days == 0 {
        return Ok(seconds);
    }
    match parts.len() {
        1 => Ok(days
            .saturating_mul(86_400)
            .saturating_add(parts[0].saturating_mul(3_600))),
        2 => Ok(days
            .saturating_mul(86_400)
            .saturating_add(parts[0].saturating_mul(3_600))
            .saturating_add(parts[1].saturating_mul(60))),
        3 => Ok(days.saturating_mul(86_400).saturating_add(seconds)),
        _ => bail!("unsupported Slurm time limit format '{input}'"),
    }
}

fn parse_walltime_component(input: &str, label: &str) -> Result<u64> {
    input
        .parse::<u64>()
        .with_context(|| format!("invalid {label} value '{input}'"))
}

fn normalize_notify_events(events: &[NotifyEvent]) -> Vec<NotifyEvent> {
    if events.is_empty() {
        return vec![NotifyEvent::End, NotifyEvent::Fail];
    }
    if events.contains(&NotifyEvent::All) {
        return vec![NotifyEvent::All];
    }

    let mut normalized = Vec::new();
    for event in [NotifyEvent::Start, NotifyEvent::End, NotifyEvent::Fail] {
        if events.contains(&event) {
            normalized.push(event);
        }
    }
    normalized
}

fn notify_event_mail_type(event: NotifyEvent) -> &'static str {
    match event {
        NotifyEvent::Start => "BEGIN",
        NotifyEvent::End => "END",
        NotifyEvent::Fail => "FAIL",
        NotifyEvent::All => "ALL",
    }
}

fn validate_mpi_type_token(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("x-slurm.mpi.type must not be empty");
    }
    if value.starts_with('-') {
        bail!("x-slurm.mpi.type must not start with '-'");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
    {
        bail!(
            "x-slurm.mpi.type must be a safe single CLI token using letters, numbers, '_', '-', '.', or '+'"
        );
    }
    Ok(())
}

const FIRST_CLASS_TOP_LEVEL_SLURM_FLAGS: &[(&str, &str)] = &[
    ("gpus_per_node", "--gpus-per-node"),
    ("gpus_per_task", "--gpus-per-task"),
    ("cpus_per_gpu", "--cpus-per-gpu"),
    ("mem_per_gpu", "--mem-per-gpu"),
    ("gpu_bind", "--gpu-bind"),
    ("cpu_bind", "--cpu-bind"),
    ("mem_bind", "--mem-bind"),
    ("distribution", "--distribution"),
    ("hint", "--hint"),
];

const FIRST_CLASS_SERVICE_SLURM_FLAGS: &[(&str, &str)] = &[
    ("gpus_per_node", "--gpus-per-node"),
    ("gpus_per_task", "--gpus-per-task"),
    ("cpus_per_gpu", "--cpus-per-gpu"),
    ("mem_per_gpu", "--mem-per-gpu"),
    ("gpu_bind", "--gpu-bind"),
    ("cpu_bind", "--cpu-bind"),
    ("mem_bind", "--mem-bind"),
    ("distribution", "--distribution"),
    ("hint", "--hint"),
];

fn validate_submit_arg_conflicts(slurm: &SlurmConfig) -> Result<()> {
    for (field, flag) in FIRST_CLASS_TOP_LEVEL_SLURM_FLAGS {
        if top_level_slurm_field_is_set(slurm, field)
            && slurm
                .submit_args
                .iter()
                .any(|arg| raw_arg_has_flag(arg, flag))
        {
            bail!("x-slurm.{field} cannot be combined with raw {flag} in x-slurm.submit_args");
        }
    }
    Ok(())
}

fn validate_extra_srun_arg_conflicts(slurm: &ServiceSlurmConfig, service_name: &str) -> Result<()> {
    for (field, flag) in FIRST_CLASS_SERVICE_SLURM_FLAGS {
        if service_slurm_field_is_set(slurm, field)
            && slurm
                .extra_srun_args
                .iter()
                .any(|arg| raw_arg_has_flag(arg, flag))
        {
            bail!(
                "service '{service_name}' x-slurm.{field} cannot be combined with raw {flag} in service-level x-slurm.extra_srun_args"
            );
        }
    }
    Ok(())
}

fn top_level_slurm_field_is_set(slurm: &SlurmConfig, field: &str) -> bool {
    match field {
        "gpus_per_node" => slurm.gpus_per_node.is_some(),
        "gpus_per_task" => slurm.gpus_per_task.is_some(),
        "cpus_per_gpu" => slurm.cpus_per_gpu.is_some(),
        "mem_per_gpu" => slurm.mem_per_gpu.is_some(),
        "gpu_bind" => slurm.gpu_bind.is_some(),
        "cpu_bind" => slurm.cpu_bind.is_some(),
        "mem_bind" => slurm.mem_bind.is_some(),
        "distribution" => slurm.distribution.is_some(),
        "hint" => slurm.hint.is_some(),
        _ => false,
    }
}

fn service_slurm_field_is_set(slurm: &ServiceSlurmConfig, field: &str) -> bool {
    match field {
        "gpus_per_node" => slurm.gpus_per_node.is_some(),
        "gpus_per_task" => slurm.gpus_per_task.is_some(),
        "cpus_per_gpu" => slurm.cpus_per_gpu.is_some(),
        "mem_per_gpu" => slurm.mem_per_gpu.is_some(),
        "gpu_bind" => slurm.gpu_bind.is_some(),
        "cpu_bind" => slurm.cpu_bind.is_some(),
        "mem_bind" => slurm.mem_bind.is_some(),
        "distribution" => slurm.distribution.is_some(),
        "hint" => slurm.hint.is_some(),
        _ => false,
    }
}

fn raw_arg_has_flag(arg: &str, flag: &str) -> bool {
    let trimmed = arg.trim_start();
    trimmed == flag
        || trimmed
            .strip_prefix(flag)
            .is_some_and(|rest| rest.starts_with('=') || rest.starts_with(char::is_whitespace))
}

fn submit_args_contain_mail_settings(args: &[String]) -> bool {
    args.iter().any(|arg| {
        let trimmed = arg.trim();
        trimmed.starts_with("--mail-user")
            || trimmed.starts_with("--mail-type")
            || trimmed.starts_with("mail-user")
            || trimmed.starts_with("mail-type")
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::env;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    use proptest::prelude::*;
    use proptest::string::string_regex;

    use super::*;

    fn write_spec(tmpdir: &Path, body: &str) -> std::path::PathBuf {
        let path = tmpdir.join("compose.yaml");
        fs::write(&path, body).expect("write compose");
        path
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn prop_config() -> ProptestConfig {
        ProptestConfig {
            cases: 64,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        string_regex("[A-Za-z_][A-Za-z0-9_-]{0,15}").expect("key regex")
    }

    fn value_strategy() -> impl Strategy<Value = String> {
        string_regex("[A-Za-z0-9_./:-]{0,12}").expect("value regex")
    }

    #[test]
    fn load_minimal_spec_uses_defaults() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
name: demo
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(spec.name.as_deref(), Some("demo"));
        assert_eq!(spec.services.len(), 1);
        assert!(spec.slurm.cache_dir.is_none());
        let service = spec.services.get("app").expect("service");
        assert!(service.command.is_none());
        assert!(service.volumes.is_empty());
    }

    #[test]
    fn rejects_build_with_actionable_message() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    build: .
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("build is not supported in v1"));
        assert!(err.to_string().contains("x-runtime.prepare"));
    }

    #[test]
    fn rejects_ports_with_actionable_message() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    ports:
      - "6379:6379"
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("ports are not supported"));
    }

    #[test]
    fn rejects_unknown_service_key() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    mystery: true
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("unsupported key 'mystery'"));
    }

    #[test]
    fn service_hooks_accept_shorthand_and_explicit_context() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  trainer:
    image: trainer:latest
    command: python train.py
    x-slurm:
      prologue: |
        module load cuda/12.1
        nvidia-smi
      epilogue:
        context: container
        script: |
          tar czf /shared/logs-${SLURM_JOB_ID}.tar.gz /hpc-compose/job/logs
"#,
        );

        let spec = ComposeSpec::load(&path).expect("load spec");
        let service = spec.services.get("trainer").expect("trainer");
        let prologue = service.slurm.prologue.as_ref().expect("prologue");
        assert_eq!(prologue.context, ServiceHookContext::Host);
        assert!(prologue.script.contains("module load cuda/12.1"));
        let epilogue = service.slurm.epilogue.as_ref().expect("epilogue");
        assert_eq!(epilogue.context, ServiceHookContext::Container);
        assert!(epilogue.script.contains("${SLURM_JOB_ID}"));
    }

    #[test]
    fn service_hooks_reject_empty_scripts_and_unknown_fields() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let empty = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      prologue: ""
"#,
        );
        let err = ComposeSpec::load(&empty).expect_err("empty hook should fail");
        assert!(err.to_string().contains("x-slurm.prologue"));
        assert!(err.to_string().contains("must not be empty"));

        let unknown = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      epilogue:
        script: echo done
        where: host
"#,
        );
        let err = ComposeSpec::load(&unknown).expect_err("unknown hook field should fail");
        let message = err.to_string();
        assert!(
            message.contains("failed to deserialize spec") || message.contains("unknown field"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_non_mapping_root() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(tmpdir.path(), "- not-a-mapping\n");
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("top-level YAML document must be a mapping")
        );
    }

    #[test]
    fn rejects_missing_services() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(tmpdir.path(), "name: demo\n");
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("top-level 'services'"));
    }

    #[test]
    fn environment_list_requires_key_value_pairs() {
        let env = EnvironmentSpec::List(vec!["GOOD=1".into(), "BROKEN".into()]);
        let err = env.to_pairs().expect_err("should fail");
        assert!(err.to_string().contains("KEY=VALUE"));
    }

    #[test]
    fn depends_on_map_rejects_unsupported_condition() {
        let deps = DependsOnSpec::Map(BTreeMap::from([(
            "redis".into(),
            DependsOnConditionSpec {
                condition: Some("service_ready".into()),
            },
        )]));
        let err = deps.entries().expect_err("should fail");
        assert!(err.to_string().contains("service_completed_successfully"));
    }

    #[test]
    fn depends_on_map_accepts_started_healthy_and_completed_successfully() {
        let deps = DependsOnSpec::Map(BTreeMap::from([
            (
                "redis".into(),
                DependsOnConditionSpec {
                    condition: Some("service_started".into()),
                },
            ),
            (
                "db".into(),
                DependsOnConditionSpec {
                    condition: Some("service_healthy".into()),
                },
            ),
            (
                "preprocess".into(),
                DependsOnConditionSpec {
                    condition: Some("service_completed_successfully".into()),
                },
            ),
        ]));
        assert_eq!(
            deps.entries().expect("entries"),
            vec![
                ServiceDependency {
                    name: "db".into(),
                    condition: DependencyCondition::ServiceHealthy,
                },
                ServiceDependency {
                    name: "preprocess".into(),
                    condition: DependencyCondition::ServiceCompletedSuccessfully,
                },
                ServiceDependency {
                    name: "redis".into(),
                    condition: DependencyCondition::ServiceStarted,
                },
            ]
        );
    }

    #[test]
    fn command_accessors_match_variants() {
        let string_cmd = CommandSpec::String("echo hi".into());
        assert!(string_cmd.is_string());
        assert_eq!(string_cmd.as_string(), Some("echo hi"));
        assert!(string_cmd.as_vec().is_none());

        let vec_cmd = CommandSpec::Vec(vec!["python".into(), "-m".into(), "main".into()]);
        assert!(!vec_cmd.is_string());
        assert!(vec_cmd.as_string().is_none());
        assert_eq!(
            vec_cmd.as_vec(),
            Some(&["python".to_string(), "-m".to_string(), "main".to_string()][..])
        );
    }

    #[test]
    fn metrics_block_defaults_to_enabled_interval_and_collectors() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  metrics: {}
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert!(spec.slurm.metrics_enabled());
        assert_eq!(spec.slurm.metrics_interval_seconds(), 5);
        assert_eq!(
            spec.slurm.metrics_collectors(),
            vec![MetricsCollector::Gpu, MetricsCollector::Slurm]
        );
    }

    #[test]
    fn metrics_block_rejects_zero_interval() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  metrics:
    interval_seconds: 0
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("interval_seconds"));
    }

    #[test]
    fn metrics_block_rejects_unknown_collectors() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  metrics:
    collectors: [gpu, mystery]
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn artifacts_block_defaults_to_always_and_accepts_job_mount_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths:
      - /hpc-compose/job/metrics/**
      - /hpc-compose/job/checkpoints/*.pt
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert!(spec.slurm.artifacts_enabled());
        assert_eq!(
            spec.slurm.artifacts_collect_policy(),
            ArtifactCollectPolicy::Always
        );
        let artifacts = spec.slurm.artifacts.expect("artifacts");
        assert_eq!(artifacts.export_dir.as_deref(), Some("./results"));
        assert_eq!(artifacts.paths.len(), 2);
    }

    #[test]
    fn artifacts_block_rejects_missing_export_dir() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("artifacts.export_dir"));
    }

    #[test]
    fn artifacts_block_rejects_empty_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths: []
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("must contain at least one source path")
        );
    }

    #[test]
    fn resume_block_accepts_absolute_shared_path() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume:
    path: /shared/runs/demo
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(spec.slurm.resume_dir(), Some("/shared/runs/demo"));
    }

    #[test]
    fn resume_block_interpolates_env_values() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let env_file = tmpdir.path().join(".env");
        fs::write(&env_file, "RUN_ID=exp-42\n").expect("env");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume:
    path: /shared/$RUN_ID
services:
  app:
    image: redis:7
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(spec.slurm.resume_dir(), Some("/shared/exp-42"));
    }

    #[test]
    fn resume_block_rejects_missing_relative_empty_and_container_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");

        let missing = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume: {}
services:
  app:
    image: redis:7
"#,
        );
        assert!(ComposeSpec::load(&missing).is_err());

        let empty = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume:
    path: ""
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&empty).expect_err("empty");
        assert!(err.to_string().contains("resume.path"));

        let relative = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume:
    path: ./runs/demo
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&relative).expect_err("relative");
        assert!(err.to_string().contains("absolute host path"));

        let container = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  resume:
    path: /hpc-compose/resume/demo
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&container).expect_err("container");
        assert!(err.to_string().contains("host path"));
    }

    #[test]
    fn artifacts_block_rejects_reserved_default_bundle_name() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    bundles:
      default:
        paths:
          - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("bundle name 'default' is reserved")
        );
    }

    #[test]
    fn artifacts_block_rejects_non_absolute_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths:
      - ./checkpoints/*.pt
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("must be absolute"));
    }

    #[test]
    fn artifacts_block_rejects_paths_outside_job_mount() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths:
      - /tmp/output.txt
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(err.to_string().contains("/hpc-compose/job"));
    }

    #[test]
    fn artifacts_block_rejects_recursive_artifacts_sources() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths:
      - /hpc-compose/job/artifacts/**
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("must not read from /hpc-compose/job/artifacts")
        );
    }

    #[test]
    fn readiness_variants_deserialize() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  tcp:
    image: redis:7
    readiness:
      type: tcp
      port: 6379
      host: 127.0.0.1
      timeout_seconds: 30
  log:
    image: redis:7
    readiness:
      type: log
      pattern: ready
      timeout_seconds: 10
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        assert_eq!(
            spec.services
                .get("tcp")
                .and_then(|svc| svc.readiness.clone()),
            Some(ReadinessSpec::Tcp {
                port: 6379,
                host: Some("127.0.0.1".into()),
                timeout_seconds: Some(30),
            })
        );
        assert_eq!(
            spec.services
                .get("log")
                .and_then(|svc| svc.readiness.clone()),
            Some(ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: Some(10),
            })
        );
    }

    #[test]
    fn healthcheck_cmd_normalizes_to_tcp_readiness() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  redis:
    image: redis:7
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "6379"]
      timeout: 30s
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        let service = spec.services.get("redis").expect("service");
        assert!(service.healthcheck.is_none());
        assert_eq!(
            service.readiness,
            Some(ReadinessSpec::Tcp {
                host: Some("127.0.0.1".into()),
                port: 6379,
                timeout_seconds: Some(30),
            })
        );
    }

    #[test]
    fn healthcheck_shell_normalizes_to_http_readiness() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  api:
    image: python:3.11
    healthcheck:
      test:
        - CMD-SHELL
        - curl --silent --fail http://127.0.0.1:8080/health
      timeout: 2m
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        let service = spec.services.get("api").expect("service");
        assert_eq!(
            service.readiness,
            Some(ReadinessSpec::Http {
                url: "http://127.0.0.1:8080/health".into(),
                status_code: 200,
                timeout_seconds: Some(120),
            })
        );
    }

    #[test]
    fn healthcheck_disable_and_validation_errors_are_enforced() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let disabled = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    healthcheck:
      disable: true
"#,
        );
        let spec = ComposeSpec::load(&disabled).expect("load");
        assert!(
            spec.services
                .get("app")
                .and_then(|service| service.readiness.as_ref())
                .is_none()
        );

        let conflict = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    readiness:
      type: sleep
      seconds: 1
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "6379"]
"#,
        );
        let err = ComposeSpec::load(&conflict).expect_err("conflict");
        assert!(err.to_string().contains("mutually exclusive"));

        let unsupported = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    healthcheck:
      test: ["CMD", "echo", "ok"]
      interval: 5s
"#,
        );
        let err = ComposeSpec::load(&unsupported).expect_err("unsupported");
        assert!(err.to_string().contains("healthcheck.interval"));
    }

    #[test]
    fn healthcheck_helper_parsers_cover_remaining_error_paths() {
        assert!(parse_healthcheck_argv(&[]).is_err());
        assert!(parse_healthcheck_argv(&["CMD".into()]).is_err());
        assert!(parse_healthcheck_argv(&["CMD-SHELL".into()]).is_err());
        assert!(parse_healthcheck_argv(&["NONE".into(), "echo".into()]).is_err());

        assert_eq!(
            parse_nc_probe(&["curl".into(), "http://127.0.0.1".into()]).expect("non nc"),
            None
        );
        assert!(parse_nc_probe(&["nc".into(), "127.0.0.1".into(), "80".into()]).is_err());
        assert!(parse_nc_probe(&["nc".into(), "-z".into(), "127.0.0.1".into()]).is_err());
        assert!(
            parse_nc_probe(&["nc".into(), "-z".into(), "127.0.0.1".into(), "nope".into()]).is_err()
        );
        assert_eq!(
            parse_nc_probe(&[
                "nc".into(),
                "-v".into(),
                "-z".into(),
                "127.0.0.1".into(),
                "8080".into(),
            ])
            .expect("nc")
            .expect("some"),
            ("127.0.0.1".into(), 8080)
        );

        assert_eq!(
            parse_http_probe(&[
                "wget".into(),
                "--spider".into(),
                "http://127.0.0.1:8080/health".into(),
            ]),
            Some("http://127.0.0.1:8080/health".into())
        );
        assert_eq!(
            parse_http_probe(&["wget".into(), "http://127.0.0.1:8080/health".into()]),
            None
        );
    }

    #[test]
    fn healthcheck_duration_and_conversion_helpers_cover_remaining_branches() {
        assert_eq!(
            HealthcheckDuration::Seconds(7)
                .to_seconds()
                .expect("seconds"),
            7
        );
        assert_eq!(
            parse_duration_seconds("15").expect("plain integer seconds"),
            15
        );
        assert_eq!(
            parse_duration_seconds("1h2m3s").expect("compound duration"),
            3723
        );
        assert!(parse_duration_seconds("").is_err());
        assert!(parse_duration_seconds("ms").is_err());
        assert!(parse_duration_seconds("7q").is_err());
        assert!(parse_duration_seconds("7m30").is_err());

        let mut vars = BTreeMap::new();
        vars.insert("PORT".into(), "9090".into());
        let mut test = HealthcheckTest::String("curl http://127.0.0.1:${PORT}/ready".into());
        test.interpolate(&vars).expect("interpolate");
        assert_eq!(
            test.to_readiness(Some(12)).expect("http readiness"),
            ReadinessSpec::Http {
                url: "http://127.0.0.1:9090/ready".into(),
                status_code: 200,
                timeout_seconds: Some(12),
            }
        );

        let unsupported = HealthcheckTest::String("echo ok".into());
        assert!(unsupported.to_readiness(None).is_err());
    }

    #[test]
    fn artifact_and_interpolation_validation_cover_remaining_error_paths() {
        assert!(validate_artifact_bundle_name("bad.name").is_err());

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let empty_export_dir = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: "   "
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
        );
        assert!(
            ComposeSpec::load(&empty_export_dir)
                .expect_err("empty export")
                .to_string()
                .contains("must not be empty")
        );

        let empty_bundle_paths = write_spec(
            tmpdir.path(),
            r#"
x-slurm:
  artifacts:
    export_dir: ./results
    bundles:
      logs:
        paths: []
services:
  app:
    image: redis:7
"#,
        );
        assert!(
            ComposeSpec::load(&empty_bundle_paths)
                .expect_err("empty bundle")
                .to_string()
                .contains("bundles.logs.paths must contain at least one source path")
        );

        let bad_healthcheck = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "6379"]
      retries: 2
"#,
        );
        assert!(
            ComposeSpec::load(&bad_healthcheck)
                .expect_err("retries")
                .to_string()
                .contains("healthcheck.retries")
        );

        let start_period = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "6379"]
      start_period: 5s
"#,
        );
        assert!(
            ComposeSpec::load(&start_period)
                .expect_err("start period")
                .to_string()
                .contains("healthcheck.start_period")
        );

        let list_env = EnvironmentSpec::List(vec!["BROKEN".into()]);
        assert!(list_env.to_pairs().is_err());

        let mut list_env = EnvironmentSpec::List(vec!["URL=http://${HOST}".into()]);
        let mut vars = BTreeMap::new();
        vars.insert("HOST".into(), "localhost".into());
        list_env.interpolate_values(&vars).expect("interpolate env");
        assert_eq!(
            list_env.to_pairs().expect("pairs"),
            vec![("URL".into(), "http://localhost".into())]
        );

        let deps = DependsOnSpec::Map(BTreeMap::from([(
            "db".into(),
            DependsOnConditionSpec {
                condition: Some("service_healthy".into()),
            },
        )]));
        assert_eq!(deps.names().expect("names"), vec!["db".to_string()]);
    }

    #[test]
    fn parse_and_structure_errors_cover_remaining_validation_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");

        let invalid_yaml = write_spec(tmpdir.path(), "services: [\n");
        let err = ComposeSpec::load(&invalid_yaml).expect_err("invalid yaml");
        assert!(err.to_string().contains("failed to parse YAML"));

        let non_mapping_services = write_spec(
            tmpdir.path(),
            r#"
services:
  - app
"#,
        );
        let err = ComposeSpec::load(&non_mapping_services).expect_err("services mapping");
        assert!(err.to_string().contains("'services' must be a mapping"));

        let non_mapping_service = write_spec(
            tmpdir.path(),
            r#"
services:
  app: hello
"#,
        );
        let err = ComposeSpec::load(&non_mapping_service).expect_err("service mapping");
        assert!(err.to_string().contains("service 'app' must be a mapping"));

        let root_unknown = write_spec(
            tmpdir.path(),
            r#"
version: "3"
unknown: true
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&root_unknown).expect_err("root unknown");
        assert!(
            err.to_string()
                .contains("root uses unsupported key 'unknown'")
        );

        let networks = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    networks: [default]
"#,
        );
        let err = ComposeSpec::load(&networks).expect_err("networks");
        assert!(err.to_string().contains("custom container networking"));

        let restart = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    restart: always
"#,
        );
        let err = ComposeSpec::load(&restart).expect_err("restart");
        assert!(err.to_string().contains("x-slurm.failure_policy"));

        let deploy = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    deploy: {}
"#,
        );
        let err = ComposeSpec::load(&deploy).expect_err("deploy");
        assert!(err.to_string().contains("long-running orchestrator"));
    }

    #[test]
    fn environment_map_and_command_defaults_cover_remaining_helpers() {
        let env = EnvironmentSpec::Map(BTreeMap::from([("A".into(), "B".into())]));
        assert_eq!(
            env.to_pairs().expect("pairs"),
            vec![("A".into(), "B".into())]
        );
        assert!(default_true());
    }

    #[test]
    fn deserialize_and_key_type_errors_cover_last_branches() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");

        let bad_image_type = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: [redis:7]
"#,
        );
        let err = ComposeSpec::load(&bad_image_type).expect_err("deserialize");
        assert!(err.to_string().contains("failed to deserialize"));

        let numeric_service_name = write_spec(
            tmpdir.path(),
            r#"
services:
  1:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&numeric_service_name).expect_err("non-string service");
        assert!(err.to_string().contains("service names must be strings"));

        let non_string_root_key = write_spec(
            tmpdir.path(),
            r#"
1: true
services:
  app:
    image: redis:7
"#,
        );
        let err = ComposeSpec::load(&non_string_root_key).expect_err("non-string key");
        assert!(err.to_string().contains("root contains a non-string key"));
    }

    #[test]
    fn env_file_interpolates_selected_fields() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::write(
            tmpdir.path().join(".env"),
            "IMAGE=python:3.11-slim\nSRC_DIR=app\nARG=main\nTOKEN=from-dotenv\n",
        )
        .expect("dotenv");
        fs::create_dir_all(tmpdir.path().join("app")).expect("app");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: ${IMAGE}
    working_dir: ${WORKDIR:-/workspace}
    volumes:
      - ./${SRC_DIR}:/workspace
    environment:
      SECRET_TOKEN: ${TOKEN}
      FALLBACK: ${MISSING:-fallback}
    command:
      - python
      - -m
      - ${ARG}
    x-enroot:
      prepare:
        commands:
          - echo $TOKEN
        env:
          PREP_TOKEN: ${TOKEN}
  shell:
    image: redis:7
    command: echo $TOKEN
"#,
        );

        let spec = ComposeSpec::load(&path).expect("load");
        let app = spec.services.get("app").expect("app");
        assert_eq!(app.image.as_deref(), Some("python:3.11-slim"));
        assert_eq!(app.working_dir.as_deref(), Some("/workspace"));
        assert_eq!(app.volumes, vec!["./app:/workspace".to_string()]);
        assert_eq!(
            app.environment.to_pairs().expect("env"),
            vec![
                ("FALLBACK".into(), "fallback".into()),
                ("SECRET_TOKEN".into(), "from-dotenv".into()),
            ]
        );
        assert_eq!(
            app.command.as_ref().and_then(CommandSpec::as_vec),
            Some(&["python".to_string(), "-m".to_string(), "main".to_string()][..])
        );
        assert_eq!(
            app.enroot
                .prepare
                .as_ref()
                .expect("prepare")
                .env
                .to_pairs()
                .expect("prepare env"),
            vec![("PREP_TOKEN".into(), "from-dotenv".into())]
        );
        assert_eq!(
            app.enroot.prepare.as_ref().expect("prepare").commands,
            vec!["echo $TOKEN".to_string()]
        );
        assert_eq!(
            spec.services
                .get("shell")
                .and_then(|svc| svc.command.as_ref())
                .and_then(CommandSpec::as_string),
            Some("echo $TOKEN")
        );
    }

    #[test]
    fn shell_environment_overrides_dotenv_and_default_operators_work() {
        let _guard = env_lock().lock().expect("env lock");
        let old_image = env::var_os("IMAGE");
        let old_empty = env::var_os("EMPTY");
        unsafe {
            env::set_var("IMAGE", "redis:7");
            env::set_var("EMPTY", "");
        }

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::write(tmpdir.path().join(".env"), "IMAGE=python:3.11-slim\n").expect("dotenv");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: ${IMAGE}
    environment:
      DASH: ${EMPTY-default}
      COLON: ${EMPTY:-default}
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        let env_pairs = spec
            .services
            .get("app")
            .expect("app")
            .environment
            .to_pairs()
            .expect("pairs");
        assert_eq!(
            spec.services.get("app").expect("app").image.as_deref(),
            Some("redis:7")
        );
        assert_eq!(
            env_pairs,
            vec![
                ("COLON".into(), "default".into()),
                ("DASH".into(), "".into())
            ]
        );

        match old_image {
            Some(value) => unsafe { env::set_var("IMAGE", value) },
            None => unsafe { env::remove_var("IMAGE") },
        }
        match old_empty {
            Some(value) => unsafe { env::set_var("EMPTY", value) },
            None => unsafe { env::remove_var("EMPTY") },
        }
    }

    #[test]
    fn nested_default_interpolation_resolves_correct_values() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    environment:
      KEEP: "${A:-${B:-fallback}}"
"#,
        );

        let spec = ComposeSpec::load_with_interpolation_vars(
            &path,
            &BTreeMap::from([("A".to_string(), "present".to_string())]),
        )
        .expect("outer value");
        assert_eq!(
            spec.services
                .get("app")
                .expect("app")
                .environment
                .to_pairs()
                .expect("pairs"),
            vec![("KEEP".into(), "present".into())]
        );

        let spec = ComposeSpec::load_with_interpolation_vars(
            &path,
            &BTreeMap::from([("B".to_string(), "inner".to_string())]),
        )
        .expect("inner value");
        assert_eq!(
            spec.services
                .get("app")
                .expect("app")
                .environment
                .to_pairs()
                .expect("pairs"),
            vec![("KEEP".into(), "inner".into())]
        );

        let spec =
            ComposeSpec::load_with_interpolation_vars(&path, &BTreeMap::new()).expect("fallback");
        assert_eq!(
            spec.services
                .get("app")
                .expect("app")
                .environment
                .to_pairs()
                .expect("pairs"),
            vec![("KEEP".into(), "fallback".into())]
        );
    }

    #[test]
    fn strict_env_scanner_handles_nested_defaults_and_escaped_dollars() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    environment:
      KEEP: "${A:-${B:-fallback}}"
      ESCAPED: "$${C:-literal}"
"#,
        );

        let missing = missing_defaulted_variables(
            &path,
            &BTreeMap::from([("A".to_string(), "present".to_string())]),
        )
        .expect("scan");
        assert!(missing.is_empty());

        let missing = missing_defaulted_variables(
            &path,
            &BTreeMap::from([("B".to_string(), "inner".to_string())]),
        )
        .expect("scan");
        assert_eq!(missing, BTreeSet::from(["A".to_string()]));

        let missing = missing_defaulted_variables(&path, &BTreeMap::new()).expect("scan");
        assert_eq!(missing, BTreeSet::from(["A".to_string(), "B".to_string()]));
    }

    #[test]
    fn strict_env_scanner_ignores_yaml_comments_and_mapping_keys() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    environment:
      "${IGNORED_KEY:-key}": fixed
      KEEP: "${A:-ok}"
    # ${IGNORED_COMMENT:-comment}
"#,
        );

        let missing = missing_defaulted_variables(
            &path,
            &BTreeMap::from([("A".to_string(), "present".to_string())]),
        )
        .expect("scan");
        assert!(missing.is_empty());
    }

    #[test]
    fn strict_env_scanner_reports_malformed_placeholders_without_panicking() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    environment:
      KEEP: "${}"
"#,
        );

        let outcome =
            std::panic::catch_unwind(|| missing_defaulted_variables(&path, &BTreeMap::new()));
        let result = outcome.expect("malformed strict-env scan should not panic");
        let err = result.expect_err("malformed placeholder should fail");
        assert!(err.to_string().contains("invalid variable expression"));
    }

    #[test]
    fn missing_variable_without_default_is_an_error() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: ${IMAGE}
"#,
        );
        let err = ComposeSpec::load(&path).expect_err("missing");
        assert!(err.to_string().contains("missing variable 'IMAGE'"));
    }

    #[test]
    fn http_readiness_deserializes_with_defaults() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  api:
    image: python:3.11
    readiness:
      type: http
      url: http://127.0.0.1:8080/health
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        let service = spec.services.get("api").expect("service");
        match service.readiness.as_ref().expect("readiness") {
            ReadinessSpec::Http {
                url,
                status_code,
                timeout_seconds,
            } => {
                assert_eq!(url, "http://127.0.0.1:8080/health");
                assert_eq!(*status_code, 200);
                assert_eq!(*timeout_seconds, None);
            }
            other => panic!("expected Http readiness, got {:?}", other),
        }
    }

    #[test]
    fn http_readiness_deserializes_with_custom_values() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = write_spec(
            tmpdir.path(),
            r#"
services:
  api:
    image: python:3.11
    readiness:
      type: http
      url: http://localhost:9000/ready
      status_code: 204
      timeout_seconds: 120
"#,
        );
        let spec = ComposeSpec::load(&path).expect("load");
        let service = spec.services.get("api").expect("service");
        match service.readiness.as_ref().expect("readiness") {
            ReadinessSpec::Http {
                url,
                status_code,
                timeout_seconds,
            } => {
                assert_eq!(url, "http://localhost:9000/ready");
                assert_eq!(*status_code, 204);
                assert_eq!(*timeout_seconds, Some(120));
            }
            other => panic!("expected Http readiness, got {:?}", other),
        }
    }

    #[test]
    fn service_mpi_config_deserializes_supported_types() {
        for raw in ["pmix", "pmi2", "pmi1", "openmpi", "pmix_v4"] {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                &format!(
                    r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: {raw}
"#
                ),
            );
            let spec = ComposeSpec::load(&path).expect("load");
            let mpi = spec
                .services
                .get("app")
                .expect("service")
                .slurm
                .mpi
                .as_ref()
                .expect("mpi");
            assert_eq!(mpi.mpi_type.as_srun_value(), raw);
        }
    }

    #[test]
    fn service_placement_deserializes_interpolates_and_validates() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let valid = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "${APP_RANGE:-0-2}"
        exclude: "${APP_EXCLUDE:-1}"
        allow_overlap: true
"#,
        );
        let spec = ComposeSpec::load(&valid).expect("load");
        let placement = spec
            .services
            .get("app")
            .expect("service")
            .slurm
            .placement
            .as_ref()
            .expect("placement");
        assert_eq!(placement.node_range.as_deref(), Some("0-2"));
        assert_eq!(placement.exclude.as_deref(), Some("1"));
        assert!(placement.allow_overlap);

        for (name, body, needle) in [
            (
                "missing-selector",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        allow_overlap: true
"#,
                "exactly one",
            ),
            (
                "multiple-selectors",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "0-1"
        node_count: 2
"#,
                "exactly one",
            ),
            (
                "zero-count",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_count: 0
"#,
                "node_count must be at least 1",
            ),
            (
                "bad-percent",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_percent: 101
"#,
                "node_percent must be between 1 and 100",
            ),
            (
                "share-with-exclude",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        share_with: workers
        exclude: "0"
"#,
                "share_with cannot be combined",
            ),
            (
                "start-index-with-range",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "0-1"
        start_index: 1
"#,
                "start_index is only valid",
            ),
            (
                "descending-range",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "3-1"
"#,
                "descending range",
            ),
            (
                "empty-exclude-segment",
                r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_count: 2
        exclude: "0,,2"
"#,
                "empty range segment",
            ),
        ] {
            let path = write_spec(tmpdir.path(), body);
            let err = ComposeSpec::load(&path).unwrap_err();
            assert!(
                err.to_string().contains(needle),
                "{name}: expected error containing '{needle}', got {err}"
            );
        }
    }

    #[test]
    fn service_mpi_rejects_invalid_type_and_raw_mpi_conflict() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let invalid = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: "pmix v4"
"#,
        );
        let err = ComposeSpec::load(&invalid).expect_err("invalid mpi type");
        assert!(err.to_string().contains("failed to deserialize spec"));

        let conflict = write_spec(
            tmpdir.path(),
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
      extra_srun_args:
        - --mpi=pmi2
"#,
        );
        let err = ComposeSpec::load(&conflict).expect_err("mpi conflict");
        assert!(err.to_string().contains("use one MPI source"));
    }

    #[test]
    fn slurm_config_rejects_newlines_in_sbatch_fields() {
        let config = SlurmConfig {
            job_name: Some("valid-name".to_string()),
            ..SlurmConfig::default()
        };
        assert!(config.validate().is_ok());

        let config = SlurmConfig {
            job_name: Some("bad\nname".to_string()),
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("newline in job_name");
        assert!(err.to_string().contains("x-slurm.job-name"));

        let config = SlurmConfig {
            partition: Some("bad\0partition".to_string()),
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("null in partition");
        assert!(err.to_string().contains("x-slurm.partition"));

        let config = SlurmConfig {
            output: Some("bad\rpath".to_string()),
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("line break in output");
        assert!(err.to_string().contains("x-slurm.output"));

        let config = SlurmConfig {
            submit_args: vec![
                "--reservation=ok".to_string(),
                "--comment=bad\narg".to_string(),
            ],
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("line break in submit arg");
        assert!(err.to_string().contains("x-slurm.submit_args[1]"));
    }

    #[test]
    fn slurm_binding_fields_reject_raw_flag_conflicts() {
        let config = SlurmConfig {
            gpus_per_node: Some(4),
            submit_args: vec!["--gpus-per-node=8".into()],
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("top-level conflict");
        assert!(err.to_string().contains("gpus_per_node"));

        let service = ServiceSlurmConfig {
            gpu_bind: Some("closest".into()),
            extra_srun_args: vec!["--gpu-bind=none".into()],
            ..ServiceSlurmConfig::default()
        };
        let err = service.validate("trainer").expect_err("service conflict");
        assert!(err.to_string().contains("gpu_bind"));
    }

    proptest! {
        #![proptest_config(prop_config())]

        #[test]
        fn property_rejects_unsupported_root_keys(
            key in key_strategy().prop_filter("unsupported root key", |key| {
                !matches!(key.as_str(), "name" | "services" | "version" | "x-slurm")
            })
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                &format!(
                    "services:\n  app:\n    image: redis:7\n{key}: true\n"
                ),
            );
            let err = ComposeSpec::load(&path).expect_err("unsupported root key");
            let needle = format!("unsupported key '{key}'");
            prop_assert!(err.to_string().contains(&needle));
        }

        #[test]
        fn property_rejects_unsupported_service_keys(
            key in key_strategy().prop_filter("unsupported service key", |key| {
                !matches!(
                    key.as_str(),
                    "image"
                        | "command"
                        | "entrypoint"
                        | "environment"
                        | "volumes"
                        | "working_dir"
                        | "depends_on"
                        | "readiness"
                        | "healthcheck"
                        | "x-slurm"
                        | "x-enroot"
                )
            })
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                &format!(
                    "services:\n  app:\n    image: redis:7\n    {key}: true\n"
                ),
            );
            let err = ComposeSpec::load(&path).expect_err("unsupported service key");
            let needle = format!("unsupported key '{key}'");
            prop_assert!(err.to_string().contains(&needle));
        }

        #[test]
        fn property_accepts_minimal_valid_specs_with_allowed_keys_only(
            name in prop::option::of(string_regex("[a-z][a-z0-9_-]{0,8}").expect("name regex")),
            version in prop::option::of(Just("3".to_string())),
            working_dir in prop::option::of(string_regex("/[A-Za-z0-9_/-]{1,12}").expect("dir regex")),
            command in prop::option::of(value_strategy()),
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let mut body = String::new();
            if let Some(name) = name {
                body.push_str(&format!("name: {name}\n"));
            }
            if let Some(version) = version {
                body.push_str(&format!("version: \"{version}\"\n"));
            }
            body.push_str("services:\n  app:\n    image: redis:7\n");
            if let Some(command) = command {
                body.push_str(&format!("    command: \"echo {command}\"\n"));
            }
            if let Some(working_dir) = working_dir {
                body.push_str(&format!("    working_dir: {working_dir}\n"));
            }
            let path = write_spec(tmpdir.path(), &body);
            prop_assert!(ComposeSpec::load(&path).is_ok());
        }

        #[test]
        fn property_nested_defaults_resolve_expected_value(
            a in prop::option::of(value_strategy()),
            b in prop::option::of(value_strategy()),
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                r#"
services:
  app:
    image: redis:7
    environment:
      KEEP: "${A:-${B:-fallback}}"
"#,
            );
            let mut vars = BTreeMap::new();
            if let Some(a) = a.clone() {
                vars.insert("A".to_string(), a);
            }
            if let Some(b) = b.clone() {
                vars.insert("B".to_string(), b);
            }
            let spec = ComposeSpec::load_with_interpolation_vars(&path, &vars).expect("load");
            let expected = a
                .filter(|value| !value.is_empty())
                .or_else(|| b.filter(|value| !value.is_empty()))
                .unwrap_or_else(|| "fallback".to_string());
            prop_assert_eq!(
                spec.services
                    .get("app")
                    .expect("app")
                    .environment
                    .to_pairs()
                    .expect("pairs"),
                vec![("KEEP".into(), expected)]
            );
        }

        #[test]
        fn property_strict_env_scanner_tracks_defaulted_variables(
            a in prop::option::of(value_strategy()),
            b in prop::option::of(value_strategy()),
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                r#"
services:
  app:
    image: redis:7
    environment:
      KEEP: "${A:-${B:-fallback}}"
      ESCAPED: "$${C:-literal}"
"#,
            );
            let mut vars = BTreeMap::new();
            if let Some(a) = a.clone() {
                vars.insert("A".to_string(), a);
            }
            if let Some(b) = b.clone() {
                vars.insert("B".to_string(), b);
            }
            let missing = missing_defaulted_variables(&path, &vars).expect("scan");
            let mut expected = BTreeSet::new();
            let outer_default_used = a.as_ref().is_none_or(|value| value.is_empty());
            if a.is_none() {
                expected.insert("A".to_string());
            }
            if outer_default_used && b.is_none() {
                expected.insert("B".to_string());
            }
            prop_assert_eq!(missing, expected);
        }

        #[test]
        fn property_malformed_interpolation_fails_without_panicking(
            prefix in value_strategy(),
            suffix in value_strategy(),
            malformed in prop_oneof![
                Just("${}".to_string()),
                Just("${A".to_string()),
                Just("${1BAD}".to_string()),
                Just("${A:+oops}".to_string()),
            ],
        ) {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let path = write_spec(
                tmpdir.path(),
                &format!(
                    "services:\n  app:\n    image: redis:7\n    environment:\n      KEEP: \"{prefix}{malformed}{suffix}\"\n      ESCAPED: \"$${{SAFE:-literal}}\"\n"
                ),
            );

            let strict_scan = std::panic::catch_unwind(|| missing_defaulted_variables(&path, &BTreeMap::new()));
            prop_assert!(strict_scan.is_ok());
            prop_assert!(strict_scan.expect("strict scan result").is_err());

            let load = std::panic::catch_unwind(|| ComposeSpec::load_with_interpolation_vars(&path, &BTreeMap::new()));
            prop_assert!(load.is_ok());
            prop_assert!(load.expect("load result").is_err());
        }
    }
}
