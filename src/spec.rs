//! Compose-like spec parsing, interpolation, and validation.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};

mod validation;

use validation::{
    validate_positive_u32, validate_sbatch_safe_string, validate_sbatch_safe_strings,
};

const ROOT_ALLOWED_KEYS: &[&str] = &["name", "services", "version", "x-slurm"];
const SERVICE_ALLOWED_KEYS: &[&str] = &[
    "image",
    "command",
    "entrypoint",
    "environment",
    "volumes",
    "working_dir",
    "depends_on",
    "readiness",
    "healthcheck",
    "x-slurm",
    "x-enroot",
];

/// Top-level compose file accepted by `hpc-compose`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
pub struct ComposeSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "x-slurm", default)]
    pub slurm: SlurmConfig,
    pub services: BTreeMap<String, ServiceSpec>,
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
    pub image: String,
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
    pub time_limit: Option<String>,
    #[serde(default)]
    pub extra_srun_args: Vec<String>,
    #[serde(default)]
    pub failure_policy: Option<ServiceFailurePolicySpec>,
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
    pub constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chdir: Option<String>,
    pub cache_dir: String,
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
    pub image: String,
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
    pub time_limit: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub extra_srun_args: Vec<String>,
    pub failure_policy: EffectiveFailurePolicyConfig,
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

/// Stable effective `x-enroot.prepare` config with defaults applied.
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

/// Per-service `x-enroot` configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrootConfig {
    #[serde(default)]
    pub prepare: Option<PrepareSpec>,
}

/// `x-enroot.prepare` customization for rebuilding an image on the login node.
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
}

/// A normalized service dependency edge.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDependency {
    pub name: String,
    pub condition: DependencyCondition,
}

/// Accepted environment syntaxes for service or prepare environments.
#[derive(Debug, Clone, Default, Deserialize)]
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
        let raw = fs::read_to_string(path)
            .context(format!("failed to read spec at {}", path.display()))?;
        let value: Value = serde_yaml::from_str(&raw)
            .context(format!("failed to parse YAML at {}", path.display()))?;
        validate_root(&value)?;
        let mut spec: ComposeSpec = serde_yaml::from_value(value)
            .context(format!("failed to deserialize spec at {}", path.display()))?;
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
                        ntasks: service.slurm.ntasks,
                        ntasks_per_node: service.slurm.ntasks_per_node,
                        cpus_per_task: service.slurm.cpus_per_task,
                        gpus: service.slurm.gpus,
                        gres: service.slurm.gres.clone(),
                        time_limit: service.slurm.time_limit.clone(),
                        extra_srun_args: service.slurm.extra_srun_args.clone(),
                        failure_policy: EffectiveFailurePolicyConfig::from_policy(
                            &normalized_policy,
                        ),
                    },
                    enroot,
                },
            );
        }

        Ok(EffectiveComposeConfig {
            name: self.name.clone(),
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
                constraint: self.slurm.constraint.clone(),
                output: self.slurm.output.clone(),
                error: self.slurm.error.clone(),
                chdir: self.slurm.chdir.clone(),
                cache_dir: cache_dir.display().to_string(),
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
                        Some(other) => {
                            bail!(
                                "depends_on condition for service '{name}' must be 'service_started' or 'service_healthy', got '{other}'"
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
        validate_sbatch_safe_string(self.job_name.as_deref(), "x-slurm.job-name")?;
        validate_sbatch_safe_string(self.partition.as_deref(), "x-slurm.partition")?;
        validate_sbatch_safe_string(self.account.as_deref(), "x-slurm.account")?;
        validate_sbatch_safe_string(self.qos.as_deref(), "x-slurm.qos")?;
        validate_sbatch_safe_string(self.constraint.as_deref(), "x-slurm.constraint")?;
        validate_sbatch_safe_string(self.time.as_deref(), "x-slurm.time")?;
        validate_sbatch_safe_string(self.mem.as_deref(), "x-slurm.mem")?;
        validate_sbatch_safe_string(self.gres.as_deref(), "x-slurm.gres")?;
        validate_sbatch_safe_string(self.output.as_deref(), "x-slurm.output")?;
        validate_sbatch_safe_string(self.error.as_deref(), "x-slurm.error")?;
        validate_sbatch_safe_string(self.chdir.as_deref(), "x-slurm.chdir")?;
        validate_sbatch_safe_strings(
            self.submit_args.iter().map(String::as_str),
            "x-slurm.submit_args",
        )?;
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
        interpolate_optional_string(&mut self.constraint, vars)?;
        interpolate_optional_string(&mut self.output, vars)?;
        interpolate_optional_string(&mut self.error, vars)?;
        interpolate_optional_string(&mut self.chdir, vars)?;
        interpolate_optional_string(&mut self.cache_dir, vars)?;
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
        self.image = interpolate_string(&self.image, vars)?;
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
        if let Some(limit) = &self.time_limit {
            parse_slurm_time_limit(limit).with_context(|| {
                format!("service '{service_name}' x-slurm.time_limit is invalid")
            })?;
        }
        Ok(())
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
        interpolate_optional_string(&mut self.gres, vars)?;
        interpolate_optional_string(&mut self.time_limit, vars)?;
        interpolate_vec_strings(&mut self.extra_srun_args, vars)?;
        Ok(())
    }
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

type InterpolationVars = BTreeMap<String, String>;

fn interpolation_vars(path: &Path) -> Result<InterpolationVars> {
    let mut vars = load_dotenv_vars(path.parent().unwrap_or_else(|| Path::new(".")))?;
    for (key, value) in env::vars() {
        vars.insert(key, value);
    }
    Ok(vars)
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

fn submit_args_contain_mail_settings(args: &[String]) -> bool {
    args.iter().any(|arg| {
        let trimmed = arg.trim();
        trimmed.starts_with("--mail-user")
            || trimmed.starts_with("--mail-type")
            || trimmed.starts_with("mail-user")
            || trimmed.starts_with("mail-type")
    })
}

/// Returns variables that consumed `${VAR:-default}` or `${VAR-default}`
/// defaults because `VAR` was missing from `vars`.
///
/// # Errors
///
/// Returns an error when interpolation syntax is malformed.
pub fn missing_defaulted_variables(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    let raw =
        fs::read_to_string(path).context(format!("failed to read spec at {}", path.display()))?;
    let value: Value = serde_yaml::from_str(&raw)
        .context(format!("failed to parse YAML at {}", path.display()))?;
    let mut missing = BTreeSet::new();
    collect_missing_defaulted_variables_from_value(&value, vars, &mut missing)?;
    Ok(missing)
}

fn collect_missing_defaulted_variables_from_value(
    value: &Value,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    match value {
        Value::String(current) => collect_missing_defaulted_variables_in_string(current, vars, out),
        Value::Sequence(items) => {
            for item in items {
                collect_missing_defaulted_variables_from_value(item, vars, out)?;
            }
            Ok(())
        }
        Value::Mapping(entries) => {
            for value in entries.values() {
                collect_missing_defaulted_variables_from_value(value, vars, out)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_missing_defaulted_variables_in_string(
    input: &str,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != '$' {
            index += 1;
            continue;
        }
        if matches!(chars.get(index + 1), Some('$')) {
            index += 2;
            continue;
        }
        if matches!(chars.get(index + 1), Some('{')) {
            let start = index;
            index += 2;
            let (expr, next_index) = read_braced_expression(&chars, index, input, start)?;
            index = next_index;
            collect_missing_from_braced_expr(&expr, vars, out, input, start)?;
            continue;
        }
        index += 1;
    }
    Ok(())
}

fn collect_missing_from_braced_expr(
    expr: &str,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
    input: &str,
    start: usize,
) -> Result<()> {
    let mut chars = expr.chars();
    let Some(first) = chars.next() else {
        bail!("invalid variable expression in '{}'", &input[start..]);
    };
    if !is_var_start(first) {
        bail!("invalid variable expression in '{}'", &input[start..]);
    }
    let name_len = 1 + chars.take_while(|ch| is_var_char(*ch)).count();
    let name = &expr[..name_len];
    let suffix = &expr[name_len..];

    match suffix {
        "" => {}
        _ if suffix.starts_with(":-") => {
            let default_used = match vars.get(name) {
                Some(value) => value.is_empty(),
                None => true,
            };
            if !vars.contains_key(name) {
                out.insert(name.to_string());
            }
            if default_used {
                collect_missing_defaulted_variables_in_string(&suffix[2..], vars, out)?;
            }
        }
        _ if suffix.starts_with('-') => {
            if !vars.contains_key(name) {
                out.insert(name.to_string());
                collect_missing_defaulted_variables_in_string(&suffix[1..], vars, out)?;
            }
        }
        _ => bail!("invalid variable expression '${{{expr}}}' in '{input}'"),
    }
    Ok(())
}

fn load_dotenv_vars(project_dir: &Path) -> Result<InterpolationVars> {
    let dotenv_path = project_dir.join(".env");
    if !dotenv_path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&dotenv_path)
        .context(format!("failed to read {}", dotenv_path.display()))?;
    let mut vars = BTreeMap::new();
    for (line_no, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = trimmed.split_once('=') else {
            bail!(
                "failed to parse {}: line {} must use KEY=VALUE syntax",
                dotenv_path.display(),
                line_no + 1
            );
        };
        let key = key.trim();
        if key.is_empty() {
            bail!(
                "failed to parse {}: line {} has an empty variable name",
                dotenv_path.display(),
                line_no + 1
            );
        }
        let value = value.trim();
        let value = if quoted(value, '"') || quoted(value, '\'') {
            value[1..value.len() - 1].to_string()
        } else {
            value.to_string()
        };
        vars.insert(key.to_string(), value);
    }
    Ok(vars)
}

fn quoted(value: &str, quote: char) -> bool {
    value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote)
}

fn interpolate_optional_string(value: &mut Option<String>, vars: &InterpolationVars) -> Result<()> {
    if let Some(current) = value {
        *current = interpolate_string(current, vars)?;
    }
    Ok(())
}

fn interpolate_vec_strings(values: &mut [String], vars: &InterpolationVars) -> Result<()> {
    for value in values {
        *value = interpolate_string(value, vars)?;
    }
    Ok(())
}

fn interpolate_string(input: &str, vars: &InterpolationVars) -> Result<String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] != '$' {
            out.push(chars[index]);
            index += 1;
            continue;
        }

        if matches!(chars.get(index + 1), Some('$')) {
            out.push('$');
            index += 2;
            continue;
        }

        if matches!(chars.get(index + 1), Some('{')) {
            let start = index;
            index += 2;
            let (expr, next_index) = read_braced_expression(&chars, index, input, start)?;
            index = next_index;
            out.push_str(&resolve_braced_variable(&expr, vars, input, start)?);
            continue;
        }

        index += 1;
        if !matches!(chars.get(index), Some(ch) if is_var_start(*ch)) {
            out.push('$');
            continue;
        }

        let mut name = String::new();
        while let Some(ch) = chars.get(index) {
            if is_var_char(*ch) {
                name.push(*ch);
                index += 1;
            } else {
                break;
            }
        }

        let Some(value) = vars.get(&name) else {
            bail!("missing variable '{name}' referenced in '{input}'");
        };
        out.push_str(value);
    }

    Ok(out)
}

fn read_braced_expression(
    chars: &[char],
    mut index: usize,
    input: &str,
    start: usize,
) -> Result<(String, usize)> {
    let mut expr = String::new();
    let mut nested_braces = 0usize;

    while let Some(ch) = chars.get(index) {
        if *ch == '$' {
            if matches!(chars.get(index + 1), Some('$')) {
                expr.push('$');
                expr.push('$');
                index += 2;
                continue;
            }
            if matches!(chars.get(index + 1), Some('{')) {
                nested_braces += 1;
                expr.push('$');
                expr.push('{');
                index += 2;
                continue;
            }
        }

        if *ch == '}' {
            if nested_braces == 0 {
                return Ok((expr, index + 1));
            }
            nested_braces -= 1;
        }

        expr.push(*ch);
        index += 1;
    }

    bail!("unterminated variable expression in '{}'", &input[start..]);
}

fn resolve_braced_variable(
    expr: &str,
    vars: &InterpolationVars,
    input: &str,
    start: usize,
) -> Result<String> {
    let mut chars = expr.chars();
    let Some(first) = chars.next() else {
        bail!("invalid variable expression in '{}'", &input[start..]);
    };
    if !is_var_start(first) {
        bail!("invalid variable expression in '{}'", &input[start..]);
    }
    let name_len = 1 + chars.take_while(|ch| is_var_char(*ch)).count();
    let name = &expr[..name_len];
    let suffix = &expr[name_len..];

    match suffix {
        "" => resolve_required_variable(name, vars),
        _ if suffix.starts_with(":-") => {
            let default = &suffix[2..];
            match vars.get(name) {
                Some(value) if !value.is_empty() => Ok(value.clone()),
                _ => interpolate_string(default, vars),
            }
        }
        _ if suffix.starts_with('-') => match vars.get(name) {
            Some(value) => Ok(value.clone()),
            None => interpolate_string(&suffix[1..], vars),
        },
        _ => bail!("invalid variable expression '${{{expr}}}' in '{input}'"),
    }
}

fn resolve_required_variable(name: &str, vars: &InterpolationVars) -> Result<String> {
    vars.get(name)
        .cloned()
        .context(format!("missing variable '{name}'"))
}

fn parse_healthcheck_argv(items: &[String]) -> Result<Vec<String>> {
    if items.is_empty() {
        bail!("healthcheck.test must not be empty");
    }
    match items[0].as_str() {
        "CMD" => {
            if items.len() < 2 {
                bail!("healthcheck.test CMD form must include a command");
            }
            Ok(items[1..].to_vec())
        }
        "CMD-SHELL" => {
            let Some(shell) = items.get(1) else {
                bail!("healthcheck.test CMD-SHELL form must include a shell command");
            };
            Ok(shell.split_whitespace().map(ToString::to_string).collect())
        }
        _ => bail!("healthcheck.test must start with CMD or CMD-SHELL for Compose compatibility"),
    }
}

fn parse_nc_probe(argv: &[String]) -> Result<Option<(String, u16)>> {
    if argv.first().map(String::as_str) != Some("nc") {
        return Ok(None);
    }
    let mut non_flags = Vec::new();
    let mut has_zero_scan = false;
    let mut index = 1;
    while index < argv.len() {
        match argv[index].as_str() {
            "-z" => {
                has_zero_scan = true;
                index += 1;
            }
            flag if flag.starts_with('-') => {
                index += 1;
            }
            value => {
                non_flags.push(value.to_string());
                index += 1;
            }
        }
    }
    if !has_zero_scan {
        bail!("healthcheck nc probes must include '-z'; use explicit readiness otherwise");
    }
    if non_flags.len() != 2 {
        bail!("healthcheck nc probes must use 'nc -z HOST PORT'");
    }
    let port = non_flags[1]
        .parse::<u16>()
        .context("healthcheck nc probe port must be a valid TCP port")?;
    Ok(Some((non_flags[0].clone(), port)))
}

fn parse_http_probe(argv: &[String]) -> Option<String> {
    match argv.first().map(String::as_str) {
        Some("curl") => argv.iter().rev().find(|item| looks_like_url(item)).cloned(),
        Some("wget") if argv.iter().any(|item| item == "--spider") => {
            argv.iter().rev().find(|item| looks_like_url(item)).cloned()
        }
        _ => None,
    }
}

fn looks_like_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn parse_duration_seconds(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("healthcheck duration must not be empty");
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return trimmed
            .parse::<u64>()
            .context("healthcheck duration must be a valid integer number of seconds");
    }

    let mut total = 0_u64;
    let mut number = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            bail!("unsupported healthcheck duration '{trimmed}'; use values like 30s or 2m");
        }
        let value = number
            .parse::<u64>()
            .context("healthcheck duration segment must be numeric")?;
        let factor = match ch {
            'h' => 3600,
            'm' => 60,
            's' => 1,
            _ => {
                bail!("unsupported healthcheck duration unit '{ch}' in '{trimmed}'; use h, m, or s")
            }
        };
        total = total.saturating_add(value.saturating_mul(factor));
        number.clear();
    }
    if !number.is_empty() {
        bail!("unsupported healthcheck duration '{trimmed}'; include a unit suffix");
    }
    Ok(total)
}

fn validate_artifact_bundle_name(name: &str) -> Result<()> {
    if name == "default" {
        bail!("x-slurm.artifacts bundle name 'default' is reserved for top-level artifact paths");
    }
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        bail!("x-slurm.artifacts bundle names must match [A-Za-z0-9_-]+, got '{name}'");
    }
    Ok(())
}

fn is_var_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_var_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn validate_artifact_path(path: &str) -> Result<()> {
    let candidate = Path::new(path);
    if !candidate.is_absolute() {
        bail!(
            "x-slurm.artifacts.paths entries must be absolute paths under /hpc-compose/job, got '{path}'"
        );
    }

    let mut normalized = Vec::new();
    for component in candidate.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop().context(format!(
                    "x-slurm.artifacts.paths entry '{path}' escapes the root path"
                ))?;
            }
            std::path::Component::Normal(part) => {
                normalized.push(part.to_string_lossy().into_owned())
            }
            std::path::Component::Prefix(_) => {
                bail!("x-slurm.artifacts.paths entry '{path}' must use Unix-style absolute paths");
            }
        }
    }

    if normalized.first().map(String::as_str) != Some("hpc-compose")
        || normalized.get(1).map(String::as_str) != Some("job")
    {
        bail!("x-slurm.artifacts.paths entries must stay under /hpc-compose/job, got '{path}'");
    }
    if normalized.get(2).map(String::as_str) == Some("artifacts") {
        bail!("x-slurm.artifacts.paths must not read from /hpc-compose/job/artifacts");
    }
    Ok(())
}

fn validate_resume_path(path: &str) -> Result<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("x-slurm.resume.path must not be empty");
    }

    let candidate = Path::new(trimmed);
    if !candidate.is_absolute() {
        bail!("x-slurm.resume.path must be an absolute host path, got '{path}'");
    }

    let mut normalized = Vec::new();
    for component in candidate.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop().context(format!(
                    "x-slurm.resume.path entry '{path}' escapes the root path"
                ))?;
            }
            std::path::Component::Normal(part) => {
                normalized.push(part.to_string_lossy().into_owned())
            }
            std::path::Component::Prefix(_) => {
                bail!("x-slurm.resume.path '{path}' must use Unix-style absolute paths");
            }
        }
    }

    if normalized.first().map(String::as_str) == Some("hpc-compose") {
        bail!("x-slurm.resume.path must be a host path, not a container-visible /hpc-compose path");
    }
    Ok(())
}

fn validate_root(value: &Value) -> Result<()> {
    let Some(root) = value.as_mapping() else {
        bail!("top-level YAML document must be a mapping");
    };
    validate_mapping_keys("root", root, ROOT_ALLOWED_KEYS)?;
    let Some(services) = root.get(Value::String("services".into())) else {
        bail!("spec must contain a top-level 'services' mapping");
    };
    let Some(service_map) = services.as_mapping() else {
        bail!("'services' must be a mapping");
    };
    for (name, service) in service_map {
        let Some(service_name) = name.as_str() else {
            bail!("service names must be strings");
        };
        let Some(service_mapping) = service.as_mapping() else {
            bail!("service '{service_name}' must be a mapping");
        };
        validate_mapping_keys(
            &format!("service '{service_name}'"),
            service_mapping,
            SERVICE_ALLOWED_KEYS,
        )?;
    }
    Ok(())
}

fn validate_mapping_keys(scope: &str, mapping: &Mapping, allowed: &[&str]) -> Result<()> {
    for key in mapping.keys() {
        let Some(key_name) = key.as_str() else {
            bail!("{scope} contains a non-string key");
        };
        if allowed.contains(&key_name) {
            continue;
        }
        let message = match key_name {
            "build" => {
                "build is not supported in v1; use image: plus x-enroot.prepare to customize an Enroot image before submission"
            }
            "ports" => {
                "ports are not supported; use host-network semantics and explicit readiness checks"
            }
            "networks" | "network_mode" => {
                "custom container networking is not supported under this Slurm/Enroot execution model"
            }
            "restart" => {
                "Compose restart policies are not supported; use services.<name>.x-slurm.failure_policy instead"
            }
            "deploy" => {
                "deploy is not supported; this tool targets one Slurm allocation, not a long-running orchestrator"
            }
            other => bail!("{scope} uses unsupported key '{other}'"),
        };
        bail!("{scope}: {message}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

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
        assert!(err.to_string().contains("x-enroot.prepare"));
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
                condition: Some("service_completed_successfully".into()),
            },
        )]));
        let err = deps.entries().expect_err("should fail");
        assert!(err.to_string().contains("service_healthy"));
    }

    #[test]
    fn depends_on_map_accepts_service_started_and_healthy() {
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
        ]));
        assert_eq!(
            deps.entries().expect("entries"),
            vec![
                ServiceDependency {
                    name: "db".into(),
                    condition: DependencyCondition::ServiceHealthy,
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
        assert_eq!(app.image, "python:3.11-slim");
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
        assert_eq!(spec.services.get("app").expect("app").image, "redis:7");
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
}
