//! Compose-like spec parsing, interpolation, and validation.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, de};

use crate::domain::{MountParts, parse_node_index_ranges, split_mount_parts};
use crate::spec_error::SpecError;
use crate::suggest;

mod interpolate;
mod parse;
mod validation;

pub use interpolate::{missing_defaulted_variables, referenced_variables};

use interpolate::{
    InterpolationVars, interpolate_optional_string, interpolate_string, interpolate_vec_strings,
    interpolation_vars,
};
use parse::load_raw_spec;
use validation::{
    parse_duration_seconds, parse_healthcheck_argv, parse_http_probe, parse_nc_probe,
    validate_artifact_bundle_name, validate_artifact_path, validate_positive_u32,
    validate_resume_path, validate_sbatch_safe_string, validate_sbatch_safe_strings,
    validate_service_assert_artifact_pattern, validate_shell_hook_script,
    validate_slurm_array_spec, validate_slurm_job_id,
};

/// Top-level compose file accepted by `hpc-compose`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize)]
pub struct ComposeSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(rename = "x-env", default)]
    pub software_env: SoftwareEnvConfig,
    #[serde(rename = "x-slurm", default)]
    pub slurm: SlurmConfig,
    #[serde(default)]
    pub sweep: Option<SweepConfig>,
    /// Named secrets resolved from local files or environment variables.
    /// Each resolved value feeds the interpolation map tagged as a secret, so
    /// `${secret_name}` works in `environment:` and is redacted in
    /// `config`/`context`/inspect output.
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretSpec>,
    pub services: BTreeMap<String, ServiceSpec>,
}

/// One declared secret under the top-level `secrets:` block.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretSpec {
    /// Read the secret value from this file (contents are trimmed).
    #[serde(default)]
    pub file: Option<String>,
    /// Read the secret value from this named environment variable.
    #[serde(default)]
    pub env: Option<String>,
}

impl SecretSpec {
    /// Exactly one of `file` or `env` must be set.
    ///
    /// # Errors
    ///
    /// Returns an error when both or neither source is configured.
    pub fn validate(&self, name: &str) -> Result<()> {
        match (&self.file, &self.env) {
            (Some(_), None) => Ok(()),
            (None, Some(_)) => Ok(()),
            (Some(_), Some(_)) => {
                bail!("secret '{name}' must set exactly one of 'file' or 'env', not both")
            }
            (None, None) => bail!("secret '{name}' must set either 'file' or 'env'"),
        }
    }
}

/// Embedded hyperparameter sweep metadata.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SweepConfig {
    pub parameters: BTreeMap<String, Vec<SweepParameterValue>>,
    pub matrix: SweepMatrix,
    /// Optional objective used by `sweep observe` to rank trials and by
    /// `sweep stop --stop-when` to trigger early termination.
    #[serde(default)]
    pub objective: Option<SweepObjective>,
}

/// Optimization direction for a sweep objective.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveDirection {
    /// Lower objective values are better.
    Minimize,
    /// Higher objective values are better.
    Maximize,
}

/// How a trial's objective value is parsed from its tracked outputs.
///
/// Exactly one parse source is set: a regex against the trial's service log,
/// or a JSON field read from an artifact-collected file.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SweepObjective {
    /// Whether lower or higher objective values are better.
    pub direction: ObjectiveDirection,
    /// Regex matched against the trial's primary service log; the capture
    /// group `group` (default 1) is parsed as `f64`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_pattern: Option<String>,
    /// Capture group index for `log_pattern` (1-based); defaults to 1.
    #[serde(
        default = "default_sweep_objective_group",
        skip_serializing_if = "is_one_u32"
    )]
    pub group: u32,
    /// Artifact-collected JSON file (relative to the trial job's artifact
    /// tree) whose `json_field` holds the objective.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_path: Option<String>,
    /// Field name to read from the JSON file when `json_path` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_field: Option<String>,
}

impl SweepObjective {
    /// Validates that exactly one parse source is configured.
    ///
    /// # Errors
    ///
    /// Returns an error when neither or both parse sources are set, or when
    /// `json_path` is set without `json_field`.
    pub fn validate(&self) -> Result<()> {
        let has_log = self.log_pattern.is_some();
        let has_json = self.json_path.is_some();
        if has_log && has_json {
            bail!("sweep.objective must set either log_pattern or json_path, not both");
        }
        if !has_log && !has_json {
            bail!("sweep.objective must set log_pattern or json_path");
        }
        if has_json && self.json_field.is_none() {
            bail!("sweep.objective.json_path requires json_field");
        }
        if self.group == 0 {
            bail!("sweep.objective.group must be at least 1");
        }
        if let Some(pattern) = &self.log_pattern {
            regex::Regex::new(pattern).with_context(|| {
                format!("sweep.objective.log_pattern '{pattern}' is not a valid regex")
            })?;
        }
        Ok(())
    }
}

fn is_one_u32(value: &u32) -> bool {
    *value == 1
}

fn default_sweep_objective_group() -> u32 {
    1
}

impl SweepConfig {
    /// Returns the number of parameter combinations in this sweep.
    ///
    /// # Errors
    ///
    /// Returns an error if the Cartesian product would overflow `usize`.
    pub fn total_trials(&self) -> Result<usize> {
        self.parameters.values().try_fold(1_usize, |total, values| {
            total
                .checked_mul(values.len())
                .context("sweep parameter matrix is too large")
        })
    }

    fn validate(&self) -> Result<()> {
        if self.parameters.is_empty() {
            bail!("sweep.parameters must contain at least one parameter");
        }
        for (name, values) in &self.parameters {
            validate_sweep_parameter_name(name)?;
            if values.is_empty() {
                bail!("sweep.parameters.{name} must contain at least one value");
            }
        }
        if let SweepMatrix::Random { random, .. } = &self.matrix {
            if *random == 0 {
                bail!("sweep.matrix.random must be at least 1");
            }
            let total = self.total_trials()?;
            if *random > total {
                bail!(
                    "sweep.matrix.random requests {random} trials but only {total} combinations exist"
                );
            }
        }
        if let Some(objective) = &self.objective {
            objective.validate()?;
        }
        Ok(())
    }
}

fn validate_sweep_parameter_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("sweep parameter names must not be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        bail!("sweep parameter '{name}' is not a valid interpolation variable name");
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        bail!("sweep parameter '{name}' is not a valid interpolation variable name");
    }
    if name.starts_with("HPC_COMPOSE_SWEEP_") {
        bail!("sweep parameter '{name}' uses the reserved HPC_COMPOSE_SWEEP_ prefix");
    }
    Ok(())
}

/// One scalar sweep parameter value, stored as the string passed to interpolation.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct SweepParameterValue(String);

impl SweepParameterValue {
    /// Returns the interpolation value for this sweep scalar.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SweepParameterValue {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl<'de> Deserialize<'de> for SweepParameterValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarVisitor;

        impl Visitor<'_> for ScalarVisitor {
            type Value = SweepParameterValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, number, or boolean sweep value")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value))
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value.to_string()))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value.to_string()))
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value.to_string()))
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(SweepParameterValue(value.to_string()))
            }
        }

        deserializer.deserialize_any(ScalarVisitor)
    }
}

/// Sweep matrix expansion strategy.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SweepMatrix {
    Full,
    Random {
        random: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        seed: Option<String>,
    },
}

impl<'de> Deserialize<'de> for SweepMatrix {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RandomMatrix {
            random: usize,
            #[serde(default)]
            seed: Option<String>,
        }

        struct MatrixVisitor;

        impl<'de> Visitor<'de> for MatrixVisitor {
            type Value = SweepMatrix;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("\"full\" or a mapping with random and optional seed")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "full" {
                    Ok(SweepMatrix::Full)
                } else {
                    Err(E::custom(
                        "sweep.matrix must be \"full\" or { random, seed }",
                    ))
                }
            }

            fn visit_map<A>(self, map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let random = RandomMatrix::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SweepMatrix::Random {
                    random: random.random,
                    seed: random.seed,
                })
            }
        }

        deserializer.deserialize_any(MatrixVisitor)
    }
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
    #[serde(skip)]
    pub software_env: SoftwareEnvConfig,
    #[serde(default)]
    pub resources: Option<String>,
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
    pub array: Option<String>,
    #[serde(default)]
    pub after_job: Option<JobDependencySpec>,
    #[serde(default)]
    pub dependency: Option<JobDependencyMode>,
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
    #[serde(default)]
    pub rendezvous: Option<RendezvousClientConfig>,
}

/// Top-level client-side cross-job rendezvous discovery config.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RendezvousClientConfig {
    pub discover: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub require: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawRendezvousClientConfig {
    Name(String),
    Names(Vec<String>),
    Mapping(RawRendezvousClientMapping),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRendezvousClientMapping {
    #[serde(default)]
    discover: Option<RawRendezvousDiscover>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    require: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawRendezvousDiscover {
    Name(String),
    Names(Vec<String>),
}

impl<'de> Deserialize<'de> for RendezvousClientConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let raw = RawRendezvousClientConfig::deserialize(deserializer)?;
        let config = match raw {
            RawRendezvousClientConfig::Name(name) => Self {
                discover: vec![name],
                timeout_seconds: None,
                require: None,
            },
            RawRendezvousClientConfig::Names(discover) => Self {
                discover,
                timeout_seconds: None,
                require: None,
            },
            RawRendezvousClientConfig::Mapping(mapping) => {
                let discover = match mapping.discover {
                    Some(RawRendezvousDiscover::Name(name)) => vec![name],
                    Some(RawRendezvousDiscover::Names(names)) => names,
                    None => Vec::new(),
                };
                Self {
                    discover,
                    timeout_seconds: mapping.timeout_seconds,
                    require: mapping.require,
                }
            }
        };
        Ok(config)
    }
}

/// Per-service provider-side rendezvous config.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceRendezvousConfig {
    #[serde(default)]
    pub register: Option<RendezvousRegisterConfig>,
}

/// Service registration written under `<cache_dir>/rendezvous/<name>/`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RendezvousRegisterConfig {
    pub name: String,
    pub port: u16,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Accepted `x-slurm.after_job` syntaxes.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum JobDependencySpec {
    /// Shorthand job id form, equivalent to `afterany:<id>`.
    Id(String),
    /// Mapping form with an explicit condition.
    Mapping(JobDependency),
}

impl JobDependencySpec {
    /// Returns the normalized dependency id.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Id(id) => id,
            Self::Mapping(dependency) => dependency.id.as_str(),
        }
    }

    /// Returns the normalized dependency condition.
    #[must_use]
    pub fn condition(&self) -> JobDependencyCondition {
        match self {
            Self::Id(_) => JobDependencyCondition::AfterAny,
            Self::Mapping(dependency) => dependency.condition.unwrap_or_default(),
        }
    }

    fn validate(&self, field: &str) -> Result<()> {
        validate_slurm_job_id(self.id(), field)
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        match self {
            Self::Id(id) => {
                *id = interpolate_string(id, vars)?;
                Ok(())
            }
            Self::Mapping(dependency) => {
                dependency.id = interpolate_string(&dependency.id, vars)?;
                Ok(())
            }
        }
    }
}

/// Slurm job dependency mapping form.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobDependency {
    pub id: String,
    #[serde(default)]
    pub condition: Option<JobDependencyCondition>,
}

/// Slurm job dependency condition for id-based dependencies.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum JobDependencyCondition {
    /// Start after the target job terminates in any state.
    #[default]
    #[serde(rename = "afterany")]
    AfterAny,
    /// Start after the target job succeeds.
    #[serde(rename = "afterok")]
    AfterOk,
    /// Start after the target job fails.
    #[serde(rename = "afternotok")]
    AfterNotOk,
}

impl JobDependencyCondition {
    /// Returns the Slurm CLI token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AfterAny => "afterany",
            Self::AfterOk => "afterok",
            Self::AfterNotOk => "afternotok",
        }
    }
}

/// Slurm dependency mode that does not require an explicit job id.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobDependencyMode {
    /// Slurm singleton dependency.
    Singleton,
}

impl JobDependencyMode {
    /// Returns the Slurm CLI token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Singleton => "singleton",
        }
    }
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

/// Structured host-side software environment setup for modules, Spack views,
/// and exported environment variables.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SoftwareEnvConfig {
    #[serde(default)]
    pub modules: ModuleEnvSpec,
    #[serde(default)]
    pub spack: Option<SpackEnvSpec>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// Accepted `x-env.modules` syntaxes.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ModuleEnvSpec {
    /// Whether to run `module purge` before loading modules.
    pub purge: bool,
    /// Module names to load in order.
    pub load: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawModuleEnvSpec {
    List(Vec<String>),
    Object(RawModuleEnvObject),
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawModuleEnvObject {
    #[serde(default)]
    purge: bool,
    #[serde(default)]
    load: Vec<String>,
}

impl<'de> Deserialize<'de> for ModuleEnvSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match RawModuleEnvSpec::deserialize(deserializer)? {
            RawModuleEnvSpec::List(load) => Ok(Self { purge: false, load }),
            RawModuleEnvSpec::Object(raw) => Ok(Self {
                purge: raw.purge,
                load: raw.load,
            }),
        }
    }
}

/// Spack environment view configuration.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SpackEnvSpec {
    pub view: String,
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
    pub script: Option<String>,
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
    #[serde(rename = "assert", default)]
    pub assertions: Option<ServiceAssertSpec>,
    #[serde(rename = "x-env", default)]
    pub software_env: SoftwareEnvConfig,
    #[serde(rename = "x-slurm", default)]
    pub slurm: ServiceSlurmConfig,
    #[serde(rename = "x-runtime", default)]
    pub runtime: ServiceRuntimeConfig,
    #[serde(rename = "x-enroot", default)]
    pub enroot: ServiceEnrootConfig,
}

/// Per-service post-run assertion contract.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceAssertSpec {
    #[serde(default)]
    pub exit_code: Option<u16>,
    #[serde(default)]
    pub artifacts_contain: Option<String>,
    #[serde(default)]
    pub max_duration_seconds: Option<u64>,
}

impl ServiceAssertSpec {
    /// Returns true when at least one assertion is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.exit_code.is_none()
            && self.artifacts_contain.is_none()
            && self.max_duration_seconds.is_none()
    }

    /// Returns the container-rooted artifact glob used by the runtime script.
    ///
    /// Relative patterns resolve under `/hpc-compose/job`.
    #[must_use]
    pub fn normalized_artifacts_contain(&self) -> Option<String> {
        self.artifacts_contain
            .as_deref()
            .map(normalize_service_assert_artifact_pattern)
    }
}

/// Per-service `x-slurm` overrides.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSlurmConfig {
    #[serde(skip)]
    pub software_env: SoftwareEnvConfig,
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
    pub hooks: Vec<ServiceEventHookSpec>,
    #[serde(default)]
    pub scratch: Option<ServiceScratchConfig>,
    #[serde(default)]
    pub rendezvous: Option<ServiceRendezvousConfig>,
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

/// Event emitted by the service failure-policy supervisor.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHookEvent {
    /// A failed service is about to be relaunched by `restart_on_failure`.
    Restart,
    /// A failed service exceeded the rolling restart-window guard.
    WindowExhausted,
}

/// Per-service failure-policy event hook.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceEventHookSpec {
    pub on: ServiceHookEvent,
    #[serde(default)]
    pub context: ServiceHookContext,
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
    pub profile: Option<MpiProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation: Option<MpiImplementation>,
    #[serde(default, skip_serializing_if = "MpiLauncher::is_default")]
    pub launcher: MpiLauncher,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_ranks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_mpi: Option<HostMpiConfig>,
}

/// MPI compatibility profile used for validation and diagnostics.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MpiProfile {
    /// Open MPI with Slurm PMI/PMIx launch.
    Openmpi,
    /// MPICH with Slurm PMI/PMIx launch.
    Mpich,
    /// Intel MPI with Slurm PMI-2 launch.
    IntelMpi,
}

impl MpiProfile {
    /// Returns the config spelling for this profile.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Openmpi => "openmpi",
            Self::Mpich => "mpich",
            Self::IntelMpi => "intel_mpi",
        }
    }

    /// Returns the matching MPI implementation family for this profile.
    #[must_use]
    pub fn implementation(self) -> MpiImplementation {
        match self {
            Self::Openmpi => MpiImplementation::Openmpi,
            Self::Mpich => MpiImplementation::Mpich,
            Self::IntelMpi => MpiImplementation::IntelMpi,
        }
    }
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
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
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
    #[default]
    Unknown,
}

impl MpiImplementation {
    /// Returns the config spelling for this MPI implementation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Openmpi => "openmpi",
            Self::Mpich => "mpich",
            Self::IntelMpi => "intel_mpi",
            Self::Mvapich2 => "mvapich2",
            Self::CrayMpi => "cray_mpi",
            Self::HpeMpi => "hpe_mpi",
            Self::Unknown => "unknown",
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sweep: Option<SweepConfig>,
    #[serde(rename = "x-env", skip_serializing_if = "SoftwareEnvConfig::is_empty")]
    pub software_env: SoftwareEnvConfig,
    #[serde(rename = "x-slurm")]
    pub slurm: EffectiveSlurmConfig,
    pub services: BTreeMap<String, EffectiveServiceConfig>,
}

/// Stable top-level `x-slurm` config with defaults materialized.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveSlurmConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_job: Option<EffectiveJobDependency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependency: Option<JobDependencyMode>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rendezvous: Option<RendezvousClientConfig>,
}

/// Stable effective representation of an id-based Slurm dependency.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectiveJobDependency {
    pub id: String,
    pub condition: JobDependencyCondition,
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
    #[serde(rename = "assert", skip_serializing_if = "Option::is_none")]
    pub assertions: Option<ServiceAssertSpec>,
    #[serde(rename = "x-env", skip_serializing_if = "SoftwareEnvConfig::is_empty")]
    pub software_env: SoftwareEnvConfig,
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
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub hooks: Vec<ServiceEventHookSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scratch: Option<ServiceScratchConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rendezvous: Option<ServiceRendezvousConfig>,
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
    /// `true` when the author omitted the condition (list-form `depends_on` or
    /// mapping form without an explicit `condition:` key). Lint uses this to
    /// recommend making the implicit `service_started` default explicit.
    #[serde(default)]
    pub implicit: bool,
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
    /// Loads only the embedded sweep metadata without applying interpolation to
    /// the runnable spec. This lets `sweep submit` provide trial variables that
    /// a normal `plan` invocation may not have in its environment.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, the YAML cannot be
    /// parsed, or the sweep block is invalid.
    pub fn load_sweep(path: &Path) -> Result<Option<SweepConfig>> {
        let spec = load_raw_spec(path)?;
        if let Some(sweep) = &spec.sweep {
            sweep.validate()?;
        }
        Ok(spec.sweep)
    }

    /// Loads only the top-level `secrets:` block from a compose file, without
    /// applying interpolation (secrets feed the interpolation map themselves).
    /// Each declared secret is validated to set exactly one source.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be parsed or a secret declares
    /// both or neither of `file`/`env`.
    pub fn load_secrets(path: &Path) -> Result<BTreeMap<String, SecretSpec>> {
        let spec = load_raw_spec(path)?;
        for (name, secret) in &spec.secrets {
            secret.validate(name)?;
        }
        Ok(spec.secrets)
    }

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
        self.software_env.interpolate(vars)?;
        self.slurm.interpolate(vars)?;
        for service in self.services.values_mut() {
            service.interpolate(vars)?;
        }
        Ok(())
    }

    fn validate(&mut self) -> Result<()> {
        self.software_env.validate("x-env")?;
        if let Some(sweep) = &self.sweep {
            sweep.validate()?;
        }
        for (name, secret) in &self.secrets {
            secret.validate(name)?;
        }
        self.slurm.software_env = self.software_env.clone();
        self.slurm.validate()?;
        for (name, service) in &mut self.services {
            service.normalize_script_and_command(name)?;
            service.normalize_healthcheck(name)?;
            if service.runtime.prepare.is_some() && service.enroot.prepare.is_some() {
                return Err(SpecError::DuplicatePrepareHook {
                    service: name.clone(),
                }
                .into());
            }
            if self.runtime.backend != RuntimeBackend::Pyxis && service.enroot.prepare.is_some() {
                return Err(SpecError::EnrootPrepareRequiresPyxis {
                    service: name.clone(),
                    backend: self.runtime.backend.as_str().to_string(),
                }
                .into());
            }
            service
                .environment
                .validate_names(&format!("service '{name}' environment"))?;
            service
                .software_env
                .validate(&format!("service '{name}' x-env"))?;
            if let Some(assertions) = &service.assertions {
                assertions.validate(name)?;
            }
            service.slurm.software_env = service.software_env.clone();
            service.slurm.validate(name)?;
            service.runtime.validate(name)?;
            service.enroot.validate(name)?;
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
                    assertions: service.assertions.clone(),
                    software_env: service.software_env.clone(),
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
                        hooks: service.slurm.hooks.clone(),
                        scratch: service.slurm.scratch.clone(),
                        rendezvous: service.slurm.rendezvous.clone(),
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
            sweep: self.sweep.clone(),
            software_env: self.software_env.clone(),
            slurm: EffectiveSlurmConfig {
                resources: self.slurm.resources.clone(),
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
                array: self.slurm.array.clone(),
                after_job: self
                    .slurm
                    .after_job
                    .as_ref()
                    .map(|dependency| EffectiveJobDependency {
                        id: dependency.id().to_string(),
                        condition: dependency.condition(),
                    }),
                dependency: self.slurm.dependency,
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
                rendezvous: self.slurm.rendezvous.clone(),
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
                        implicit: true,
                    });
                }
                Ok(out)
            }
            DependsOnSpec::Map(items) => {
                let mut out = Vec::with_capacity(items.len());
                for (name, cfg) in items {
                    let (condition, implicit) = match cfg.condition.as_deref() {
                        None => (DependencyCondition::ServiceStarted, true),
                        Some("service_started") => (DependencyCondition::ServiceStarted, false),
                        Some("service_healthy") => (DependencyCondition::ServiceHealthy, false),
                        Some("service_completed_successfully") => {
                            (DependencyCondition::ServiceCompletedSuccessfully, false)
                        }
                        Some(other) => {
                            let candidates = [
                                "service_started",
                                "service_healthy",
                                "service_completed_successfully",
                            ];
                            let help_text = match suggest::nearest_default(other, &candidates) {
                                Some(s) => format!(
                                    "Use one of the three supported Compose dependency conditions. Did you mean \"{s}\"?"
                                ),
                                None => {
                                    "Use one of the three supported Compose dependency conditions."
                                        .to_string()
                                }
                            };
                            return Err(SpecError::InvalidDependencyCondition {
                                service: name.clone(),
                                got: other.to_string(),
                                help_text,
                            }
                            .into());
                        }
                    };
                    out.push(ServiceDependency {
                        name: name.clone(),
                        condition,
                        implicit,
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
                        return Err(SpecError::InvalidEnvironmentEntry.into());
                    };
                    pairs.push((key.to_string(), value.to_string()));
                }
                Ok(pairs)
            }
        }
    }

    fn validate_names(&self, field: &str) -> Result<()> {
        for (name, _) in self
            .to_pairs()
            .with_context(|| format!("{field} is invalid"))?
        {
            validate_safe_env_name(&name, field)?;
        }
        Ok(())
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
                        return Err(SpecError::InvalidEnvironmentEntry.into());
                    };
                    *item = format!("{key}={}", interpolate_string(value, vars)?);
                }
                Ok(())
            }
        }
    }
}

impl SoftwareEnvConfig {
    /// Returns true when no module, Spack, or environment setup is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.modules.purge
            && self.modules.load.is_empty()
            && self.spack.is_none()
            && self.env.is_empty()
    }

    fn validate(&self, field: &str) -> Result<()> {
        for (index, module) in self.modules.load.iter().enumerate() {
            validate_software_env_string(module, &format!("{field}.modules[{index}]"))?;
            if module.starts_with('-') {
                bail!("{field}.modules[{index}] must not start with '-'");
            }
        }
        if let Some(spack) = &self.spack {
            validate_software_env_string(&spack.view, &format!("{field}.spack.view"))?;
            if !Path::new(&spack.view).is_absolute() {
                bail!("{field}.spack.view must be an absolute path");
            }
        }
        for (name, value) in &self.env {
            validate_safe_env_name(name, &format!("{field}.env"))?;
            validate_software_env_string(value, &format!("{field}.env.{name}"))?;
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_vec_strings(&mut self.modules.load, vars)?;
        if let Some(spack) = &mut self.spack {
            spack.view = interpolate_string(&spack.view, vars)?;
        }
        for value in self.env.values_mut() {
            *value = interpolate_string(value, vars)?;
        }
        Ok(())
    }
}

fn validate_software_env_string(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.contains('\0') || value.contains('\n') || value.contains('\r') {
        bail!("{field} must not contain line breaks or null bytes");
    }
    Ok(())
}

fn validate_safe_env_name(name: &str, field: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        bail!("{field} contains an empty environment variable name");
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        bail!("{field}.{name} is not a safe environment variable name");
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        bail!("{field}.{name} is not a safe environment variable name");
    }
    Ok(())
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

    /// Returns the normalized Slurm dependency CLI value, if configured.
    #[must_use]
    pub fn dependency_cli_value(&self) -> Option<String> {
        let mut parts = Vec::new();
        if let Some(dependency) = &self.after_job {
            parts.push(format!(
                "{}:{}",
                dependency.condition().as_str(),
                dependency.id()
            ));
        }
        if let Some(dependency) = self.dependency {
            parts.push(dependency.as_str().to_string());
        }
        (!parts.is_empty()).then(|| parts.join(","))
    }

    /// Returns whether this config uses a scheduler-level dependency.
    #[must_use]
    pub fn has_scheduler_dependency(&self) -> bool {
        self.after_job.is_some() || self.dependency.is_some()
    }

    /// Validates semantic rules that serde alone cannot express.
    ///
    /// # Errors
    ///
    /// Returns an error when allocation, metrics, artifact, or resume settings
    /// violate `hpc-compose`'s supported Slurm model.
    pub fn validate(&self) -> Result<()> {
        validate_sbatch_safe_string(self.resources.as_deref(), "x-slurm.resources")?;
        if self
            .resources
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(SpecError::EmptyField {
                field: "x-slurm.resources".into(),
            }
            .into());
        }
        validate_positive_u32(self.nodes, "x-slurm.nodes")?;
        validate_positive_u32(self.ntasks, "x-slurm.ntasks")?;
        validate_positive_u32(self.ntasks_per_node, "x-slurm.ntasks_per_node")?;
        validate_positive_u32(self.cpus_per_task, "x-slurm.cpus_per_task")?;
        validate_positive_u32(self.gpus, "x-slurm.gpus")?;
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
        validate_slurm_array_spec(self.array.as_deref(), "x-slurm.array")?;
        if let Some(after_job) = &self.after_job {
            after_job.validate("x-slurm.after_job")?;
        }
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
            return Err(SpecError::MetricsIntervalTooLow.into());
        }
        if let Some(artifacts) = &self.artifacts {
            let Some(export_dir) = artifacts.export_dir.as_deref() else {
                return Err(SpecError::ArtifactsMissingExportDir.into());
            };
            if export_dir.trim().is_empty() {
                return Err(SpecError::EmptyField {
                    field: "x-slurm.artifacts.export_dir".into(),
                }
                .into());
            }
            if artifacts.paths.is_empty() && artifacts.bundles.is_empty() {
                return Err(SpecError::ArtifactsNoSources.into());
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
        if let Some(rendezvous) = &self.rendezvous {
            rendezvous.validate()?;
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_optional_string(&mut self.resources, vars)?;
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
        interpolate_optional_string(&mut self.array, vars)?;
        if let Some(after_job) = &mut self.after_job {
            after_job.interpolate(vars)?;
        }
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
        if let Some(rendezvous) = &mut self.rendezvous {
            rendezvous.interpolate(vars)?;
        }
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
    match split_mount_parts(value) {
        MountParts::HostContainer {
            host, container, ..
        } => {
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
        MountParts::UnsupportedMode(mode) => {
            bail!("{field} uses unsupported mode '{mode}'; use ro or rw")
        }
        MountParts::InvalidShape => {
            bail!("{field} must use host_path:container_path[:ro|rw] syntax")
        }
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
            *export_dir = interpolate_string_preserving_slurm_job_id(export_dir, vars)?;
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
        if let Some(assertions) = &mut self.assertions {
            assertions.interpolate(vars)?;
        }
        self.software_env.interpolate(vars)?;
        self.slurm.interpolate(vars)?;
        self.runtime.interpolate(vars)?;
        self.enroot.interpolate(vars)?;
        Ok(())
    }

    fn normalize_script_and_command(&mut self, name: &str) -> Result<()> {
        if let Some(script) = self.script.take() {
            if self.command.is_some() {
                return Err(SpecError::ScriptCommandConflict {
                    service: name.to_string(),
                    conflict: "command".into(),
                }
                .into());
            }
            if self.entrypoint.is_some() {
                return Err(SpecError::ScriptCommandConflict {
                    service: name.to_string(),
                    conflict: "entrypoint".into(),
                }
                .into());
            }
            validate_service_script(&script, &format!("service '{name}' script"))?;
            self.command = Some(CommandSpec::Vec(vec![
                "/bin/sh".into(),
                "-lc".into(),
                script,
            ]));
        }

        let Some(CommandSpec::String(command)) = self.command.as_ref() else {
            return Ok(());
        };
        if !command.contains('\n') {
            return Ok(());
        }
        let command = match self.entrypoint.take() {
            None => command.clone(),
            Some(CommandSpec::String(entrypoint)) => format!("{entrypoint} {command}"),
            Some(CommandSpec::Vec(_)) => {
                return Err(SpecError::MixedCommandForms {
                    service: name.to_string(),
                    form_a: "array".into(),
                    form_b: "string".into(),
                }
                .into());
            }
        };
        self.command = Some(CommandSpec::Vec(vec![
            "/bin/sh".into(),
            "-lc".into(),
            command,
        ]));
        Ok(())
    }

    fn normalize_healthcheck(&mut self, name: &str) -> Result<()> {
        if self.readiness.is_some() && self.healthcheck.is_some() {
            return Err(SpecError::ReadinessHealthcheckConflict {
                service: name.to_string(),
            }
            .into());
        }

        let Some(healthcheck) = self.healthcheck.take() else {
            return Ok(());
        };
        if healthcheck.disable.unwrap_or(false) {
            self.readiness = None;
            return Ok(());
        }
        if healthcheck.interval.is_some() {
            return Err(SpecError::HealthcheckUnsupportedField {
                service: name.to_string(),
                field: "interval".into(),
            }
            .into());
        }
        if healthcheck.retries.is_some() {
            return Err(SpecError::HealthcheckUnsupportedField {
                service: name.to_string(),
                field: "retries".into(),
            }
            .into());
        }
        if healthcheck.start_period.is_some() {
            return Err(SpecError::HealthcheckUnsupportedField {
                service: name.to_string(),
                field: "start_period".into(),
            }
            .into());
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

impl ServiceAssertSpec {
    fn validate(&self, service_name: &str) -> Result<()> {
        if self.is_empty() {
            bail!("service '{service_name}' assert must configure at least one assertion");
        }
        if let Some(exit_code) = self.exit_code
            && exit_code > 255
        {
            bail!("service '{service_name}' assert.exit_code must be between 0 and 255");
        }
        if let Some(pattern) = self.artifacts_contain.as_deref() {
            validate_service_assert_artifact_pattern(
                pattern,
                &format!("service '{service_name}' assert.artifacts_contain"),
            )?;
        }
        if matches!(self.max_duration_seconds, Some(0)) {
            bail!("service '{service_name}' assert.max_duration_seconds must be at least 1");
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_optional_string(&mut self.artifacts_contain, vars)?;
        Ok(())
    }
}

fn validate_service_script(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    Ok(())
}

fn validate_rendezvous_name(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("{field} must contain only ASCII letters, digits, '.', '_', or '-'");
    }
    Ok(())
}

fn validate_rendezvous_protocol(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        bail!("{field} must contain only ASCII letters, digits, '+', '-', or '.'");
    }
    Ok(())
}

fn validate_rendezvous_path(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    if !value.is_empty() && !value.starts_with('/') {
        bail!("{field} must be empty or start with '/'");
    }
    Ok(())
}

impl RendezvousClientConfig {
    fn validate(&self) -> Result<()> {
        if self.discover.is_empty() {
            bail!("x-slurm.rendezvous.discover must contain at least one name");
        }
        for (index, name) in self.discover.iter().enumerate() {
            validate_rendezvous_name(name, &format!("x-slurm.rendezvous.discover[{index}]"))?;
        }
        if matches!(self.timeout_seconds, Some(0)) {
            bail!("x-slurm.rendezvous.timeout_seconds must be at least 1");
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        interpolate_vec_strings(&mut self.discover, vars)
    }
}

impl ServiceRendezvousConfig {
    fn validate(&self, service_name: &str) -> Result<()> {
        let Some(register) = &self.register else {
            return Ok(());
        };
        register.validate(service_name)
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(register) = &mut self.register {
            register.interpolate(vars)?;
        }
        Ok(())
    }
}

impl RendezvousRegisterConfig {
    fn validate(&self, service_name: &str) -> Result<()> {
        validate_rendezvous_name(
            &self.name,
            &format!("service '{service_name}' x-slurm.rendezvous.register.name"),
        )?;
        if self.port == 0 {
            bail!("service '{service_name}' x-slurm.rendezvous.register.port must be at least 1");
        }
        validate_rendezvous_protocol(
            self.protocol.as_deref(),
            &format!("service '{service_name}' x-slurm.rendezvous.register.protocol"),
        )?;
        validate_rendezvous_path(
            self.path.as_deref(),
            &format!("service '{service_name}' x-slurm.rendezvous.register.path"),
        )?;
        if matches!(self.ttl_seconds, Some(0)) {
            bail!(
                "service '{service_name}' x-slurm.rendezvous.register.ttl_seconds must be at least 1"
            );
        }
        for (key, value) in &self.metadata {
            validate_rendezvous_name(
                key,
                &format!("service '{service_name}' x-slurm.rendezvous.register.metadata key"),
            )?;
            if value.contains('\0') {
                bail!(
                    "service '{service_name}' x-slurm.rendezvous.register.metadata.{key} must not contain null bytes"
                );
            }
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        self.name = interpolate_string(&self.name, vars)?;
        interpolate_optional_string(&mut self.protocol, vars)?;
        interpolate_optional_string(&mut self.path, vars)?;
        for value in self.metadata.values_mut() {
            *value = interpolate_string(value, vars)?;
        }
        Ok(())
    }
}

fn normalize_service_assert_artifact_pattern(value: &str) -> String {
    if value.starts_with('/') {
        value.to_string()
    } else {
        crate::tracked_paths::under_job_container_dir(value)
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
            self.cpus_per_task,
            &format!("service '{service_name}' x-slurm.cpus_per_task"),
        )?;
        validate_positive_u32(self.gpus, &format!("service '{service_name}' x-slurm.gpus"))?;
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
        for (index, hook) in self.hooks.iter().enumerate() {
            hook.validate(&format!("service '{service_name}' x-slurm.hooks[{index}]"))?;
        }
        if let Some(rendezvous) = &self.rendezvous {
            rendezvous.validate(service_name)?;
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
        if let Some(rendezvous) = &mut self.rendezvous {
            rendezvous.interpolate(vars)?;
        }
        Ok(())
    }
}

impl MpiConfig {
    /// Returns the explicit implementation or the implementation implied by a profile.
    #[must_use]
    pub fn resolved_implementation(&self) -> Option<MpiImplementation> {
        self.implementation
            .or_else(|| self.profile.map(MpiProfile::implementation))
    }

    fn validate(&self, service_name: &str) -> Result<()> {
        if let (Some(profile), Some(implementation)) = (self.profile, self.implementation)
            && profile.implementation() != implementation
        {
            bail!(
                "service '{service_name}' x-slurm.mpi.profile={} conflicts with x-slurm.mpi.implementation={}",
                profile.as_str(),
                implementation.as_str()
            );
        }
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
        self.env.validate_names(&format!(
            "service '{service_name}' x-slurm.mpi.host_mpi.env"
        ))?;
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

impl ServiceEventHookSpec {
    fn validate(&self, field: &str) -> Result<()> {
        validate_shell_hook_script(&self.script, field)?;
        if self.context != ServiceHookContext::Host {
            bail!(
                "{field}.context must be host; event-driven hooks do not support container context"
            );
        }
        Ok(())
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
    parse_node_index_ranges(value, label)?;
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
    fn validate(&self, service_name: &str) -> Result<()> {
        if let Some(prepare) = &self.prepare {
            prepare.validate(&format!("service '{service_name}' x-enroot.prepare"))?;
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(prepare) = &mut self.prepare {
            prepare.interpolate(vars)?;
        }
        Ok(())
    }
}

impl ServiceRuntimeConfig {
    fn validate(&self, service_name: &str) -> Result<()> {
        if let Some(prepare) = &self.prepare {
            prepare.validate(&format!("service '{service_name}' x-runtime.prepare"))?;
        }
        Ok(())
    }

    fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(prepare) = &mut self.prepare {
            prepare.interpolate(vars)?;
        }
        Ok(())
    }
}

impl PrepareSpec {
    fn validate(&self, field: &str) -> Result<()> {
        self.env.validate_names(&format!("{field}.env"))
    }

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

/// Parses a short duration string (`30s`, `2m`, `1h`, or a bare number of
/// seconds) into seconds. Used by CLI flags like `--ready-timeout`.
///
/// # Errors
///
/// Returns an error when the input is empty or uses an unsupported unit.
pub fn parse_short_duration(raw: &str) -> Result<u64> {
    validation::parse_duration_seconds(raw)
}

/// One gibibyte in bytes, shared by every memory-size parser and formatter.
pub(crate) const GIB: u64 = 1_024 * 1_024 * 1_024;

/// Parses a memory-size string (`512M`, `1.5G`, `2GiB`, `1048576`, …) into a
/// byte count.
///
/// This is the single shared implementation used by the linter and the
/// `job` accounting/rightsize/scoring code so they all agree on units and edge
/// cases. It accepts an optional decimal magnitude, the `B`/`K`/`M`/`G`/`T`/`P`
/// suffixes (with `B`/`iB` variants), and a bare byte count. The Slurm `sacct`
/// literal `unknown` (any case) and the empty string map to `None`. All
/// arithmetic saturates, so the function is total and never panics or overflows.
#[must_use]
pub(crate) fn parse_memory_bytes(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    let number_end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_digit() && ch != '.').then_some(index))
        .unwrap_or(trimmed.len());
    let number = &trimmed[..number_end];
    if number.is_empty() {
        return None;
    }
    let magnitude = number.parse::<f64>().ok()?;
    if !magnitude.is_finite() || magnitude < 0.0 {
        return None;
    }
    let multiplier = match trimmed[number_end..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1_u64,
        "K" | "KB" | "KIB" => 1_024,
        "M" | "MB" | "MIB" => 1_024_u64.pow(2),
        "G" | "GB" | "GIB" => GIB,
        "T" | "TB" | "TIB" => 1_024_u64.pow(4),
        "P" | "PB" | "PIB" => 1_024_u64.pow(5),
        _ => return None,
    };
    // Multiply in f64 to honor decimals, then clamp into u64 saturatingly.
    let bytes = magnitude * multiplier as f64;
    if bytes >= u64::MAX as f64 {
        Some(u64::MAX)
    } else {
        Some(bytes as u64)
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

    let has_days = input.contains('-');
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
    validate_walltime_ranges(&parts, has_days, input)?;
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

/// Validates the range of each `:`-separated time component.
///
/// The leading component (minutes for bare time-of-day forms, or hours when a
/// `D-` day prefix is present) is left unbounded to match Slurm, which only
/// allows a large value in the most-significant position. Every minutes and
/// seconds field must be 0-59, and when days are present the hours field is
/// bounded to 0-23.
fn validate_walltime_ranges(parts: &[u64], has_days: bool, input: &str) -> Result<()> {
    // Position of fields, from least to most significant, depends on length:
    //   [MM], [MM, SS], [HH, MM, SS]; a leading `D-` shifts HH out of the
    //   unbounded slot because days then carry the unbounded magnitude.
    let bad = match (parts, has_days) {
        // Bare minutes (`MM`) — leading field, unbounded.
        ([_minutes], false) => false,
        // Days plus bare hours (`D-HH`) — hours bounded, days unbounded.
        ([hours], true) => *hours > 23,
        // `MM:SS` — minutes leading and unbounded, seconds bounded.
        ([_minutes, seconds], false) => *seconds > 59,
        // `D-HH:MM` — hours and minutes bounded, days unbounded.
        ([hours, minutes], true) => *hours > 23 || *minutes > 59,
        // `HH:MM:SS` — hours leading and unbounded, minutes/seconds bounded.
        ([_hours, minutes, seconds], false) => *minutes > 59 || *seconds > 59,
        // `D-HH:MM:SS` — hours/minutes/seconds bounded, days unbounded.
        ([hours, minutes, seconds], true) => *hours > 23 || *minutes > 59 || *seconds > 59,
        _ => false,
    };
    if bad {
        bail!(
            "minutes and seconds must be 0-59 (and hours 0-23 with a day prefix) in time limit '{input}'"
        );
    }
    Ok(())
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
    ("array", "--array"),
    ("array", "-a"),
    ("after_job", "--dependency"),
    ("dependency", "--dependency"),
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
        "array" => slurm.array.is_some(),
        "after_job" => slurm.after_job.is_some(),
        "dependency" => slurm.dependency.is_some(),
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
mod tests;
