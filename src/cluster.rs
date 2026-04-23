//! Best-effort cluster capability profiles and plan compatibility checks.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::context::ResolvedBinaries;
use crate::preflight::{Item, Level, Report};
use crate::prepare::RuntimePlan;
use crate::spec::{RuntimeBackend, ScratchScope, parse_slurm_time_limit};

/// Relative location of the generated cluster profile.
pub const CLUSTER_PROFILE_RELATIVE_PATH: &str = ".hpc-compose/cluster.toml";

const CLUSTER_PROFILE_SCHEMA_VERSION: u32 = 1;

/// Complete generated cluster profile.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterProfile {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub generated_at_unix: Option<u64>,
    #[serde(default)]
    pub slurm_version: Option<String>,
    #[serde(default)]
    pub mpi_types: Vec<String>,
    #[serde(default)]
    pub partitions: Vec<PartitionProfile>,
    #[serde(default)]
    pub qos: Vec<String>,
    #[serde(default)]
    pub gpu_models: Vec<String>,
    #[serde(default)]
    pub runtimes: RuntimeAvailability,
    #[serde(default)]
    pub shared_cache_paths: Vec<String>,
}

/// Per-partition capability snapshot.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PartitionProfile {
    pub name: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub max_time: Option<String>,
    #[serde(default)]
    pub default_time: Option<String>,
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub cpus_per_node: Option<u32>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub qos: Vec<String>,
    #[serde(default)]
    pub default_qos: Option<String>,
}

/// Runtime backend availability snapshot.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAvailability {
    #[serde(default)]
    pub pyxis: bool,
    #[serde(default)]
    pub enroot: bool,
    #[serde(default)]
    pub apptainer: bool,
    #[serde(default)]
    pub singularity: bool,
    #[serde(default = "default_true")]
    pub host: bool,
}

/// One cluster-profile compatibility warning.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterValidationWarning {
    pub message: String,
    pub remediation: Option<String>,
}

/// Result of generating a cluster profile.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ClusterReportGeneration {
    pub profile: ClusterProfile,
    pub diagnostics: Report,
}

fn default_schema_version() -> u32 {
    CLUSTER_PROFILE_SCHEMA_VERSION
}

fn default_true() -> bool {
    true
}

impl ClusterProfile {
    /// Returns compatibility warnings for a runtime plan.
    #[must_use]
    pub fn validate_runtime_plan(&self, plan: &RuntimePlan) -> Vec<ClusterValidationWarning> {
        let mut warnings = Vec::new();
        self.check_runtime_backend(plan.runtime.backend, &mut warnings);
        self.check_partition(plan, &mut warnings);
        self.check_mpi(plan, &mut warnings);
        self.check_shared_paths(plan, &mut warnings);
        warnings
    }

    fn check_runtime_backend(
        &self,
        backend: RuntimeBackend,
        warnings: &mut Vec<ClusterValidationWarning>,
    ) {
        let available = match backend {
            RuntimeBackend::Pyxis => self.runtimes.pyxis && self.runtimes.enroot,
            RuntimeBackend::Apptainer => self.runtimes.apptainer,
            RuntimeBackend::Singularity => self.runtimes.singularity,
            RuntimeBackend::Host => self.runtimes.host,
        };
        if !available {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "cluster profile does not report runtime.backend={} as available",
                    backend.as_str()
                ),
                remediation: Some(
                    "Choose a supported runtime.backend for this cluster or regenerate the cluster profile on the target login node.".to_string(),
                ),
            });
        }
    }

    fn check_partition(&self, plan: &RuntimePlan, warnings: &mut Vec<ClusterValidationWarning>) {
        let Some(requested_partition) = plan.slurm.partition.as_deref() else {
            return;
        };
        if self.partitions.is_empty() {
            return;
        }
        let Some(partition) = self
            .partitions
            .iter()
            .find(|partition| partition.name == requested_partition)
        else {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "x-slurm.partition='{requested_partition}' is not present in the cluster profile"
                ),
                remediation: Some(
                    "Run 'hpc-compose doctor --cluster-report' on the target cluster or choose an available partition.".to_string(),
                ),
            });
            return;
        };

        if let Some(state) = partition.state.as_deref()
            && !partition_state_looks_available(state)
        {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "partition '{requested_partition}' is reported as state '{state}'"
                ),
                remediation: Some("Choose an available partition before submitting.".to_string()),
            });
        }

        if let (Some(requested), Some(max_time)) =
            (plan.slurm.time.as_deref(), partition.max_time.as_deref())
            && let (Ok(requested_seconds), Ok(max_seconds)) = (
                parse_slurm_time_limit(requested),
                parse_slurm_time_limit(max_time),
            )
            && requested_seconds > max_seconds
        {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "x-slurm.time='{requested}' exceeds partition '{requested_partition}' max_time='{max_time}'"
                ),
                remediation: Some("Reduce x-slurm.time or use a partition with a longer walltime.".to_string()),
            });
        }

        if plan_requests_gpu(plan) && !partition_supports_gpu(partition) {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "partition '{requested_partition}' does not report GPU GRES, but the plan requests GPUs"
                ),
                remediation: Some("Use a GPU-capable partition or remove GPU requests.".to_string()),
            });
        }

        if let Some(constraint) = plan.slurm.constraint.as_deref()
            && !constraint_matches_features(constraint, &partition.features)
        {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "x-slurm.constraint='{constraint}' was not found in partition '{requested_partition}' features"
                ),
                remediation: Some("Choose a known partition feature/constraint from the cluster profile.".to_string()),
            });
        }

        if let Some(qos) = plan.slurm.qos.as_deref()
            && !self.qos.is_empty()
            && !self.qos.iter().any(|known| known == qos)
            && !partition
                .qos
                .iter()
                .any(|known| known == "ALL" || known == qos)
        {
            warnings.push(ClusterValidationWarning {
                message: format!("x-slurm.qos='{qos}' is not present in the cluster profile"),
                remediation: Some(
                    "Choose an available QOS for the target account/partition.".to_string(),
                ),
            });
        }
    }

    fn check_mpi(&self, plan: &RuntimePlan, warnings: &mut Vec<ClusterValidationWarning>) {
        if self.mpi_types.is_empty() {
            return;
        }
        for service in &plan.ordered_services {
            let Some(mpi) = &service.slurm.mpi else {
                continue;
            };
            let requested = mpi.mpi_type.as_srun_value();
            if !self.mpi_types.iter().any(|known| known == requested) {
                warnings.push(ClusterValidationWarning {
                    message: format!(
                        "service '{}' requests MPI type '{requested}', but the cluster profile does not list it",
                        service.name
                    ),
                    remediation: Some("Use one of the MPI types reported by srun --mpi=list.".to_string()),
                });
            }
        }
    }

    fn check_shared_paths(&self, plan: &RuntimePlan, warnings: &mut Vec<ClusterValidationWarning>) {
        if self.shared_cache_paths.is_empty() {
            return;
        }
        let cache = plan.cache_dir.display().to_string();
        if !self.path_under_shared_candidate(&cache) {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "x-slurm.cache_dir '{}' is not under a shared path recorded in the cluster profile",
                    plan.cache_dir.display()
                ),
                remediation: Some("Use a shared filesystem path visible from login and compute nodes.".to_string()),
            });
        }
        if let Some(scratch) = &plan.slurm.scratch
            && scratch.scope == ScratchScope::Shared
            && !self.path_under_shared_candidate(&scratch.base)
        {
            warnings.push(ClusterValidationWarning {
                message: format!(
                    "x-slurm.scratch.base '{}' is marked shared but is not under a shared path recorded in the cluster profile",
                    scratch.base
                ),
                remediation: Some("Use a known shared scratch/work path or set scratch.scope=node_local.".to_string()),
            });
        }
    }

    fn path_under_shared_candidate(&self, path: &str) -> bool {
        let path = Path::new(path);
        self.shared_cache_paths
            .iter()
            .any(|candidate| !candidate.is_empty() && path.starts_with(Path::new(candidate)))
    }
}

/// Generates a best-effort cluster profile from local Slurm/runtime tools.
#[must_use]
pub fn generate_cluster_profile(binaries: &ResolvedBinaries) -> ClusterReportGeneration {
    let mut diagnostics = Report { items: Vec::new() };
    let slurm_version = run_capture(&binaries.sbatch.value, &["--version"], &mut diagnostics)
        .or_else(|| run_capture(&binaries.srun.value, &["--version"], &mut diagnostics));
    let partitions = collect_partitions(binaries, &mut diagnostics);
    let mpi_types = run_capture(&binaries.srun.value, &["--mpi=list"], &mut diagnostics)
        .map(|raw| advertised_mpi_types(&raw))
        .unwrap_or_default();
    let runtimes = RuntimeAvailability {
        pyxis: srun_has_pyxis(&binaries.srun.value),
        enroot: binary_available(&binaries.enroot.value),
        apptainer: binary_available(&binaries.apptainer.value),
        singularity: binary_available(&binaries.singularity.value),
        host: true,
    };
    let gpu_models = collect_gpu_models(&partitions);
    let qos = collect_qos(&partitions);
    let shared_cache_paths = collect_shared_path_candidates();
    ClusterReportGeneration {
        profile: ClusterProfile {
            schema_version: CLUSTER_PROFILE_SCHEMA_VERSION,
            generated_at_unix: Some(unix_timestamp_now()),
            slurm_version,
            mpi_types,
            partitions,
            qos,
            gpu_models,
            runtimes,
            shared_cache_paths,
        },
        diagnostics,
    }
}

/// Resolves the default profile path relative to the current repository or cwd.
#[must_use]
pub fn default_cluster_profile_path(start: &Path) -> PathBuf {
    crate::context::repo_root_or_cwd(start).join(CLUSTER_PROFILE_RELATIVE_PATH)
}

/// Discovers `.hpc-compose/cluster.toml` by searching upward from `start`.
#[must_use]
pub fn discover_cluster_profile_path(start: &Path) -> Option<PathBuf> {
    for dir in start.ancestors() {
        let candidate = dir.join(CLUSTER_PROFILE_RELATIVE_PATH);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Loads a cluster profile from disk.
pub fn load_cluster_profile(path: &Path) -> Result<ClusterProfile> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read cluster profile {}", path.display()))?;
    let profile: ClusterProfile = toml::from_str(&raw)
        .with_context(|| format!("failed to parse cluster profile {}", path.display()))?;
    if profile.schema_version != CLUSTER_PROFILE_SCHEMA_VERSION {
        bail!(
            "unsupported cluster profile schema version {}; expected {}",
            profile.schema_version,
            CLUSTER_PROFILE_SCHEMA_VERSION
        );
    }
    Ok(profile)
}

/// Writes a cluster profile to disk, creating parent directories.
pub fn write_cluster_profile(path: &Path, profile: &ClusterProfile) -> Result<()> {
    if profile.schema_version != CLUSTER_PROFILE_SCHEMA_VERSION {
        bail!(
            "refusing to write cluster profile with unsupported schema version {}",
            profile.schema_version
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered =
        toml::to_string_pretty(profile).context("failed to serialize cluster profile")?;
    fs::write(path, rendered).with_context(|| format!("failed to write {}", path.display()))
}

fn collect_partitions(
    binaries: &ResolvedBinaries,
    diagnostics: &mut Report,
) -> Vec<PartitionProfile> {
    let mut partitions = Vec::new();
    if let Some(raw) = run_capture(
        &binaries.sinfo.value,
        &["-h", "-o", "%P|%a|%l|%D|%c|%G|%f"],
        diagnostics,
    ) {
        for line in raw.lines() {
            let fields = line.split('|').collect::<Vec<_>>();
            if fields.len() != 7 {
                continue;
            }
            let name = fields[0].trim_end_matches('*').trim().to_string();
            if name.is_empty() {
                continue;
            }
            partitions.push(PartitionProfile {
                name,
                state: non_empty(fields[1]),
                max_time: non_empty(fields[2]).filter(|value| value != "infinite"),
                nodes: fields[3].trim().parse::<u32>().ok(),
                cpus_per_node: fields[4].trim().parse::<u32>().ok(),
                gres: non_empty(fields[5]).filter(|value| value != "(null)" && value != "N/A"),
                features: split_features(fields[6]),
                ..PartitionProfile::default()
            });
        }
    }
    if let Some(raw) = run_capture(
        &binaries.scontrol.value,
        &["show", "partition", "-o"],
        diagnostics,
    ) {
        merge_scontrol_partitions(&mut partitions, &raw);
    }
    partitions.sort_by(|left, right| left.name.cmp(&right.name));
    partitions.dedup_by(|left, right| left.name == right.name);
    partitions
}

fn merge_scontrol_partitions(partitions: &mut Vec<PartitionProfile>, raw: &str) {
    for line in raw.lines() {
        let attrs = line
            .split_whitespace()
            .filter_map(|part| part.split_once('='))
            .collect::<std::collections::BTreeMap<_, _>>();
        let Some(name) = attrs.get("PartitionName").copied() else {
            continue;
        };
        let index = partitions
            .iter()
            .position(|partition| partition.name == name)
            .unwrap_or_else(|| {
                partitions.push(PartitionProfile {
                    name: name.to_string(),
                    ..PartitionProfile::default()
                });
                partitions.len() - 1
            });
        let partition = &mut partitions[index];
        if partition.state.is_none() {
            partition.state = attrs.get("State").map(|value| (*value).to_string());
        }
        if partition.max_time.is_none() {
            partition.max_time = attrs
                .get("MaxTime")
                .copied()
                .and_then(non_empty)
                .filter(|value| value != "UNLIMITED");
        }
        if partition.default_time.is_none() {
            partition.default_time = attrs
                .get("DefaultTime")
                .copied()
                .and_then(non_empty)
                .filter(|value| value != "NONE");
        }
        if partition.gres.is_none() {
            partition.gres = attrs
                .get("TRES")
                .or_else(|| attrs.get("Gres"))
                .map(|value| (*value).to_string());
        }
        if partition.qos.is_empty()
            && let Some(qos) = attrs.get("AllowQos").or_else(|| attrs.get("QoS"))
        {
            partition.qos = split_csv(qos);
        }
        if partition.default_qos.is_none() {
            partition.default_qos = attrs.get("DefaultQOS").map(|value| (*value).to_string());
        }
    }
}

fn run_capture(bin: &str, args: &[&str], diagnostics: &mut Report) -> Option<String> {
    match Command::new(bin).args(args).output() {
        Ok(output) if output.status.success() => {
            diagnostics.items.push(Item {
                level: Level::Ok,
                message: format!("captured {} {}", bin, args.join(" ")),
                remediation: None,
            });
            let text = String::from_utf8_lossy(&output.stdout).to_string()
                + &String::from_utf8_lossy(&output.stderr);
            Some(text.trim().to_string()).filter(|value| !value.is_empty())
        }
        Ok(output) => {
            diagnostics.items.push(Item {
                level: Level::Warn,
                message: format!(
                    "{} {} exited with status {}",
                    bin,
                    args.join(" "),
                    output.status
                ),
                remediation: Some("Cluster report generation is best-effort; missing fields can be edited manually in .hpc-compose/cluster.toml.".to_string()),
            });
            None
        }
        Err(err) => {
            diagnostics.items.push(Item {
                level: Level::Warn,
                message: format!("failed to run {} {}: {err}", bin, args.join(" ")),
                remediation: Some(
                    "Install the Slurm client tool or edit the generated cluster profile manually."
                        .to_string(),
                ),
            });
            None
        }
    }
}

fn srun_has_pyxis(srun_bin: &str) -> bool {
    let output = Command::new(srun_bin).arg("--help").output();
    let Ok(output) = output else {
        return false;
    };
    let text = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);
    text.contains("--container-image")
}

fn binary_available(binary: &str) -> bool {
    if binary.contains(std::path::MAIN_SEPARATOR) {
        return Path::new(binary).exists();
    }
    env::var_os("PATH")
        .map(|path| env::split_paths(&path).any(|dir| dir.join(binary).exists()))
        .unwrap_or(false)
}

fn advertised_mpi_types(output: &str) -> Vec<String> {
    let mut values = output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '+')))
        .filter(|token| mpi_advertised_token_looks_useful(token))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn mpi_advertised_token_looks_useful(token: &str) -> bool {
    if token.is_empty() || token.starts_with('-') {
        return false;
    }
    let lower = token.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "mpi"
            | "plugin"
            | "plugins"
            | "type"
            | "types"
            | "are"
            | "available"
            | "specific"
            | "version"
            | "versions"
    ) {
        return false;
    }
    token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
}

fn collect_gpu_models(partitions: &[PartitionProfile]) -> Vec<String> {
    let mut models = BTreeSet::new();
    for partition in partitions {
        let Some(gres) = partition.gres.as_deref() else {
            continue;
        };
        for part in gres.split(',') {
            let fields = part.split(':').collect::<Vec<_>>();
            if fields.first().is_some_and(|value| value.contains("gpu"))
                && let Some(model) = fields.get(1)
                && !model.chars().all(|ch| ch.is_ascii_digit())
            {
                models.insert((*model).to_string());
            }
        }
    }
    models.into_iter().collect()
}

fn collect_qos(partitions: &[PartitionProfile]) -> Vec<String> {
    let mut qos = BTreeSet::new();
    for partition in partitions {
        for item in &partition.qos {
            if item != "ALL" {
                qos.insert(item.clone());
            }
        }
        if let Some(default_qos) = &partition.default_qos {
            qos.insert(default_qos.clone());
        }
    }
    qos.into_iter().collect()
}

fn collect_shared_path_candidates() -> Vec<String> {
    let mut paths = BTreeSet::new();
    for key in [
        "WORK",
        "WORKSPACE",
        "PROJECT",
        "PROJECT_HOME",
        "SCRATCH",
        "SCRATCHDIR",
    ] {
        if let Ok(value) = env::var(key)
            && Path::new(&value).is_absolute()
        {
            paths.insert(value);
        }
    }
    if let Ok(home) = env::var("HOME")
        && Path::new(&home).is_absolute()
    {
        paths.insert(home);
    }
    paths.into_iter().collect()
}

fn split_features(value: &str) -> Vec<String> {
    split_csv(value)
        .into_iter()
        .filter(|feature| feature != "(null)" && feature != "N/A")
        .collect()
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty() && *item != "NONE")
        .map(str::to_string)
        .collect()
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn partition_state_looks_available(state: &str) -> bool {
    let normalized = state.to_ascii_lowercase();
    normalized == "up" || normalized == "idle" || normalized == "mixed"
}

fn partition_supports_gpu(partition: &PartitionProfile) -> bool {
    partition.gres.as_deref().is_some_and(contains_gpu)
}

fn constraint_matches_features(constraint: &str, features: &[String]) -> bool {
    constraint
        .split('&')
        .map(|group| {
            group
                .split('|')
                .map(normalize_constraint_token)
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|alternatives| !alternatives.is_empty())
        .all(|alternatives| {
            alternatives
                .iter()
                .any(|part| features.iter().any(|feature| feature == part))
        })
}

fn normalize_constraint_token(part: &str) -> &str {
    part.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-')
}

fn plan_requests_gpu(plan: &RuntimePlan) -> bool {
    plan.slurm.gpus.unwrap_or(0) > 0
        || plan.slurm.gpus_per_node.unwrap_or(0) > 0
        || plan.slurm.gpus_per_task.unwrap_or(0) > 0
        || plan.slurm.cpus_per_gpu.unwrap_or(0) > 0
        || plan.slurm.mem_per_gpu.is_some()
        || plan.slurm.gres.as_deref().is_some_and(contains_gpu)
        || plan.ordered_services.iter().any(|service| {
            service.slurm.gpus.unwrap_or(0) > 0
                || service.slurm.gpus_per_node.unwrap_or(0) > 0
                || service.slurm.gpus_per_task.unwrap_or(0) > 0
                || service.slurm.cpus_per_gpu.unwrap_or(0) > 0
                || service.slurm.mem_per_gpu.is_some()
                || service.slurm.gres.as_deref().is_some_and(contains_gpu)
        })
}

fn contains_gpu(value: &str) -> bool {
    value.to_ascii_lowercase().contains("gpu")
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    use crate::context::{ResolvedValue, ValueSource};
    use crate::planner::{ExecutionSpec, ImageSource, ServicePlacement};
    use crate::prepare::RuntimeService;
    use crate::spec::{
        MpiConfig, MpiType, RuntimeConfig, ScratchConfig, ServiceFailurePolicy, ServiceSlurmConfig,
        SlurmConfig,
    };

    fn resolved_binary(path: PathBuf) -> ResolvedValue<String> {
        ResolvedValue {
            value: path.display().to_string(),
            source: ValueSource::Cli,
        }
    }

    fn resolved_binaries(tmpdir: &Path) -> ResolvedBinaries {
        ResolvedBinaries {
            enroot: resolved_binary(tmpdir.join("enroot")),
            apptainer: resolved_binary(tmpdir.join("missing-apptainer")),
            singularity: resolved_binary(tmpdir.join("missing-singularity")),
            sbatch: resolved_binary(tmpdir.join("sbatch")),
            srun: resolved_binary(tmpdir.join("srun")),
            scontrol: resolved_binary(tmpdir.join("scontrol")),
            sinfo: resolved_binary(tmpdir.join("sinfo")),
            squeue: resolved_binary(tmpdir.join("squeue")),
            sacct: resolved_binary(tmpdir.join("sacct")),
            sstat: resolved_binary(tmpdir.join("sstat")),
            scancel: resolved_binary(tmpdir.join("scancel")),
        }
    }

    fn write_executable(path: &Path, body: &str) {
        fs::write(path, body).expect("write executable");
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod executable");
    }

    fn basic_runtime_plan(cache_dir: PathBuf) -> RuntimePlan {
        RuntimePlan {
            name: "demo".into(),
            cache_dir,
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Pyxis,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "trainer".into(),
                runtime_image: PathBuf::from("/shared/cache/trainer.sqsh"),
                execution: ExecutionSpec::Shell("echo train".into()),
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
            }],
        }
    }

    #[test]
    fn cluster_profile_defaults_paths_round_trip_and_reject_bad_schema() {
        let profile: ClusterProfile =
            toml::from_str("[runtimes]\npyxis = true\nenroot = true\n").expect("profile");
        assert_eq!(profile.schema_version, CLUSTER_PROFILE_SCHEMA_VERSION);
        assert!(profile.runtimes.host);

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let repo = tmpdir.path().join("repo");
        let nested = repo.join("a/b");
        fs::create_dir_all(&nested).expect("nested");
        fs::create_dir_all(repo.join(".git")).expect("git");
        let path = repo.join(CLUSTER_PROFILE_RELATIVE_PATH);

        write_cluster_profile(&path, &profile).expect("write profile");
        assert_eq!(load_cluster_profile(&path).expect("load profile"), profile);
        assert_eq!(discover_cluster_profile_path(&nested), Some(path.clone()));
        assert_eq!(default_cluster_profile_path(&nested), path);

        fs::write(&path, "schema_version = 2\n").expect("bad schema");
        let err = load_cluster_profile(&path).expect_err("schema rejected");
        assert!(
            err.to_string()
                .contains("unsupported cluster profile schema version")
        );

        let mut invalid = profile;
        invalid.schema_version = 2;
        let err = write_cluster_profile(&repo.join("bad.toml"), &invalid)
            .expect_err("write rejects schema");
        assert!(
            err.to_string()
                .contains("refusing to write cluster profile")
        );
    }

    #[test]
    fn generate_cluster_profile_parses_fake_slurm_and_runtime_tools() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let enroot = tmpdir.path().join("enroot");
        fs::write(&enroot, "").expect("enroot marker");
        write_executable(
            &tmpdir.path().join("sbatch"),
            "#!/bin/sh\nif [ \"${1:-}\" = \"--version\" ]; then echo 'slurm 24.05.1' >&2; exit 0; fi\nexit 2\n",
        );
        write_executable(
            &tmpdir.path().join("srun"),
            "#!/bin/sh\ncase \"${1:-}\" in\n  --version) echo 'srun 24.05.1' ;;\n  --mpi=list) echo 'MPI plugin types are pmix,pmi2 openmpi pmix' ;;\n  --help) echo 'usage: srun --container-image=IMAGE' ;;\n  *) exit 0 ;;\nesac\n",
        );
        write_executable(
            &tmpdir.path().join("sinfo"),
            "#!/bin/sh\nprintf '%s\\n' 'gpu*|up|02:00:00|4|128|gpu:a100:4|a100,ib' 'broken|row' '|up|01:00:00|1|32|N/A|N/A'\n",
        );
        write_executable(
            &tmpdir.path().join("scontrol"),
            "#!/bin/sh\nprintf '%s\\n' 'PartitionName=gpu State=UP MaxTime=04:00:00 DefaultTime=01:00:00 TRES=cpu=128,gres/gpu:a100:4 AllowQos=normal,debug DefaultQOS=normal' 'PartitionName=cpu State=DOWN MaxTime=UNLIMITED DefaultTime=NONE Gres=(null) AllowQos=ALL'\n",
        );

        let generated = generate_cluster_profile(&resolved_binaries(tmpdir.path()));
        let profile = generated.profile;
        assert_eq!(profile.slurm_version.as_deref(), Some("slurm 24.05.1"));
        assert_eq!(
            profile.mpi_types,
            vec!["openmpi".to_string(), "pmi2".into(), "pmix".into()]
        );
        assert!(profile.runtimes.pyxis);
        assert!(profile.runtimes.enroot);
        assert!(!profile.runtimes.apptainer);
        assert!(!profile.runtimes.singularity);
        assert_eq!(profile.gpu_models, vec!["a100".to_string()]);
        assert_eq!(profile.qos, vec!["debug".to_string(), "normal".into()]);

        let gpu = profile
            .partitions
            .iter()
            .find(|partition| partition.name == "gpu")
            .expect("gpu partition");
        assert_eq!(gpu.state.as_deref(), Some("up"));
        assert_eq!(gpu.max_time.as_deref(), Some("02:00:00"));
        assert_eq!(gpu.default_time.as_deref(), Some("01:00:00"));
        assert_eq!(gpu.nodes, Some(4));
        assert_eq!(gpu.cpus_per_node, Some(128));
        assert_eq!(gpu.features, vec!["a100".to_string(), "ib".into()]);
        assert_eq!(gpu.qos, vec!["normal".to_string(), "debug".into()]);

        let cpu = profile
            .partitions
            .iter()
            .find(|partition| partition.name == "cpu")
            .expect("cpu partition from scontrol");
        assert_eq!(cpu.state.as_deref(), Some("DOWN"));
        assert!(cpu.max_time.is_none());
        assert!(cpu.default_time.is_none());
        assert_eq!(cpu.qos, vec!["ALL".to_string()]);

        assert!(
            generated
                .diagnostics
                .items
                .iter()
                .any(|item| { item.level == Level::Ok && item.message.contains("captured") })
        );
    }

    #[test]
    fn run_capture_reports_success_empty_failure_and_spawn_errors() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let tool = tmpdir.path().join("tool");
        write_executable(
            &tool,
            "#!/bin/sh\ncase \"${1:-}\" in\n  ok) printf 'out\\n'; printf 'err\\n' >&2 ;;\n  empty) exit 0 ;;\n  fail) exit 7 ;;\nesac\n",
        );
        let mut diagnostics = Report { items: Vec::new() };

        assert_eq!(
            run_capture(tool.to_str().expect("path"), &["ok"], &mut diagnostics).as_deref(),
            Some("out\nerr")
        );
        assert!(run_capture(tool.to_str().expect("path"), &["empty"], &mut diagnostics).is_none());
        assert!(run_capture(tool.to_str().expect("path"), &["fail"], &mut diagnostics).is_none());
        assert!(
            run_capture(
                tmpdir.path().join("missing").to_str().expect("path"),
                &["x"],
                &mut diagnostics
            )
            .is_none()
        );

        let text = diagnostics.render_verbose();
        assert!(text.contains("captured"));
        assert!(text.contains("exited with status"));
        assert!(text.contains("failed to run"));
    }

    #[test]
    fn cluster_helper_parsers_match_exact_features_and_deduplicate_sets() {
        assert_eq!(
            advertised_mpi_types("MPI plugin types: pmix pmi2 pmix_v4 openmpi unknown"),
            vec![
                "openmpi".to_string(),
                "pmi2".into(),
                "pmix".into(),
                "pmix_v4".into(),
                "unknown".into()
            ]
        );
        assert_eq!(
            split_features("a100,(null),N/A,ib"),
            vec!["a100".to_string(), "ib".into()]
        );
        assert_eq!(
            split_csv("normal, ALL, NONE,debug"),
            vec!["normal".to_string(), "ALL".into(), "debug".into()]
        );
        assert_eq!(non_empty("  value  ").as_deref(), Some("value"));
        assert!(non_empty(" \t ").is_none());

        let features = vec!["a100".to_string(), "ib".into()];
        assert!(constraint_matches_features("(a100|v100)&ib", &features));
        assert!(!constraint_matches_features("a100&hbm", &features));
        assert!(!constraint_matches_features("gpu", &["nogpu".to_string()]));

        let partitions = vec![
            PartitionProfile {
                name: "gpu-a".into(),
                gres: Some("gpu:a100:4,gpu:2".into()),
                qos: vec!["normal".into(), "ALL".into()],
                default_qos: Some("debug".into()),
                ..PartitionProfile::default()
            },
            PartitionProfile {
                name: "gpu-h".into(),
                gres: Some("gres/gpu:h100:8".into()),
                ..PartitionProfile::default()
            },
        ];
        assert_eq!(
            collect_gpu_models(&partitions),
            vec!["a100".to_string(), "h100".into()]
        );
        assert_eq!(
            collect_qos(&partitions),
            vec!["debug".to_string(), "normal".into()]
        );
        assert!(partition_state_looks_available("MIXED"));
        assert!(!partition_state_looks_available("down"));
        assert!(partition_supports_gpu(&PartitionProfile {
            name: "gpu".into(),
            gres: Some("GPU:a100:1".into()),
            ..PartitionProfile::default()
        }));
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("enroot");
        fs::write(&path, "").expect("marker");
        assert!(binary_available(path.to_str().expect("path")));
        assert!(!binary_available(
            tmpdir.path().join("missing").to_str().expect("path")
        ));
    }

    #[test]
    fn cluster_profile_validation_accepts_or_constraints_and_rejects_path_prefixes() {
        let profile = ClusterProfile {
            schema_version: 1,
            generated_at_unix: None,
            slurm_version: None,
            mpi_types: Vec::new(),
            partitions: vec![PartitionProfile {
                name: "gpu".into(),
                state: Some("UP".into()),
                max_time: Some("04:00:00".into()),
                gres: Some("GPU:a100:4".into()),
                features: vec!["a100".into(), "ib".into()],
                qos: vec!["debug".into()],
                ..PartitionProfile::default()
            }],
            qos: vec!["normal".into()],
            gpu_models: Vec::new(),
            runtimes: RuntimeAvailability {
                pyxis: true,
                enroot: true,
                apptainer: false,
                singularity: false,
                host: true,
            },
            shared_cache_paths: vec!["/shared".into()],
        };
        let mut plan = basic_runtime_plan(PathBuf::from("/shared/cache"));
        plan.slurm = SlurmConfig {
            partition: Some("gpu".into()),
            qos: Some("debug".into()),
            time: Some("00:30:00".into()),
            constraint: Some("(a100|v100)&ib".into()),
            scratch: Some(ScratchConfig {
                scope: ScratchScope::Shared,
                base: "/shared/scratch".into(),
                mount: "/scratch".into(),
                cleanup: Default::default(),
            }),
            ..SlurmConfig::default()
        };
        plan.ordered_services[0].slurm.gres = Some("GPU:a100:1".into());

        let warnings = profile.validate_runtime_plan(&plan);
        assert_eq!(warnings, Vec::<ClusterValidationWarning>::new());

        let mut wildcard_qos_profile = profile.clone();
        wildcard_qos_profile.partitions[0].qos = vec!["ALL".into()];
        let mut wildcard_qos_plan = plan.clone();
        wildcard_qos_plan.slurm.qos = Some("long".into());
        let warnings = wildcard_qos_profile.validate_runtime_plan(&wildcard_qos_plan);
        assert!(
            !warnings
                .iter()
                .any(|warning| warning.message.contains("x-slurm.qos"))
        );

        let mut bad_plan = plan;
        bad_plan.cache_dir = PathBuf::from("/sharedness/cache");
        bad_plan.slurm.scratch = Some(ScratchConfig {
            scope: ScratchScope::Shared,
            base: "/sharedness/scratch".into(),
            mount: "/scratch".into(),
            cleanup: Default::default(),
        });
        let warnings = profile.validate_runtime_plan(&bad_plan);
        let messages = warnings
            .iter()
            .map(|warning| warning.message.as_str())
            .collect::<Vec<_>>();
        assert!(messages.iter().any(|message| message.contains("cache_dir")));
        assert!(
            messages
                .iter()
                .any(|message| message.contains("scratch.base"))
        );
    }

    #[test]
    fn cluster_profile_warns_about_incompatible_runtime_partition_and_mpi() {
        let profile = ClusterProfile {
            schema_version: 1,
            generated_at_unix: None,
            slurm_version: None,
            mpi_types: vec!["pmix".into()],
            partitions: vec![PartitionProfile {
                name: "cpu".into(),
                state: Some("up".into()),
                max_time: Some("01:00:00".into()),
                features: vec!["cpu".into()],
                ..PartitionProfile::default()
            }],
            qos: vec!["normal".into()],
            gpu_models: Vec::new(),
            runtimes: RuntimeAvailability {
                pyxis: false,
                enroot: false,
                apptainer: true,
                singularity: false,
                host: true,
            },
            shared_cache_paths: vec!["/shared".into()],
        };
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/tmp/cache"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Pyxis,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig {
                partition: Some("cpu".into()),
                time: Some("02:00:00".into()),
                gpus: Some(1),
                ..SlurmConfig::default()
            },
            ordered_services: vec![RuntimeService {
                name: "trainer".into(),
                runtime_image: PathBuf::from("/shared/cache/trainer.sqsh"),
                execution: ExecutionSpec::Shell("echo train".into()),
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig {
                    mpi: Some(MpiConfig {
                        mpi_type: MpiType::new("pmi2").expect("mpi type"),
                        implementation: None,
                        launcher: Default::default(),
                        expected_ranks: None,
                        host_mpi: None,
                    }),
                    ..ServiceSlurmConfig::default()
                },
                prepare: None,
                source: ImageSource::Remote("docker://ubuntu:24.04".into()),
            }],
        };

        let warnings = profile.validate_runtime_plan(&plan);
        let messages = warnings
            .iter()
            .map(|warning| warning.message.as_str())
            .collect::<Vec<_>>();
        assert!(
            messages
                .iter()
                .any(|message| message.contains("runtime.backend=pyxis"))
        );
        assert!(messages.iter().any(|message| message.contains("max_time")));
        assert!(
            messages
                .iter()
                .any(|message| message.contains("does not report GPU"))
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("MPI type 'pmi2'"))
        );
        assert!(messages.iter().any(|message| message.contains("cache_dir")));
    }
}
