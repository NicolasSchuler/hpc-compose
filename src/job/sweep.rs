use super::*;
use crate::spec::{SweepConfig, SweepMatrix, SweepObjective};
use crate::time_util::unix_timestamp_millis;

/// Schema version for persisted sweep manifests.
///
/// v3 added per-trial `config_key`/`replicate`/`seed` fields for `replicates: N`
/// sweeps. Every new field carries `serde(default)`, so v2 (and older) manifests
/// still deserialize: missing fields default to an empty `config_key`, replicate
/// `0`, and no seed.
pub const SWEEP_MANIFEST_SCHEMA_VERSION: u32 = 3;

/// One generated sweep trial before submission.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SweepExpansionTrial {
    pub trial_id: String,
    pub index: usize,
    pub variables: BTreeMap<String, String>,
    /// Stable key identifying the parameter config this trial replicates.
    ///
    /// All replicates of the same parameter combination share this key; it is
    /// used to group replicates for the mean±std(n) rollup.
    #[serde(default)]
    pub config_key: String,
    /// Zero-based replicate index within this config (`0` when replicates == 1).
    #[serde(default)]
    pub replicate: u32,
    /// Deterministic per-replicate seed, present only when replicates > 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
}

/// Deterministic sweep expansion result.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SweepExpansion {
    pub sweep_id: String,
    pub matrix: String,
    pub seed: Option<String>,
    pub total_combinations: usize,
    pub trials: Vec<SweepExpansionTrial>,
}

/// Persisted sweep run metadata.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct SweepManifest {
    pub schema_version: u32,
    pub sweep_id: String,
    pub compose_file: PathBuf,
    pub submitted_at: u64,
    pub matrix: String,
    /// SHA-256 (lowercase hex) of the compose file's bytes at original submit
    /// time. `--resume` compares the current file's hash against this to warn
    /// about service-level spec drift (an edited `command:`, `image:`, etc.)
    /// that the sweep-block drift guard cannot see. Absent on manifests written
    /// before this field existed (loads as `None`; the resume check is skipped).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compose_file_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
    pub total_combinations: usize,
    /// Snapshot of the sweep objective, if configured, used by `sweep observe`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<SweepObjective>,
    /// Trial id of the best trial observed so far, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub best_trial: Option<String>,
    #[serde(default)]
    pub stopped_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    pub trials: Vec<SweepManifestTrial>,
}

/// Persisted metadata for one sweep trial.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct SweepManifestTrial {
    pub trial_id: String,
    pub index: usize,
    pub variables: BTreeMap<String, String>,
    /// Stable key grouping replicates of the same parameter config.
    #[serde(default)]
    pub config_key: String,
    /// Zero-based replicate index within this config (`0` when replicates == 1).
    #[serde(default)]
    pub replicate: u32,
    /// Deterministic per-replicate seed, present only when replicates > 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
    pub script_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_error: Option<String>,
    /// Objective value parsed by `sweep observe`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<u64>,
}

/// Builds a collision-resistant, human-readable sweep id.
#[must_use]
pub fn generate_sweep_id() -> String {
    format!("sweep-{}-{}", unix_timestamp_millis(), std::process::id())
}

/// Computes the lowercase hex SHA-256 digest of a compose file's bytes.
///
/// Recorded on the manifest at original submit time so `--resume` can detect
/// service-level spec drift (an edited `command:`, `image:`, etc.) that the
/// sweep-block drift guard does not cover.
///
/// # Errors
///
/// Returns an error when the file cannot be read.
pub fn compose_file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read {} for content hashing", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex::encode(hasher.finalize()))
}

/// Expands an embedded sweep config into deterministic trial variables.
///
/// # Errors
///
/// Returns an error when the matrix is too large or an invalid random sample is
/// requested.
pub fn expand_sweep(config: &SweepConfig, sweep_id: &str) -> Result<SweepExpansion> {
    expand_sweep_with_limit(config, sweep_id, None)
}

/// Expands a sweep while enforcing an optional cap on materialized trials.
///
/// Random sweeps sample deterministic trial indexes instead of materializing the
/// full Cartesian product first.
pub fn expand_sweep_with_limit(
    config: &SweepConfig,
    sweep_id: &str,
    max_trials: Option<usize>,
) -> Result<SweepExpansion> {
    let total_combinations = config.total_trials()?;
    let replicates = config.replicates;
    // The submission guard counts materialized runs (combinations * replicates),
    // not bare combinations: each replicate is a separate allocation.
    let total_runs = config.total_runs()?;
    match &config.matrix {
        SweepMatrix::Full if max_trials.is_some_and(|max_trials| total_runs > max_trials) => {
            let max_trials = max_trials.expect("checked above");
            bail!(
                "sweep expands to {total_runs} runs, above the limit of {max_trials} ({total_combinations} configs x {replicates} replicates); rerun with --max-trials {total_runs} or larger to submit intentionally"
            );
        }
        SweepMatrix::Full => Ok(SweepExpansion {
            sweep_id: sweep_id.to_string(),
            matrix: "full".to_string(),
            seed: None,
            total_combinations,
            trials: assign_trial_ids(full_product_trials(config, sweep_id), sweep_id, replicates),
        }),
        SweepMatrix::Random { random, seed } => {
            if *random > total_combinations {
                bail!(
                    "sweep.matrix.random requests {random} trials but only {total_combinations} combinations exist"
                );
            }
            // `random` bounds the number of sampled configs; the per-config
            // replicate fan-out then multiplies that into the run count.
            let sampled_runs = random
                .checked_mul(replicates as usize)
                .with_context(|| "sweep run matrix is too large".to_string())?;
            if let Some(max_trials) = max_trials
                && sampled_runs > max_trials
            {
                bail!(
                    "sweep expands to {sampled_runs} runs, above the limit of {max_trials} ({random} sampled configs x {replicates} replicates); rerun with --max-trials {sampled_runs} or larger to submit intentionally"
                );
            }
            let resolved_seed = seed.clone().unwrap_or_else(|| sweep_id.to_string());
            let sampled = sample_trial_indices(&resolved_seed, total_combinations, *random)
                .into_iter()
                .map(|index| trial_at_index(config, index))
                .collect::<Vec<_>>();
            Ok(SweepExpansion {
                sweep_id: sweep_id.to_string(),
                matrix: "random".to_string(),
                seed: Some(resolved_seed),
                total_combinations,
                trials: assign_trial_ids(sampled, sweep_id, replicates),
            })
        }
    }
}

fn full_product_trials(config: &SweepConfig, sweep_id: &str) -> Vec<BTreeMap<String, String>> {
    let mut trials = vec![BTreeMap::new()];
    for (name, values) in &config.parameters {
        let mut next = Vec::new();
        for base in &trials {
            for value in values {
                let mut variables = base.clone();
                variables.insert(name.clone(), value.as_str().to_string());
                next.push(variables);
            }
        }
        trials = next;
    }
    if trials.is_empty() {
        let mut variables = BTreeMap::new();
        variables.insert("HPC_COMPOSE_SWEEP_ID".to_string(), sweep_id.to_string());
        trials.push(variables);
    }
    trials
}

/// Materializes expansion trials from base parameter configs, fanning each
/// config into `replicates` seeded trials.
///
/// With `replicates == 1` this is byte-identical to the legacy expansion:
/// trial ids stay `t{index:03}`, `config_key` is the readable variable join,
/// `replicate` is `0`, and `seed` is `None`. With `replicates > 1` each config
/// `c` fans out into `t{c:03}r0..t{c:03}r{N-1}` with a deterministic
/// per-replicate seed.
fn assign_trial_ids(
    configs: Vec<BTreeMap<String, String>>,
    sweep_id: &str,
    replicates: u32,
) -> Vec<SweepExpansionTrial> {
    let mut trials = Vec::with_capacity(configs.len() * replicates.max(1) as usize);
    let mut index = 0_usize;
    for (config_index, variables) in configs.into_iter().enumerate() {
        let config_key = config_key_for(&variables);
        for replicate in 0..replicates {
            let (trial_id, seed) = if replicates <= 1 {
                // Back-compat: keep the legacy `t000` ids and no per-replicate
                // seed when no fan-out is requested.
                (format!("t{config_index:03}"), None)
            } else {
                (
                    format!("t{config_index:03}r{replicate}"),
                    Some(replicate_seed(sweep_id, &config_key, replicate)),
                )
            };
            trials.push(SweepExpansionTrial {
                trial_id,
                index,
                variables: variables.clone(),
                config_key: config_key.clone(),
                replicate,
                seed,
            });
            index += 1;
        }
    }
    trials
}

/// Builds a stable, human-readable key identifying a parameter config.
///
/// The variables come from a `BTreeMap`, so they are already sorted by name;
/// the key is `name=value` pairs joined by `;`. An empty config (no parameters)
/// yields an empty string. This doubles as the grouped-row display label.
fn config_key_for(variables: &BTreeMap<String, String>) -> String {
    variables
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// Derives a deterministic per-replicate seed as a hex SHA-256 digest of
/// `sweep_id:config_key:replicate`.
///
/// The same `SweepConfig` + `sweep_id` always produces the same seed for a
/// given config/replicate, so user training scripts can reproduce it.
fn replicate_seed(sweep_id: &str, config_key: &str, replicate: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sweep_id.as_bytes());
    hasher.update(b":");
    hasher.update(config_key.as_bytes());
    hasher.update(b":");
    hasher.update(replicate.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

fn sample_trial_indices(seed: &str, total: usize, count: usize) -> Vec<usize> {
    let mut selected = std::collections::BTreeSet::new();
    let mut ordered = Vec::with_capacity(count);
    let mut counter = 0_u64;
    while ordered.len() < count {
        let index = trial_index_hash(seed, counter, total);
        if selected.insert(index) {
            ordered.push(index);
        }
        counter = counter.saturating_add(1);
    }
    ordered
}

fn trial_index_hash(seed: &str, counter: u64, total: usize) -> usize {
    let mut hasher = Sha256::new();
    hasher.update(seed.as_bytes());
    hasher.update(counter.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    (u128::from_be_bytes(bytes) % total as u128) as usize
}

fn trial_at_index(config: &SweepConfig, mut index: usize) -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();
    let parameters = config.parameters.iter().collect::<Vec<_>>();
    for (name, values) in parameters.into_iter().rev() {
        let value_index = index % values.len();
        index /= values.len();
        variables.insert(name.clone(), values[value_index].as_str().to_string());
    }
    variables
}

/// Returns the interpolation overlay for one trial, including reserved
/// `HPC_COMPOSE_SWEEP_*` variables.
#[must_use]
pub fn interpolation_vars_for_sweep_trial(
    sweep_id: &str,
    trial: &SweepExpansionTrial,
) -> BTreeMap<String, String> {
    let mut vars = trial.variables.clone();
    vars.insert("HPC_COMPOSE_SWEEP_ID".to_string(), sweep_id.to_string());
    vars.insert(
        "HPC_COMPOSE_SWEEP_TRIAL".to_string(),
        trial.trial_id.clone(),
    );
    vars.insert(
        "HPC_COMPOSE_SWEEP_TRIAL_INDEX".to_string(),
        trial.index.to_string(),
    );
    vars.insert(
        "HPC_COMPOSE_SWEEP_REPLICATE".to_string(),
        trial.replicate.to_string(),
    );
    if let Some(seed) = &trial.seed {
        vars.insert("HPC_COMPOSE_SWEEP_SEED".to_string(), seed.clone());
    }
    vars
}

/// Returns the sweep manifest path for a compose file and sweep id.
#[must_use]
pub fn sweep_manifest_path_for(spec_path: &Path, sweep_id: &str) -> PathBuf {
    tracked_paths::sweep_manifest_path_for(spec_path, sweep_id)
}

/// Returns the latest sweep manifest pointer path for a compose file.
#[must_use]
pub fn latest_sweep_manifest_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_sweep_manifest_path_for(spec_path)
}

/// Writes a sweep manifest and refreshes the latest sweep pointer.
pub fn write_sweep_manifest(manifest: &SweepManifest) -> Result<()> {
    let manifest_path = sweep_manifest_path_for(&manifest.compose_file, &manifest.sweep_id);
    write_json(&manifest_path, manifest)?;
    write_json(
        &latest_sweep_manifest_path_for(&manifest.compose_file),
        manifest,
    )
}

/// Loads one sweep manifest, defaulting to the latest sweep.
pub fn load_sweep_manifest(spec_path: &Path, sweep_id: Option<&str>) -> Result<SweepManifest> {
    let compose_file = absolute_path(spec_path)?;
    let path = match sweep_id {
        Some(sweep_id) => sweep_manifest_path_for(&compose_file, sweep_id),
        None => latest_sweep_manifest_path_for(&compose_file),
    };
    if !path.exists() {
        if let Some(sweep_id) = sweep_id {
            bail!(
                "no sweep metadata exists for sweep '{}' under {}",
                sweep_id,
                tracked_paths::sweeps_dir_for(&compose_file).display()
            );
        }
        bail!(
            "no sweep metadata exists for {}; run 'hpc-compose sweep submit' first",
            compose_file.display()
        );
    }
    let manifest: SweepManifest = read_json(&path)?;
    if manifest.schema_version > SWEEP_MANIFEST_SCHEMA_VERSION {
        bail!(
            "sweep manifest {} uses schema version {} but this version of hpc-compose only supports up to {}",
            path.display(),
            manifest.schema_version,
            SWEEP_MANIFEST_SCHEMA_VERSION
        );
    }
    Ok(manifest)
}

/// Scans persisted sweep manifests for one compose file.
pub fn scan_sweep_manifests(spec_path: &Path) -> Result<Vec<SweepManifest>> {
    let compose_file = absolute_path(spec_path)?;
    let sweeps_dir = tracked_paths::sweeps_dir_for(&compose_file);
    if !sweeps_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut manifests = Vec::new();
    for entry in
        fs::read_dir(&sweeps_dir).context(format!("failed to read {}", sweeps_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let manifest_path = path.join(tracked_paths::SWEEP_MANIFEST_FILE_NAME);
        if !manifest_path.exists() {
            continue;
        }
        if let Ok(manifest) = read_json::<SweepManifest>(&manifest_path) {
            manifests.push(manifest);
        }
    }
    manifests.sort_by(|left, right| {
        right
            .submitted_at
            .cmp(&left.submitted_at)
            .then_with(|| left.sweep_id.cmp(&right.sweep_id))
    });
    Ok(manifests)
}

/// Detects whether a sweep's compose block changed since it was first submitted.
///
/// `sweep submit --resume` re-expands the current compose file using the
/// manifest's stored sweep id (so `matrix: random` samples and per-replicate
/// seeds reproduce) and calls this before resubmitting anything. It returns
/// `None` when the re-expansion is identical to the persisted manifest along
/// every identifying axis (matrix mode, parameter combination count, trial
/// count, and each trial's id, variables, config key, replicate index, and
/// seed), or `Some(reason)` naming the first mismatch. On drift, resume refuses
/// to continue rather than submit trials that no longer match the recorded plan.
#[must_use]
pub fn detect_sweep_drift(expansion: &SweepExpansion, manifest: &SweepManifest) -> Option<String> {
    if expansion.matrix != manifest.matrix {
        return Some(format!(
            "matrix mode changed from '{}' to '{}'",
            manifest.matrix, expansion.matrix
        ));
    }
    if expansion.total_combinations != manifest.total_combinations {
        return Some(format!(
            "parameter combination count changed from {} to {}",
            manifest.total_combinations, expansion.total_combinations
        ));
    }
    if expansion.trials.len() != manifest.trials.len() {
        return Some(format!(
            "trial count changed from {} to {}",
            manifest.trials.len(),
            expansion.trials.len()
        ));
    }
    for (fresh, persisted) in expansion.trials.iter().zip(&manifest.trials) {
        if fresh.trial_id != persisted.trial_id {
            return Some(format!(
                "trial id at index {} changed from '{}' to '{}'",
                persisted.index, persisted.trial_id, fresh.trial_id
            ));
        }
        if fresh.variables != persisted.variables {
            return Some(format!(
                "variables for trial '{}' changed",
                persisted.trial_id
            ));
        }
        if fresh.config_key != persisted.config_key {
            return Some(format!(
                "config key for trial '{}' changed",
                persisted.trial_id
            ));
        }
        if fresh.replicate != persisted.replicate {
            return Some(format!(
                "replicate index for trial '{}' changed",
                persisted.trial_id
            ));
        }
        if fresh.seed != persisted.seed {
            return Some(format!("seed for trial '{}' changed", persisted.trial_id));
        }
    }
    None
}

/// Selects the manifest trial positions that still need submission on resume.
///
/// Resume targets exactly the trials that never received a job: any trial with a
/// recorded `submit_error` or a missing `job_id`. Already-submitted trials
/// (those with a `job_id` and no error) are never returned. Positions are the
/// indexes into `manifest.trials`, in natural order, so resubmission preserves
/// the original submit order.
#[must_use]
pub fn resume_trial_positions(manifest: &SweepManifest) -> Vec<usize> {
    manifest
        .trials
        .iter()
        .enumerate()
        .filter(|(_, trial)| trial.submit_error.is_some() || trial.job_id.is_none())
        .map(|(position, _)| position)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{SweepMatrix, SweepParameterValue};

    fn sweep_config() -> SweepConfig {
        SweepConfig {
            parameters: BTreeMap::from([
                (
                    "batch_size".to_string(),
                    vec![
                        SweepParameterValue::from("32".to_string()),
                        SweepParameterValue::from("64".to_string()),
                    ],
                ),
                (
                    "lr".to_string(),
                    vec![
                        SweepParameterValue::from("0.001".to_string()),
                        SweepParameterValue::from("0.01".to_string()),
                    ],
                ),
            ]),
            matrix: SweepMatrix::Full,
            objective: None,
            replicates: 1,
        }
    }

    #[test]
    fn full_sweep_expansion_is_stable() {
        let expansion = expand_sweep(&sweep_config(), "sweep-test").expect("expand");
        assert_eq!(expansion.trials.len(), 4);
        assert_eq!(expansion.trials[0].trial_id, "t000");
        assert_eq!(
            expansion.trials[0].variables,
            BTreeMap::from([
                ("batch_size".to_string(), "32".to_string()),
                ("lr".to_string(), "0.001".to_string()),
            ])
        );
        assert_eq!(expansion.trials[3].trial_id, "t003");
    }

    #[test]
    fn random_sweep_expansion_is_seeded_and_stable() {
        let mut config = sweep_config();
        config.matrix = SweepMatrix::Random {
            random: 2,
            seed: Some("seed".to_string()),
        };
        let first = expand_sweep(&config, "sweep-a").expect("first");
        let second = expand_sweep(&config, "sweep-b").expect("second");
        assert_eq!(first.trials, second.trials);
        assert_eq!(first.seed.as_deref(), Some("seed"));
    }

    #[test]
    fn random_sweep_expansion_uses_sweep_id_as_persisted_seed_when_omitted() {
        let mut config = sweep_config();
        config.matrix = SweepMatrix::Random {
            random: 2,
            seed: None,
        };

        let first = expand_sweep(&config, "sweep-a").expect("first");
        let second = expand_sweep(&config, "sweep-a").expect("second");
        let other = expand_sweep(&config, "sweep-b").expect("other");

        assert_eq!(first.seed.as_deref(), Some("sweep-a"));
        assert_eq!(other.seed.as_deref(), Some("sweep-b"));
        assert_eq!(first.trials, second.trials);
    }

    #[test]
    fn sweep_trial_interpolation_vars_include_reserved_values() {
        let expansion = expand_sweep(&sweep_config(), "sweep-test").expect("expand");
        let vars = interpolation_vars_for_sweep_trial("sweep-test", &expansion.trials[0]);
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_ID").map(String::as_str),
            Some("sweep-test")
        );
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_TRIAL").map(String::as_str),
            Some("t000")
        );
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_TRIAL_INDEX")
                .map(String::as_str),
            Some("0")
        );
        // replicates == 1: REPLICATE is 0, no SEED is injected (back-compat).
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_REPLICATE").map(String::as_str),
            Some("0")
        );
        assert_eq!(vars.get("HPC_COMPOSE_SWEEP_SEED"), None);
    }

    #[test]
    fn replicates_fan_out_each_config_with_stable_ids() {
        let mut config = sweep_config();
        config.replicates = 3;
        let expansion = expand_sweep(&config, "sweep-test").expect("expand");
        // 4 configs x 3 replicates = 12 runs.
        assert_eq!(expansion.trials.len(), 12);
        // First config fans out to t000r0..t000r2.
        assert_eq!(expansion.trials[0].trial_id, "t000r0");
        assert_eq!(expansion.trials[1].trial_id, "t000r1");
        assert_eq!(expansion.trials[2].trial_id, "t000r2");
        // Second config starts at t001r0.
        assert_eq!(expansion.trials[3].trial_id, "t001r0");
        assert_eq!(expansion.trials[11].trial_id, "t003r2");
        // Global index is contiguous across the fan-out.
        for (expected, trial) in expansion.trials.iter().enumerate() {
            assert_eq!(trial.index, expected);
        }
        // All replicates of one config share a config_key and variables.
        assert_eq!(
            expansion.trials[0].config_key,
            expansion.trials[1].config_key
        );
        assert_eq!(
            expansion.trials[0].config_key,
            expansion.trials[2].config_key
        );
        assert_eq!(expansion.trials[0].variables, expansion.trials[2].variables);
        assert_ne!(
            expansion.trials[0].config_key,
            expansion.trials[3].config_key
        );
        assert_eq!(expansion.trials[0].replicate, 0);
        assert_eq!(expansion.trials[2].replicate, 2);
    }

    #[test]
    fn replicates_one_is_byte_identical_to_legacy_expansion() {
        let mut config = sweep_config();
        config.replicates = 1;
        let expansion = expand_sweep(&config, "sweep-test").expect("expand");
        // Legacy ids and no per-replicate seed are preserved.
        assert_eq!(expansion.trials.len(), 4);
        assert_eq!(expansion.trials[0].trial_id, "t000");
        assert_eq!(expansion.trials[3].trial_id, "t003");
        for trial in &expansion.trials {
            assert_eq!(trial.replicate, 0);
            assert_eq!(trial.seed, None);
        }
    }

    #[test]
    fn replicate_seeds_are_deterministic_and_distinct() {
        let mut config = sweep_config();
        config.replicates = 3;
        let first = expand_sweep(&config, "sweep-fixed").expect("first");
        let second = expand_sweep(&config, "sweep-fixed").expect("second");
        // Same sweep id -> identical config keys and seeds across re-expansion.
        assert_eq!(first.trials, second.trials);
        let seeds: std::collections::BTreeSet<_> = first
            .trials
            .iter()
            .map(|trial| {
                trial
                    .seed
                    .clone()
                    .expect("seed present when replicates > 1")
            })
            .collect();
        // 12 distinct seeds (per config x replicate).
        assert_eq!(seeds.len(), 12);
        // Seed format is a 64-char hex SHA-256 digest.
        assert!(seeds.iter().all(|seed| seed.len() == 64));
        // A different sweep id changes the seeds.
        let other = expand_sweep(&config, "sweep-other").expect("other");
        assert_ne!(first.trials[0].seed, other.trials[0].seed);
    }

    #[test]
    fn replicate_interpolation_vars_include_replicate_and_seed() {
        let mut config = sweep_config();
        config.replicates = 2;
        let expansion = expand_sweep(&config, "sweep-test").expect("expand");
        let vars = interpolation_vars_for_sweep_trial("sweep-test", &expansion.trials[1]);
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_TRIAL").map(String::as_str),
            Some("t000r1")
        );
        assert_eq!(
            vars.get("HPC_COMPOSE_SWEEP_REPLICATE").map(String::as_str),
            Some("1")
        );
        let seed = vars
            .get("HPC_COMPOSE_SWEEP_SEED")
            .expect("seed injected when replicates > 1");
        assert_eq!(seed.len(), 64);
        assert_eq!(Some(seed), expansion.trials[1].seed.as_ref());
    }

    #[test]
    fn v2_manifest_without_replicate_fields_still_loads() {
        // A minimal v2 manifest JSON (pre-#12): no config_key/replicate/seed on
        // trials, schema_version 2. It must deserialize under the v3 structs via
        // serde(default).
        let json = r#"{
            "schema_version": 2,
            "sweep_id": "sweep-legacy",
            "compose_file": "/tmp/compose.yaml",
            "submitted_at": 100,
            "matrix": "full",
            "total_combinations": 1,
            "trials": [
                {
                    "trial_id": "t000",
                    "index": 0,
                    "variables": {"lr": "0.1"},
                    "script_path": "/tmp/sweeps/sweep-legacy/t000.sbatch"
                }
            ]
        }"#;
        let manifest: SweepManifest = serde_json::from_str(json).expect("v2 manifest loads");
        assert_eq!(manifest.schema_version, 2);
        assert!(manifest.schema_version <= SWEEP_MANIFEST_SCHEMA_VERSION);
        let trial = &manifest.trials[0];
        assert_eq!(trial.config_key, "");
        assert_eq!(trial.replicate, 0);
        assert_eq!(trial.seed, None);
    }

    /// Builds a fresh-off-submit manifest from an expansion: every trial is
    /// present with no job id, mirroring `sweep_submit`'s initial persist.
    fn manifest_from_expansion(expansion: &SweepExpansion) -> SweepManifest {
        SweepManifest {
            schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
            sweep_id: expansion.sweep_id.clone(),
            compose_file: PathBuf::from("/tmp/compose.yaml"),
            submitted_at: 0,
            matrix: expansion.matrix.clone(),
            compose_file_sha256: None,
            seed: expansion.seed.clone(),
            total_combinations: expansion.total_combinations,
            objective: None,
            best_trial: None,
            stopped_at: None,
            stop_reason: None,
            trials: expansion
                .trials
                .iter()
                .map(|trial| SweepManifestTrial {
                    trial_id: trial.trial_id.clone(),
                    index: trial.index,
                    variables: trial.variables.clone(),
                    config_key: trial.config_key.clone(),
                    replicate: trial.replicate,
                    seed: trial.seed.clone(),
                    script_path: PathBuf::from(format!("{}.sbatch", trial.trial_id)),
                    job_id: None,
                    record_path: None,
                    submitted_at: None,
                    submit_error: None,
                    objective: None,
                    objective_error: None,
                    observed_at: None,
                })
                .collect(),
        }
    }

    fn lr_config(values: &[&str]) -> SweepConfig {
        let mut config = sweep_config();
        config.parameters.insert(
            "lr".to_string(),
            values
                .iter()
                .map(|value| SweepParameterValue::from((*value).to_string()))
                .collect(),
        );
        config
    }

    #[test]
    fn detect_sweep_drift_accepts_identical_reexpansion() {
        let config = sweep_config();
        let expansion = expand_sweep(&config, "sweep-x").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        // Re-expanding the same config with the stored sweep id reproduces it.
        let reexpanded = expand_sweep(&config, &manifest.sweep_id).expect("re-expand");
        assert!(detect_sweep_drift(&reexpanded, &manifest).is_none());
    }

    #[test]
    fn detect_sweep_drift_accepts_identical_random_reexpansion() {
        let mut config = sweep_config();
        config.matrix = SweepMatrix::Random {
            random: 2,
            seed: None,
        };
        // With no explicit seed the sweep id is the seed, so re-expansion with the
        // stored id must reproduce the same sampled trials.
        let expansion = expand_sweep(&config, "sweep-rand").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        let reexpanded = expand_sweep(&config, &manifest.sweep_id).expect("re-expand");
        assert!(detect_sweep_drift(&reexpanded, &manifest).is_none());
    }

    #[test]
    fn detect_sweep_drift_flags_changed_variable_values() {
        let expansion = expand_sweep(&lr_config(&["0.001", "0.01"]), "sweep-x").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        // Same trial count and combination count, but one lr value changed.
        let reexpanded =
            expand_sweep(&lr_config(&["0.001", "0.5"]), &manifest.sweep_id).expect("re-expand");
        let reason = detect_sweep_drift(&reexpanded, &manifest).expect("variable drift");
        assert!(reason.contains("variables"), "unexpected: {reason}");
    }

    #[test]
    fn detect_sweep_drift_flags_changed_combination_count() {
        let expansion = expand_sweep(&lr_config(&["0.001", "0.01"]), "sweep-x").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        let reexpanded = expand_sweep(&lr_config(&["0.001", "0.01", "0.1"]), &manifest.sweep_id)
            .expect("re-expand");
        let reason = detect_sweep_drift(&reexpanded, &manifest).expect("count drift");
        assert!(reason.contains("combination count"), "unexpected: {reason}");
    }

    #[test]
    fn detect_sweep_drift_flags_changed_trial_count_via_replicates() {
        // Replicates leave the combination count unchanged but multiply the trial
        // count, exercising the trial-count branch specifically.
        let expansion = expand_sweep(&sweep_config(), "sweep-x").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        let mut replicated = sweep_config();
        replicated.replicates = 2;
        let reexpanded = expand_sweep(&replicated, &manifest.sweep_id).expect("re-expand");
        let reason = detect_sweep_drift(&reexpanded, &manifest).expect("trial count drift");
        assert!(reason.contains("trial count"), "unexpected: {reason}");
    }

    #[test]
    fn detect_sweep_drift_flags_changed_matrix_mode() {
        let expansion = expand_sweep(&sweep_config(), "sweep-x").expect("expand");
        let manifest = manifest_from_expansion(&expansion);
        let mut random = sweep_config();
        random.matrix = SweepMatrix::Random {
            random: 2,
            seed: Some("s".into()),
        };
        let reexpanded = expand_sweep(&random, &manifest.sweep_id).expect("re-expand");
        let reason = detect_sweep_drift(&reexpanded, &manifest).expect("matrix drift");
        assert!(reason.contains("matrix mode"), "unexpected: {reason}");
    }

    #[test]
    fn detect_sweep_drift_flags_changed_seed() {
        let expansion = expand_sweep(&sweep_config(), "sweep-x").expect("expand");
        let mut manifest = manifest_from_expansion(&expansion);
        // A stale persisted per-replicate seed no longer matches the re-expansion.
        manifest.trials[0].seed = Some("stale-seed".to_string());
        let reexpanded = expand_sweep(&sweep_config(), &manifest.sweep_id).expect("re-expand");
        let reason = detect_sweep_drift(&reexpanded, &manifest).expect("seed drift");
        assert!(reason.contains("seed"), "unexpected: {reason}");
    }

    #[test]
    fn compose_file_sha256_matches_known_vector() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("compose.yaml");
        // Canonical SHA-256 test vector: sha256("abc").
        fs::write(&path, b"abc").expect("write");
        assert_eq!(
            compose_file_sha256(&path).expect("hash"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn compose_file_sha256_errors_on_missing_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let missing = dir.path().join("does-not-exist.yaml");
        assert!(compose_file_sha256(&missing).is_err());
    }

    #[test]
    fn resume_trial_positions_selects_failed_and_unattempted() {
        let expansion = expand_sweep(&sweep_config(), "sweep-x").expect("expand");
        let mut manifest = manifest_from_expansion(&expansion);
        // t000 submitted, t001 submit_failed, t002 unattempted, t003 submitted.
        manifest.trials[0].job_id = Some("100".into());
        manifest.trials[1].submit_error = Some("boom".into());
        // t002 left untouched (no job id, no error).
        manifest.trials[3].job_id = Some("103".into());
        assert_eq!(resume_trial_positions(&manifest), vec![1, 2]);
    }

    #[test]
    fn resume_trial_positions_empty_when_all_submitted() {
        let expansion = expand_sweep(&sweep_config(), "sweep-x").expect("expand");
        let mut manifest = manifest_from_expansion(&expansion);
        for (position, trial) in manifest.trials.iter_mut().enumerate() {
            trial.job_id = Some(format!("10{position}"));
        }
        assert!(resume_trial_positions(&manifest).is_empty());
    }
}
