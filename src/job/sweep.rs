use super::*;
use crate::spec::{SweepConfig, SweepMatrix};

/// Schema version for persisted sweep manifests.
pub const SWEEP_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// One generated sweep trial before submission.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SweepExpansionTrial {
    pub trial_id: String,
    pub index: usize,
    pub variables: BTreeMap<String, String>,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SweepManifest {
    pub schema_version: u32,
    pub sweep_id: String,
    pub compose_file: PathBuf,
    pub submitted_at: u64,
    pub matrix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
    pub total_combinations: usize,
    pub trials: Vec<SweepManifestTrial>,
}

/// Persisted metadata for one sweep trial.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SweepManifestTrial {
    pub trial_id: String,
    pub index: usize,
    pub variables: BTreeMap<String, String>,
    pub script_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_error: Option<String>,
}

/// Builds a collision-resistant, human-readable sweep id.
#[must_use]
pub fn generate_sweep_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("sweep-{millis}-{}", std::process::id())
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
    match &config.matrix {
        SweepMatrix::Full
            if max_trials.is_some_and(|max_trials| total_combinations > max_trials) =>
        {
            let max_trials = max_trials.expect("checked above");
            bail!(
                "sweep expands to {total_combinations} trials, above the limit of {max_trials}; rerun with --max-trials {total_combinations} or larger to submit intentionally"
            );
        }
        SweepMatrix::Full => Ok(SweepExpansion {
            sweep_id: sweep_id.to_string(),
            matrix: "full".to_string(),
            seed: None,
            total_combinations,
            trials: assign_trial_ids(full_product_trials(config, sweep_id)),
        }),
        SweepMatrix::Random { random, seed } => {
            if *random > total_combinations {
                bail!(
                    "sweep.matrix.random requests {random} trials but only {total_combinations} combinations exist"
                );
            }
            if let Some(max_trials) = max_trials
                && *random > max_trials
            {
                bail!(
                    "sweep expands to {random} sampled trials, above the limit of {max_trials}; rerun with --max-trials {random} or larger to submit intentionally"
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
                trials: assign_trial_ids(sampled),
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

fn assign_trial_ids(trials: Vec<BTreeMap<String, String>>) -> Vec<SweepExpansionTrial> {
    trials
        .into_iter()
        .enumerate()
        .map(|(index, variables)| SweepExpansionTrial {
            trial_id: format!("t{index:03}"),
            index,
            variables,
        })
        .collect()
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
    }
}
