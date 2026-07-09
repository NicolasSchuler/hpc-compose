use std::collections::BTreeMap;
use std::fmt;

use anyhow::{Context, Result, bail};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, de};

/// Embedded hyperparameter sweep metadata.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepConfig {
    pub parameters: BTreeMap<String, Vec<SweepParameterValue>>,
    pub matrix: SweepMatrix,
    /// Optional objective used by `sweep observe` to rank trials and by
    /// `sweep stop --stop-when` to trigger early termination.
    #[serde(default)]
    pub objective: Option<SweepObjective>,
    /// Number of seeded replicate trials submitted per parameter config.
    ///
    /// Defaults to `1` (no fan-out, byte-identical to a non-replicated sweep).
    /// When `> 1`, each parameter combination is expanded into this many
    /// trials with a deterministic per-replicate seed, and `sweep
    /// status`/`observe` roll up mean±std(n) per config group.
    #[serde(default = "default_sweep_replicates")]
    pub replicates: u32,
}

/// Optimization direction for a sweep objective.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, schemars::JsonSchema)]
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
    /// Name of a sweep parameter (e.g. `nodes`, `model_size`) to use as the
    /// x-axis for the post-hoc scaling report printed by `sweep observe
    /// --scaling`. Must name a key under `sweep.parameters` whose values parse
    /// as `f64`. Purely a report/CLI knob: it is never consumed during
    /// submission or rendering and the scaling report itself is never persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_axis: Option<String>,
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

fn default_sweep_replicates() -> u32 {
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

    /// Returns the number of materialized runs once replicates are fanned out.
    ///
    /// This is `total_trials() * replicates` and is the count the `--max-trials`
    /// guard applies to. `total_trials()` itself stays combinations-only because
    /// `matrix.random` is bounded against the number of combinations.
    ///
    /// # Errors
    ///
    /// Returns an error if the product would overflow `usize`.
    pub fn total_runs(&self) -> Result<usize> {
        self.total_trials()?
            .checked_mul(self.replicates as usize)
            .context("sweep run matrix is too large")
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.parameters.is_empty() {
            bail!("sweep.parameters must contain at least one parameter");
        }
        if self.replicates == 0 {
            bail!("sweep.replicates must be at least 1");
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
            if let Some(axis) = &objective.scaling_axis {
                let Some(values) = self.parameters.get(axis) else {
                    bail!(
                        "sweep.objective.scaling_axis '{axis}' must name a sweep parameter, but no such key exists under sweep.parameters"
                    );
                };
                for value in values {
                    match value.as_str().parse::<f64>() {
                        Ok(parsed) if parsed.is_finite() && parsed > 0.0 => {}
                        _ => bail!(
                            "sweep.objective.scaling_axis '{axis}' requires positive, finite numeric values, but '{}' is not",
                            value.as_str()
                        ),
                    }
                }
            }
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
#[derive(Debug, Clone, Serialize, PartialEq, Eq, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Serialize, PartialEq, Eq, schemars::JsonSchema)]
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
