use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;

use super::interpolate::interpolation_vars;
use super::parse::{load_raw_spec, load_raw_spec_from_str};
use super::{ComposeSpec, SecretSpec, SweepConfig, interpolate_optional_string};
use crate::spec_error::{SpecError, SpecValidationError};

pub(crate) fn mark_spec_validation_error(error: anyhow::Error) -> anyhow::Error {
    if error.downcast_ref::<SpecError>().is_some()
        || error.downcast_ref::<SpecValidationError>().is_some()
    {
        error
    } else {
        SpecValidationError::new(error).into()
    }
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
        (|| {
            let spec = load_raw_spec(path)?;
            if let Some(sweep) = &spec.sweep {
                sweep.validate()?;
            }
            Ok(spec.sweep)
        })()
        .map_err(mark_spec_validation_error)
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
        (|| {
            let spec = load_raw_spec(path)?;
            for (name, secret) in &spec.secrets {
                secret.validate(name)?;
            }
            Ok(spec.secrets)
        })()
        .map_err(mark_spec_validation_error)
    }

    /// Loads only the top-level `secrets:` block from an in-memory root compose
    /// document, without applying interpolation.
    ///
    /// `extends` targets still resolve relative to `path` and are read from
    /// disk; only the root document is overlaid by `raw`.
    ///
    /// # Errors
    ///
    /// Returns an error when the document cannot be parsed or a secret declares
    /// both or neither of `file`/`env`.
    pub fn load_secrets_from_str(path: &Path, raw: &str) -> Result<BTreeMap<String, SecretSpec>> {
        (|| {
            let spec = load_raw_spec_from_str(path, raw)?;
            for (name, secret) in &spec.secrets {
                secret.validate(name)?;
            }
            Ok(spec.secrets)
        })()
        .map_err(mark_spec_validation_error)
    }

    /// Loads, interpolates, and validates a compose file from disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, the YAML cannot be
    /// parsed, interpolation fails, or semantic validation rejects the spec.
    pub fn load(path: &Path) -> Result<Self> {
        (|| {
            let vars = interpolation_vars(path)?;
            Self::load_with_interpolation_vars(path, &vars)
        })()
        .map_err(mark_spec_validation_error)
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
        (|| {
            let spec = load_raw_spec(path)?;
            Self::finish_load_with_interpolation_vars(path, spec, vars)
        })()
        .map_err(mark_spec_validation_error)
    }

    /// Loads, interpolates, and validates an in-memory root compose document
    /// using explicit interpolation variables.
    ///
    /// `extends`, `.env`, `env_file`, and secret file references stay resolved
    /// relative to `path`; only the root YAML document comes from `raw`.
    ///
    /// # Errors
    ///
    /// Returns an error when the document cannot be parsed, interpolation fails,
    /// or semantic validation rejects the spec.
    pub fn load_with_interpolation_vars_from_str(
        path: &Path,
        raw: &str,
        vars: &BTreeMap<String, String>,
    ) -> Result<Self> {
        (|| {
            let spec = load_raw_spec_from_str(path, raw)?;
            Self::finish_load_with_interpolation_vars(path, spec, vars)
        })()
        .map_err(mark_spec_validation_error)
    }

    /// Fuzz-only parser seam that exercises YAML decode, root validation,
    /// normalization, DTO deserialization, and semantic validation without
    /// resolving external files.
    ///
    /// # Errors
    ///
    /// Returns the same parsing or validation errors as the exercised in-memory
    /// stages. This function is only compiled with the non-default `fuzzing`
    /// feature.
    #[cfg(feature = "fuzzing")]
    pub fn load_fuzz_root_from_str(raw: &str) -> Result<Self> {
        (|| {
            let mut spec = super::parse::load_fuzz_raw_spec_from_str(raw)?;
            spec.validate()?;
            Ok(spec)
        })()
        .map_err(mark_spec_validation_error)
    }

    fn finish_load_with_interpolation_vars(
        path: &Path,
        mut spec: Self,
        vars: &BTreeMap<String, String>,
    ) -> Result<Self> {
        spec.interpolate_with_vars(vars)?;
        // Fold each service's `env_file:` into its `environment` before
        // validation so the merged keys are name-checked and the planner,
        // redaction, and `config` all see a single environment. Done only on
        // the load path (like interpolation), not in `validate()`/the planner,
        // which must stay path-free and idempotent.
        let project_dir = path.parent().unwrap_or_else(|| Path::new("."));
        for (name, service) in &mut spec.services {
            service.resolve_env_file(name, project_dir)?;
        }
        spec.validate()?;
        Ok(spec)
    }

    pub(super) fn interpolate_with_vars(&mut self, vars: &BTreeMap<String, String>) -> Result<()> {
        interpolate_optional_string(&mut self.name, vars)?;
        self.software_env.interpolate(vars)?;
        self.slurm.interpolate(vars)?;
        for service in self.services.values_mut() {
            service.interpolate(vars)?;
        }
        Ok(())
    }
}
