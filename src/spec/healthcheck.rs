use anyhow::{Context, Result, bail};

use super::validation::{
    parse_duration_seconds, parse_healthcheck_argv, parse_http_probe, parse_nc_probe,
};
use super::{
    HealthcheckDuration, HealthcheckSpec, HealthcheckTest, InterpolationVars, ReadinessSpec,
    ServiceSpec, interpolate_string, interpolate_vec_strings,
};
use crate::spec_error::SpecError;

impl ServiceSpec {
    pub(super) fn normalize_healthcheck(&mut self, name: &str) -> Result<()> {
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

impl HealthcheckSpec {
    pub(super) fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        if let Some(test) = &mut self.test {
            test.interpolate(vars)?;
        }
        Ok(())
    }
}

impl HealthcheckTest {
    pub(super) fn interpolate(&mut self, vars: &InterpolationVars) -> Result<()> {
        match self {
            HealthcheckTest::Vec(items) => interpolate_vec_strings(items, vars),
            HealthcheckTest::String(command) => {
                *command = interpolate_string(command, vars)?;
                Ok(())
            }
        }
    }

    pub(super) fn to_readiness(&self, timeout_seconds: Option<u64>) -> Result<ReadinessSpec> {
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
    pub(super) fn to_seconds(&self) -> Result<u64> {
        match self {
            HealthcheckDuration::Seconds(seconds) => Ok(*seconds),
            HealthcheckDuration::String(raw) => parse_duration_seconds(raw),
        }
    }
}
