use std::collections::BTreeSet;

use anyhow::{Context, Result};

use crate::spec::EffectiveComposeConfig;

/// Serializes the effective config as YAML for the persisted job-state
/// snapshot (and `diff` comparisons), redacting resolved secret values first.
///
/// The snapshot is written to `.hpc-compose/` on a shared filesystem, so it
/// must not carry cleartext secrets. Pass the secret value set from
/// [`crate::context::ResolvedContext::secret_values`] (declared `secrets:`
/// values) so values referenced under benign env names are caught in addition
/// to name-based redaction.
pub(crate) fn effective_config_snapshot_yaml(
    config: &EffectiveComposeConfig,
    secret_values: &BTreeSet<String>,
) -> Result<String> {
    let value = crate::redaction::redacted_yaml_value(config, secret_values, false)
        .context("failed to redact effective config for snapshot")?;
    serde_norway::to_string(&value).context("failed to serialize effective config as yaml")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;

    use super::*;
    use crate::spec::ComposeSpec;

    #[test]
    fn effective_config_snapshot_persists_exact_redacted_yaml_bytes() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            r#"name: snapshot-demo
x-slurm:
  cache_dir: /shared/hpc-compose-cache
services:
  app:
    image: alpine:3.20
    command: echo prefix-literal-secret-suffix
    environment:
      API_TOKEN: name-sensitive
      MIRROR: literal-secret
      SAFE: visible
"#,
        )
        .expect("write compose fixture");

        let mut spec = ComposeSpec::load(&compose).expect("load compose fixture");
        let plan = crate::planner::build_plan(&compose, spec.clone()).expect("build plan");
        spec.slurm = plan.slurm.clone();
        let normalized_policies = plan
            .ordered_services
            .iter()
            .map(|service| (service.name.clone(), service.failure_policy.clone()))
            .collect::<BTreeMap<_, _>>();
        let effective = spec
            .effective_config(&plan.cache_dir, &normalized_policies)
            .expect("effective config");
        let secret_values = BTreeSet::from(["literal-secret".to_string()]);
        let snapshot =
            effective_config_snapshot_yaml(&effective, &secret_values).expect("snapshot yaml");

        let expected = r#"name: snapshot-demo
runtime:
  backend: pyxis
  gpu: auto
x-slurm:
  cache_dir: /shared/hpc-compose-cache
services:
  app:
    image: alpine:3.20
    command: echo prefix-<redacted>-suffix
    environment:
      API_TOKEN: <redacted>
      MIRROR: <redacted>
      SAFE: visible
    x-slurm:
      failure_policy:
        mode: fail_job
"#;
        assert_eq!(snapshot.as_bytes(), expected.as_bytes());

        let runtime_plan = crate::runtime_plan::build_runtime_plan(&plan);
        let script_path = tmpdir.path().join("job.sbatch");
        fs::write(&script_path, "#!/bin/sh\n").expect("write script");
        let options = crate::job::SubmissionRecordBuildOptions {
            config_snapshot_yaml: Some(snapshot.clone()),
            ..crate::job::SubmissionRecordBuildOptions::default()
        };
        let record = crate::job::build_submission_record_with_options(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "12345",
            &options,
        )
        .expect("build record");
        crate::job::write_submission_record(&record).expect("persist record");
        let persisted = crate::job::load_submission_record(&compose, Some("12345"))
            .expect("reload persisted record");
        assert_eq!(
            persisted.config_snapshot_yaml.as_deref().map(str::as_bytes),
            Some(expected.as_bytes())
        );
    }
}
