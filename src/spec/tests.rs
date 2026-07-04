use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;

use miette::Diagnostic;
use proptest::prelude::*;
use proptest::string::string_regex;

use super::*;
use crate::test_support::env_lock;

fn write_spec(tmpdir: &Path, body: &str) -> std::path::PathBuf {
    let path = tmpdir.join("compose.yaml");
    fs::write(&path, body).expect("write compose");
    path
}

#[test]
fn missing_top_level_spec_reports_not_found_with_scaffolding_hint() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing = tmpdir.path().join("compose.yaml");
    let err = ComposeSpec::load(&missing).expect_err("missing spec");
    assert!(format!("{err:#}").contains("not found"), "{err:#}");
    // The diagnostic help steers first-run users to `new`/`evolve`, not the
    // schema/validate hint used for a present-but-invalid file.
    let help = crate::cli_error_report(err)
        .help()
        .map(|help| help.to_string())
        .unwrap_or_default();
    assert!(
        help.contains("hpc-compose new") && help.contains("evolve"),
        "help should point at scaffolding commands, got: {help}"
    );
}

/// Returns the `pub <name>:` field names declared in the struct that opens with
/// `marker`, stopping at the struct's closing brace. Used by the
/// `effective_config` drift guard below.
fn struct_field_names(source: &str, marker: &str) -> Vec<String> {
    let start = source
        .find(marker)
        .unwrap_or_else(|| panic!("struct marker not found: {marker}"));
    let body = &source[start + marker.len()..];
    let mut names = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed == "}" {
            break;
        }
        if let Some(rest) = trimmed.strip_prefix("pub ")
            && let Some(colon) = rest.find(':')
        {
            let name = &rest[..colon];
            if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                names.push(name.to_string());
            }
        }
    }
    names
}

/// Returns true when `body` reads `<prefix><field>` as a whole field access
/// (the following character is not a word character), so `gpus` does not match
/// inside `gpus_per_node`.
fn references_field(body: &str, prefix: &str, field: &str) -> bool {
    let needle = format!("{prefix}{field}");
    let mut from = 0;
    while let Some(idx) = body[from..].find(&needle) {
        let end = from + idx + needle.len();
        let boundary = body[end..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        if boundary {
            return true;
        }
        from = end;
    }
    false
}

#[test]
fn effective_config_maps_every_slurm_field_with_no_silent_drop() {
    // Drift guard. `effective_config` hand-copies SlurmConfig / ServiceSlurmConfig
    // into the Effective* mirror structs field by field. A new x-slurm field added
    // to the struct but forgotten in the mapping silently drops from `config` and
    // resume-diff output with no compiler help. This pins that every field is read
    // in the mapping, so adding one forces a conscious decision: map it, or list it
    // as intentionally excluded here.
    let source = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/spec/mod.rs"))
        .expect("source");
    let body = {
        let start = source
            .find("pub fn effective_config(")
            .expect("effective_config present");
        let rest = &source[start..];
        // The method body closes at the first 4-space-indented `}` (impl method).
        let end = rest.find("\n    }\n").map_or(rest.len(), |i| i + 6);
        // Collapse whitespace so multi-line method chains like
        // `self\n    .slurm\n    .after_job` match `self.slurm.after_job`.
        rest[..end].split_whitespace().collect::<String>()
    };

    // Excluded from the materialized snapshot on purpose:
    //  - software_env: #[serde(skip)] internal field, surfaced at the spec level.
    //  - cache_dir:    replaced by the resolved `cache_dir` parameter.
    //  - enroot_temp_dir: prepare-time scratch knob, not part of the run config.
    let slurm_exempt = ["software_env", "cache_dir", "enroot_temp_dir"];
    for field in struct_field_names(&source, "pub struct SlurmConfig {") {
        if slurm_exempt.contains(&field.as_str()) {
            continue;
        }
        assert!(
            references_field(&body, "self.slurm.", &field),
            "effective_config drops x-slurm.{field}: map it into EffectiveSlurmConfig, \
             or add it to slurm_exempt if it is intentionally excluded from `config`"
        );
    }

    //  - software_env: #[serde(skip)] internal field.
    //  - failure_policy: derived from the normalized failure policies, not the raw field.
    let service_exempt = ["software_env", "failure_policy"];
    for field in struct_field_names(&source, "pub struct ServiceSlurmConfig {") {
        if service_exempt.contains(&field.as_str()) {
            continue;
        }
        assert!(
            references_field(&body, "service.slurm.", &field),
            "effective_config drops per-service x-slurm.{field}: map it into \
             EffectiveServiceSlurmConfig, or add it to service_exempt if intentional"
        );
    }
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
fn effective_config_is_stable_across_input_service_ordering() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let first = write_spec(
        tmpdir.path(),
        r#"
name: demo
services:
  api:
    image: redis:7
    environment:
      LOG_LEVEL: info
  worker:
    image: python:3.11-slim
    depends_on:
      - api
    command: python -m worker
"#,
    );
    let second = tmpdir.path().join("compose-reordered.yaml");
    fs::write(
        &second,
        r#"
name: demo
services:
  worker:
    image: python:3.11-slim
    command: python -m worker
    depends_on:
      - api
  api:
    environment:
      LOG_LEVEL: info
    image: redis:7
"#,
    )
    .expect("write reordered");

    let first_config = ComposeSpec::load(&first)
        .expect("first load")
        .effective_config(&tmpdir.path().join("cache"), &BTreeMap::new())
        .expect("first effective config");
    let second_config = ComposeSpec::load(&second)
        .expect("second load")
        .effective_config(&tmpdir.path().join("cache"), &BTreeMap::new())
        .expect("second effective config");
    let first_json = serde_json::to_value(&first_config).expect("first json");
    let second_json = serde_json::to_value(&second_config).expect("second json");
    assert_eq!(first_json, second_json);
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
    assert!(err.to_string().contains("unsupported key 'build'"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.help()
            .is_some_and(|h| h.to_string().contains("x-runtime.prepare"))
    }));
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
    assert!(err.to_string().contains("unsupported key 'ports'"));
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
fn service_event_hooks_accept_restart_and_window_exhausted() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  trainer:
    image: trainer:latest
    command: python train.py
    x-slurm:
      hooks:
        - on: restart
          script: |
            echo "restart ${SLURM_JOB_ID}"
        - on: window_exhausted
          context: host
          script: |
            echo window
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load spec");
    let service = spec.services.get("trainer").expect("trainer");
    assert_eq!(service.slurm.hooks.len(), 2);
    assert_eq!(service.slurm.hooks[0].on, ServiceHookEvent::Restart);
    assert_eq!(service.slurm.hooks[0].context, ServiceHookContext::Host);
    assert!(service.slurm.hooks[0].script.contains("${SLURM_JOB_ID}"));
    assert_eq!(service.slurm.hooks[1].on, ServiceHookEvent::WindowExhausted);

    let effective = spec
        .effective_config(&tmpdir.path().join("cache"), &BTreeMap::new())
        .expect("effective config");
    let json = serde_json::to_value(effective).expect("effective json");
    assert_eq!(
        json["services"]["trainer"]["x-slurm"]["hooks"][0]["on"],
        "restart"
    );
    assert_eq!(
        json["services"]["trainer"]["x-slurm"]["hooks"][1]["context"],
        "host"
    );
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
fn service_event_hooks_reject_invalid_declarations() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let empty = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      hooks:
        - on: restart
          script: ""
"#,
    );
    let err = ComposeSpec::load(&empty).expect_err("empty event hook should fail");
    assert!(err.to_string().contains("x-slurm.hooks[0]"));
    assert!(err.to_string().contains("must not be empty"));

    let unknown = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      hooks:
        - on: first_failure
          script: echo first
"#,
    );
    let err = ComposeSpec::load(&unknown).expect_err("unknown event should fail");
    assert!(
        err.to_string().contains("failed to deserialize spec")
            || err.to_string().contains("unknown variant"),
        "unexpected error: {err}"
    );

    let container = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      hooks:
        - on: restart
          context: container
          script: echo restart
"#,
    );
    let err = ComposeSpec::load(&container).expect_err("container event hook should fail");
    assert!(err.to_string().contains("x-slurm.hooks[0].context"));
    assert!(err.to_string().contains("must be host"));
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
fn spec_version_accepts_v1_and_rejects_mismatches() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    for body in [
        "services:\n  app:\n    image: redis:7\n",
        "version: \"1\"\nservices:\n  app:\n    image: redis:7\n",
        "version: 1\nservices:\n  app:\n    image: redis:7\n",
    ] {
        let path = write_spec(tmpdir.path(), body);
        ComposeSpec::load(&path).unwrap_or_else(|err| panic!("v1 should load: {err:#}"));
    }

    let v2 = write_spec(
        tmpdir.path(),
        "version: \"2\"\nservices:\n  app:\n    image: redis:7\n",
    );
    let err = ComposeSpec::load(&v2).expect_err("v2 should fail");
    let message = err.to_string();
    assert!(message.contains("unsupported hpc-compose spec version '2'"));
    assert!(message.contains("steps was renamed to services in v2"));
    assert!(message.contains("docs/src/docker-compose-migration.md"));

    let compose_version = write_spec(
        tmpdir.path(),
        "version: \"3.9\"\nservices:\n  app:\n    image: redis:7\n",
    );
    let err = ComposeSpec::load(&compose_version).expect_err("compose version should fail");
    let message = err.to_string();
    assert!(message.contains("Docker Compose version"));
    assert!(message.contains("version: \"1\""));

    for (body, expected_kind) in [
        (
            "version: true\nservices:\n  app:\n    image: redis:7\n",
            "bool",
        ),
        (
            "version: [1]\nservices:\n  app:\n    image: redis:7\n",
            "sequence",
        ),
        (
            "version:\n  major: 1\nservices:\n  app:\n    image: redis:7\n",
            "mapping",
        ),
    ] {
        let path = write_spec(tmpdir.path(), body);
        let err = ComposeSpec::load(&path).expect_err("invalid version shape");
        let message = err.to_string();
        assert!(message.contains("top-level 'version' must be"));
        assert!(message.contains(expected_kind));
    }
}

#[test]
fn sweep_config_accepts_scalar_values_and_random_matrix() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
name: sweep-demo
sweep:
  parameters:
    lr: [0.001, 0.01]
    batch_size: [32, 64]
    use_amp: [true, false]
  matrix:
    random: 3
    seed: stable
services:
  trainer:
    image: python:3.11
    command: python train.py --lr ${lr:-0.001}
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load sweep spec");
    let sweep = spec.sweep.expect("sweep config");
    assert_eq!(sweep.total_trials().expect("total"), 8);
    let values = sweep.parameters.get("use_amp").expect("use_amp values");
    assert_eq!(values[0].as_str(), "true");

    let loaded = ComposeSpec::load_sweep(&path)
        .expect("load sweep only")
        .expect("sweep only config");
    assert_eq!(loaded.parameters.len(), 3);
}

#[test]
fn sweep_config_rejects_invalid_shapes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for (body, expected) in [
        (
            r#"
sweep:
  spec: train.yaml
  parameters:
    lr: [0.1]
  matrix: full
services:
  app:
    image: redis:7
"#,
            "sweep.spec is not supported",
        ),
        (
            r#"
sweep:
  typo: true
  parameters:
    lr: [0.1]
  matrix: full
services:
  app:
    image: redis:7
"#,
            "unknown field `typo`",
        ),
        (
            r#"
sweep:
  parameters: {}
  matrix: full
services:
  app:
    image: redis:7
"#,
            "must contain at least one parameter",
        ),
        (
            r#"
sweep:
  parameters:
    1bad: [0.1]
  matrix: full
services:
  app:
    image: redis:7
"#,
            "valid interpolation variable name",
        ),
        (
            r#"
sweep:
  parameters:
    HPC_COMPOSE_SWEEP_ID: [abc]
  matrix: full
services:
  app:
    image: redis:7
"#,
            "reserved HPC_COMPOSE_SWEEP_ prefix",
        ),
        (
            r#"
sweep:
  parameters:
    lr: []
  matrix: full
services:
  app:
    image: redis:7
"#,
            "must contain at least one value",
        ),
        (
            r#"
sweep:
  parameters:
    lr:
      - [0.1]
  matrix: full
services:
  app:
    image: redis:7
"#,
            "string, number, or boolean sweep value",
        ),
        (
            r#"
sweep:
  parameters:
    lr: [0.1]
  matrix:
    random: 0
services:
  app:
    image: redis:7
"#,
            "must be at least 1",
        ),
        (
            r#"
sweep:
  parameters:
    lr: [0.1]
  matrix:
    random: 2
services:
  app:
    image: redis:7
"#,
            "only 1 combinations exist",
        ),
        (
            r#"
sweep:
  parameters:
    lr: [0.1]
  matrix: full
  replicates: 0
services:
  app:
    image: redis:7
"#,
            "sweep.replicates must be at least 1",
        ),
        (
            r#"
sweep:
  parameters:
    lr: [0.1]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'loss=([0-9.]+)'
    scaling_axis: nodes
services:
  app:
    image: redis:7
"#,
            "scaling_axis 'nodes' must name a sweep parameter",
        ),
        (
            r#"
sweep:
  parameters:
    backend: [cpu, gpu]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'loss=([0-9.]+)'
    scaling_axis: backend
services:
  app:
    image: redis:7
"#,
            "scaling_axis 'backend' requires positive, finite numeric values",
        ),
        (
            r#"
sweep:
  parameters:
    nodes: [0, 4]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'loss=([0-9.]+)'
    scaling_axis: nodes
services:
  app:
    image: redis:7
"#,
            "scaling_axis 'nodes' requires positive, finite numeric values",
        ),
    ] {
        let path = write_spec(tmpdir.path(), body);
        let err = ComposeSpec::load(&path).expect_err("invalid sweep");
        assert!(
            format!("{err:#}").contains(expected),
            "expected '{expected}', got {err:#}"
        );
    }
}

#[test]
fn sweep_replicates_defaults_to_one_and_counts_runs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    // Omitted replicates defaults to 1; total_runs == total_trials.
    let path = write_spec(
        tmpdir.path(),
        r#"
name: sweep-no-replicates
sweep:
  parameters:
    lr: [0.001, 0.01]
    batch_size: [32, 64]
  matrix: full
services:
  app:
    image: redis:7
"#,
    );
    let sweep = ComposeSpec::load(&path)
        .expect("load")
        .sweep
        .expect("sweep config");
    assert_eq!(sweep.replicates, 1);
    assert_eq!(sweep.total_trials().expect("total"), 4);
    assert_eq!(sweep.total_runs().expect("runs"), 4);

    // replicates: 3 multiplies the run count but not the combination count.
    let path = write_spec(
        tmpdir.path(),
        r#"
name: sweep-replicates
sweep:
  parameters:
    lr: [0.001, 0.01]
    batch_size: [32, 64]
  matrix: full
  replicates: 3
services:
  app:
    image: redis:7
"#,
    );
    let sweep = ComposeSpec::load(&path)
        .expect("load")
        .sweep
        .expect("sweep config");
    assert_eq!(sweep.replicates, 3);
    assert_eq!(sweep.total_trials().expect("total"), 4);
    assert_eq!(sweep.total_runs().expect("runs"), 12);
}

#[test]
fn sweep_objective_scaling_axis_validates_and_round_trips() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    // A scaling_axis naming a real numeric parameter validates and is preserved.
    let path = write_spec(
        tmpdir.path(),
        r#"
name: sweep-scaling
sweep:
  parameters:
    nodes: [1, 2, 4]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'loss=([0-9.]+)'
    scaling_axis: nodes
services:
  app:
    image: redis:7
"#,
    );
    let sweep = ComposeSpec::load(&path)
        .expect("load scaling sweep")
        .sweep
        .expect("sweep config");
    let objective = sweep.objective.expect("objective");
    assert_eq!(objective.scaling_axis.as_deref(), Some("nodes"));
    // The additive field round-trips through serde.
    let json = serde_json::to_string(&objective).expect("serialize objective");
    assert!(json.contains("\"scaling_axis\":\"nodes\""));

    // An objective without scaling_axis still validates and omits the field.
    let path = write_spec(
        tmpdir.path(),
        r#"
name: sweep-no-scaling
sweep:
  parameters:
    nodes: [1, 2]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'loss=([0-9.]+)'
services:
  app:
    image: redis:7
"#,
    );
    let sweep = ComposeSpec::load(&path)
        .expect("load no-scaling sweep")
        .sweep
        .expect("sweep config");
    let objective = sweep.objective.expect("objective");
    assert!(objective.scaling_axis.is_none());
    let json = serde_json::to_string(&objective).expect("serialize objective");
    assert!(!json.contains("scaling_axis"));
}

#[test]
fn environment_list_requires_key_value_pairs() {
    let env = EnvironmentSpec::List(vec!["GOOD=1".into(), "BROKEN".into()]);
    let err = env.to_pairs().expect_err("should fail");
    assert!(err.to_string().contains("KEY=VALUE"));
}

#[test]
fn service_environment_rejects_unsafe_variable_names() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    environment:
      BAD-NAME: value
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("should fail");
    assert!(
        err.to_string()
            .contains("service 'app' environment.BAD-NAME")
    );
}

#[test]
fn prepare_environment_rejects_unsafe_variable_names() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-runtime:
      prepare:
        env:
          BAD-NAME: value
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("should fail");
    assert!(
        err.to_string()
            .contains("service 'app' x-runtime.prepare.env.BAD-NAME")
    );
}

#[test]
fn legacy_enroot_prepare_environment_rejects_unsafe_variable_names() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-enroot:
      prepare:
        env:
          BAD-NAME: value
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("should fail");
    assert!(
        err.to_string()
            .contains("service 'app' x-enroot.prepare.env.BAD-NAME")
    );
}

#[test]
fn software_env_accepts_shorthand_object_and_effective_config_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let env_file = tmpdir.path().join(".env");
    fs::write(
        &env_file,
        "CUDA_MODULE=cuda/12.4\nSPACK_VIEW=/shared/spack/views/ml\n",
    )
    .expect("env");
    let path = write_spec(
        tmpdir.path(),
        r#"
x-env:
  modules:
    - ${CUDA_MODULE}
    - openmpi/5
  spack:
    view: ${SPACK_VIEW}
  env:
    HDF5_USE_FILE_LOCKING: "FALSE"
services:
  app:
    image: python:3.11-slim
    x-env:
      modules:
        purge: false
        load:
          - netcdf/4.9
      env:
        HDF5_USE_FILE_LOCKING: "TRUE"
        OMP_NUM_THREADS: "8"
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load spec");
    assert_eq!(
        spec.software_env.modules.load,
        vec!["cuda/12.4", "openmpi/5"]
    );
    assert_eq!(
        spec.software_env
            .spack
            .as_ref()
            .map(|spack| spack.view.as_str()),
        Some("/shared/spack/views/ml")
    );
    assert_eq!(spec.slurm.software_env, spec.software_env);
    let service = spec.services.get("app").expect("app service");
    assert_eq!(service.software_env.modules.load, vec!["netcdf/4.9"]);
    assert_eq!(service.slurm.software_env, service.software_env);

    let effective = spec
        .effective_config(&tmpdir.path().join("cache"), &BTreeMap::new())
        .expect("effective config");
    assert_eq!(
        effective
            .software_env
            .env
            .get("HDF5_USE_FILE_LOCKING")
            .map(String::as_str),
        Some("FALSE")
    );
    assert_eq!(
        effective
            .services
            .get("app")
            .and_then(|service| service.software_env.env.get("OMP_NUM_THREADS"))
            .map(String::as_str),
        Some("8")
    );
}

#[test]
fn steps_alias_script_modules_and_command_normalization_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
modules:
  - cuda/${CUDA_VERSION}
steps:
  single:
    image: redis:7
    command: echo ${TOKEN}
  multi:
    image: redis:7
    command: |
      echo ${TOKEN}
      python train.py
  multi_entry:
    image: redis:7
    entrypoint: bash -lc
    command: |
      echo ${TOKEN}
      python train.py
  list:
    image: redis:7
    command:
      - echo
      - ${TOKEN}
  scripted:
    image: redis:7
    script: |
      echo ${TOKEN}
      python train.py
    modules:
      - netcdf/${NETCDF_VERSION}
"#,
    );
    let vars = BTreeMap::from([
        ("CUDA_VERSION".to_string(), "12.4".to_string()),
        ("NETCDF_VERSION".to_string(), "4.9".to_string()),
        ("TOKEN".to_string(), "expanded".to_string()),
    ]);
    let spec = ComposeSpec::load_with_interpolation_vars(&path, &vars).expect("load spec");
    assert!(spec.services.contains_key("single"));
    assert_eq!(spec.software_env.modules.load, vec!["cuda/12.4"]);
    assert_eq!(
        spec.services
            .get("single")
            .and_then(|service| service.command.as_ref())
            .and_then(CommandSpec::as_string),
        Some("echo ${TOKEN}")
    );
    assert_eq!(
        spec.services
            .get("multi")
            .and_then(|service| service.command.as_ref())
            .and_then(CommandSpec::as_vec),
        Some(
            &[
                "/bin/sh".to_string(),
                "-lc".to_string(),
                "echo ${TOKEN}\npython train.py\n".to_string()
            ][..]
        )
    );
    let multi_entry = spec.services.get("multi_entry").expect("multi_entry");
    assert!(multi_entry.entrypoint.is_none());
    assert_eq!(
        multi_entry.command.as_ref().and_then(CommandSpec::as_vec),
        Some(
            &[
                "/bin/sh".to_string(),
                "-lc".to_string(),
                "bash -lc echo ${TOKEN}\npython train.py\n".to_string()
            ][..]
        )
    );
    assert_eq!(
        spec.services
            .get("list")
            .and_then(|service| service.command.as_ref())
            .and_then(CommandSpec::as_vec),
        Some(&["echo".to_string(), "expanded".to_string()][..])
    );
    let scripted = spec.services.get("scripted").expect("scripted");
    assert_eq!(
        scripted.command.as_ref().and_then(CommandSpec::as_vec),
        Some(
            &[
                "/bin/sh".to_string(),
                "-lc".to_string(),
                "echo ${TOKEN}\npython train.py\n".to_string()
            ][..]
        )
    );
    assert_eq!(scripted.software_env.modules.load, vec!["netcdf/4.9"]);
}

#[test]
fn root_extends_merges_before_validation_and_normalization() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("base.yaml"),
        r#"
name: base
x-slurm:
  mem: 8G
steps:
  app:
    image: redis:7
    environment:
      BASE: yes
    volumes:
      - /shared/base:/data
      - /shared/keep:/keep:ro
"#,
    )
    .expect("base");
    let child = write_spec(
        tmpdir.path(),
        r#"
extends: base.yaml
name: child
x-slurm:
  cpus_per_task: 4
services:
  app:
    command: echo child
    environment:
      CHILD: yes
    volumes:
      - /shared/child:/data
      - /tmp/logs:/logs
"#,
    );

    let spec = ComposeSpec::load(&child).expect("load extended spec");
    assert_eq!(spec.name.as_deref(), Some("child"));
    assert_eq!(spec.slurm.mem.as_deref(), Some("8G"));
    assert_eq!(spec.slurm.cpus_per_task, Some(4));
    let app = spec.services.get("app").expect("app");
    assert_eq!(app.image.as_deref(), Some("redis:7"));
    assert_eq!(
        app.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo child")
    );
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![
            ("BASE".to_string(), "yes".to_string()),
            ("CHILD".to_string(), "yes".to_string())
        ]
    );
    assert_eq!(
        app.volumes,
        vec![
            "/shared/child:/data".to_string(),
            "/shared/keep:/keep:ro".to_string(),
            "/tmp/logs:/logs".to_string()
        ]
    );

    let steps_child = write_spec(
        tmpdir.path(),
        r#"
extends: base.yaml
steps:
  app:
    command: echo steps-child
"#,
    );
    let spec = ComposeSpec::load(&steps_child).expect("load child steps alias");
    let app = spec.services.get("app").expect("app");
    assert_eq!(app.image.as_deref(), Some("redis:7"));
    assert_eq!(
        app.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo steps-child")
    );
}

#[test]
fn service_extends_supports_same_file_and_external_file_shorthand() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("service-base.yaml"),
        r#"
services:
  worker:
    image: redis:7
    command: echo external
    x-slurm:
      cpus_per_task: 2
"#,
    )
    .expect("external base");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  base:
    image: alpine:3
    environment:
      SHARED: "1"
  app:
    extends: base
    command: echo app
  worker:
    extends: service-base.yaml
    command: echo child
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load extended services");
    let app = spec.services.get("app").expect("app");
    assert_eq!(app.image.as_deref(), Some("alpine:3"));
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![("SHARED".to_string(), "1".to_string())]
    );
    assert_eq!(
        app.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo app")
    );
    let worker = spec.services.get("worker").expect("worker");
    assert_eq!(worker.image.as_deref(), Some("redis:7"));
    assert_eq!(
        worker.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo child")
    );
    assert_eq!(worker.slurm.cpus_per_task, Some(2));
}

#[test]
fn service_extends_mapping_can_select_external_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("base.yaml"),
        r#"
services:
  template:
    image: redis:7
    command: echo template
"#,
    )
    .expect("base");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    extends:
      file: base.yaml
      service: template
    command: echo app
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load mapping extends");
    let app = spec.services.get("app").expect("app");
    assert_eq!(app.image.as_deref(), Some("redis:7"));
    assert_eq!(
        app.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo app")
    );
}

#[test]
fn recursive_extends_and_cycle_errors_are_reported() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("base.yaml"),
        r#"
services:
  app:
    image: redis:7
"#,
    )
    .expect("base");
    fs::write(
        tmpdir.path().join("mid.yaml"),
        r#"
extends: base.yaml
services:
  app:
    command: echo mid
"#,
    )
    .expect("mid");
    let child = write_spec(
        tmpdir.path(),
        r#"
extends: mid.yaml
services:
  app:
    environment:
      CHILD: yes
"#,
    );
    let spec = ComposeSpec::load(&child).expect("recursive extends");
    let app = spec.services.get("app").expect("app");
    assert_eq!(app.image.as_deref(), Some("redis:7"));
    assert_eq!(
        app.command.as_ref().and_then(CommandSpec::as_string),
        Some("echo mid")
    );

    fs::write(
        tmpdir.path().join("cycle-a.yaml"),
        "extends: cycle-b.yaml\nservices:\n  app:\n    image: redis:7\n",
    )
    .expect("cycle a");
    fs::write(
        tmpdir.path().join("cycle-b.yaml"),
        "extends: cycle-a.yaml\nservices:\n  app:\n    image: redis:7\n",
    )
    .expect("cycle b");
    let err = ComposeSpec::load(&tmpdir.path().join("cycle-a.yaml")).expect_err("cycle");
    assert!(format!("{err:#}").contains("extends cycle"));

    let service_cycle = write_spec(
        tmpdir.path(),
        r#"
services:
  a:
    extends: b
  b:
    extends: a
"#,
    );
    let err = ComposeSpec::load(&service_cycle).expect_err("service cycle");
    assert!(err.to_string().contains("service extends cycle"));
}

#[test]
fn extends_reports_missing_files_and_services() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing_file = write_spec(
        tmpdir.path(),
        r#"
extends: missing.yaml
services:
  app:
    image: redis:7
"#,
    );
    let err = ComposeSpec::load(&missing_file).expect_err("missing file");
    assert!(format!("{err:#}").contains("failed to load"));

    fs::write(
        tmpdir.path().join("base.yaml"),
        r#"
services:
  other:
    image: redis:7
"#,
    )
    .expect("base");
    let missing_service = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    extends:
      file: base.yaml
      service: template
"#,
    );
    let err = ComposeSpec::load(&missing_service).expect_err("missing service");
    assert!(err.to_string().contains("was not found"));
}

#[test]
fn new_foundation_alias_conflicts_are_rejected() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let both_services = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
steps:
  other:
    image: redis:7
"#,
    );
    let err = ComposeSpec::load(&both_services).expect_err("services and steps");
    assert!(
        err.to_string()
            .contains("both top-level 'services' and 'steps'")
    );

    let script_command = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    command: echo hi
    script: echo hi
"#,
    );
    let err = ComposeSpec::load(&script_command).expect_err("script command conflict");
    assert!(err.to_string().contains("both script and command"));

    let script_entrypoint = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    entrypoint: /bin/sh
    script: echo hi
"#,
    );
    let err = ComposeSpec::load(&script_entrypoint).expect_err("script entrypoint conflict");
    assert!(err.to_string().contains("both script and entrypoint"));

    let empty_script = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    script: "   "
"#,
    );
    let err = ComposeSpec::load(&empty_script).expect_err("empty script");
    assert!(err.to_string().contains("script must not be empty"));
    let err = validate_service_script("bad\0script", "service 'app' script").expect_err("nul");
    assert!(err.to_string().contains("null bytes"));

    let root_modules_conflict = write_spec(
        tmpdir.path(),
        r#"
modules:
  - cuda/12
x-env:
  modules:
    - openmpi/5
services:
  app:
    image: redis:7
"#,
    );
    let err = ComposeSpec::load(&root_modules_conflict).expect_err("root modules conflict");
    assert!(
        err.to_string()
            .contains("root sets both 'modules' and 'x-env.modules'")
    );

    let service_modules_conflict = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    modules:
      - cuda/12
    x-env:
      modules:
        - openmpi/5
"#,
    );
    let err = ComposeSpec::load(&service_modules_conflict).expect_err("service modules conflict");
    assert!(
        err.to_string()
            .contains("service 'app' sets both 'modules' and 'x-env.modules'")
    );
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
                implicit: false,
            },
            ServiceDependency {
                name: "preprocess".into(),
                condition: DependencyCondition::ServiceCompletedSuccessfully,
                implicit: false,
            },
            ServiceDependency {
                name: "redis".into(),
                condition: DependencyCondition::ServiceStarted,
                implicit: false,
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
fn slurm_resource_count_validation_rejects_zero_for_all_first_class_counts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "top-level nodes",
            r#"
x-slurm:
  nodes: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.nodes",
        ),
        (
            "top-level ntasks",
            r#"
x-slurm:
  ntasks: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.ntasks",
        ),
        (
            "top-level ntasks_per_node",
            r#"
x-slurm:
  ntasks_per_node: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.ntasks_per_node",
        ),
        (
            "top-level cpus_per_task",
            r#"
x-slurm:
  cpus_per_task: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.cpus_per_task",
        ),
        (
            "top-level gpus",
            r#"
x-slurm:
  gpus: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.gpus",
        ),
        (
            "top-level gpus_per_node",
            r#"
x-slurm:
  gpus_per_node: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.gpus_per_node",
        ),
        (
            "top-level gpus_per_task",
            r#"
x-slurm:
  gpus_per_task: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.gpus_per_task",
        ),
        (
            "top-level cpus_per_gpu",
            r#"
x-slurm:
  cpus_per_gpu: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.cpus_per_gpu",
        ),
        (
            "service nodes",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      nodes: 0
"#,
            "service 'app' x-slurm.nodes",
        ),
        (
            "service ntasks",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      ntasks: 0
"#,
            "service 'app' x-slurm.ntasks",
        ),
        (
            "service ntasks_per_node",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      ntasks_per_node: 0
"#,
            "service 'app' x-slurm.ntasks_per_node",
        ),
        (
            "service cpus_per_task",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      cpus_per_task: 0
"#,
            "service 'app' x-slurm.cpus_per_task",
        ),
        (
            "service gpus",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      gpus: 0
"#,
            "service 'app' x-slurm.gpus",
        ),
        (
            "service gpus_per_node",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      gpus_per_node: 0
"#,
            "service 'app' x-slurm.gpus_per_node",
        ),
        (
            "service gpus_per_task",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      gpus_per_task: 0
"#,
            "service 'app' x-slurm.gpus_per_task",
        ),
        (
            "service cpus_per_gpu",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      cpus_per_gpu: 0
"#,
            "service 'app' x-slurm.cpus_per_gpu",
        ),
        (
            "service mpi expected ranks",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        expected_ranks: 0
"#,
            "service 'app' x-slurm.mpi.expected_ranks",
        ),
    ];

    for (label, body, expected_field) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject zero resource count"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected_field),
            "{label} should mention {expected_field}; got {text}"
        );
        assert!(
            text.contains("must be at least 1"),
            "{label} should preserve positive-count message; got {text}"
        );
    }
}

#[test]
fn slurm_raw_flag_conflicts_cover_notify_dependency_and_resource_aliases() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "after_job dependency",
            r#"
x-slurm:
  after_job:
    id: "67890"
  submit_args:
    - "--dependency=afterok:67890"
services:
  app:
    image: redis:7
"#,
            "x-slurm.after_job cannot be combined with raw --dependency",
        ),
        (
            "dependency alias",
            r#"
x-slurm:
  dependency: singleton
  submit_args:
    - "--dependency singleton"
services:
  app:
    image: redis:7
"#,
            "x-slurm.dependency cannot be combined with raw --dependency",
        ),
        (
            "notify mail",
            r#"
x-slurm:
  notify:
    email:
      to: ops@example.com
  submit_args:
    - "--mail-type=END"
services:
  app:
    image: redis:7
"#,
            "x-slurm.notify.email cannot be combined with raw --mail-type/--mail-user",
        ),
        (
            "gpu bind",
            r#"
x-slurm:
  gpu_bind: closest
  submit_args:
    - "--gpu-bind=none"
services:
  app:
    image: redis:7
"#,
            "x-slurm.gpu_bind cannot be combined with raw --gpu-bind",
        ),
        (
            "service mem_per_gpu",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mem_per_gpu: 10G
      extra_srun_args:
        - "--mem-per-gpu=20G"
"#,
            "service 'app' x-slurm.mem_per_gpu cannot be combined with raw --mem-per-gpu",
        ),
        (
            "service distribution",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      distribution: block
      extra_srun_args:
        - "--distribution cyclic"
"#,
            "service 'app' x-slurm.distribution cannot be combined with raw --distribution",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject duplicate raw Slurm flag"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

#[test]
fn slurm_raw_flag_conflicts_reject_short_array_alias() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  array: "0-3"
  submit_args:
    - "-a 0-7"
services:
  app:
    image: redis:7
"#,
    );

    let err = ComposeSpec::load(&path).expect_err("raw -a conflict");
    assert!(
        err.to_string()
            .contains("x-slurm.array cannot be combined with raw -a"),
        "{err:#}"
    );
}

#[test]
fn service_level_gres_and_extra_srun_args_reject_line_breaks() {
    // Regression: service-level gres / extra_srun_args were not sbatch-safe
    // validated (unlike the top-level gres / submit_args), so an interpolated
    // newline could split the rendered `#SBATCH --gres=` directive or srun line.
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let gres = write_spec(
        tmpdir.path(),
        "services:\n  app:\n    image: redis:7\n    x-slurm:\n      gres: \"gpu:1\\nbad\"\n",
    );
    let err = ComposeSpec::load(&gres).expect_err("newline in service gres");
    assert!(
        format!("{err:#}").contains("x-slurm.gres must not contain line breaks"),
        "{err:#}"
    );

    let srun = write_spec(
        tmpdir.path(),
        "services:\n  app:\n    image: redis:7\n    x-slurm:\n      extra_srun_args:\n        - \"--foo\\nbar\"\n",
    );
    let err = ComposeSpec::load(&srun).expect_err("newline in service extra_srun_args");
    assert!(
        format!("{err:#}").contains("x-slurm.extra_srun_args[0] must not contain line breaks"),
        "{err:#}"
    );
}

#[test]
fn raw_flag_conflicts_ignore_longer_prefixes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  array: "0-3"
  gpu_bind: closest
  submit_args:
    - "--array-task-throttle=2"
    - "--gpu-bind-extra=debug"
services:
  app:
    image: redis:7
"#,
    );

    let spec = ComposeSpec::load(&path).expect("longer flag prefixes are not conflicts");
    assert_eq!(spec.slurm.array.as_deref(), Some("0-3"));
    assert_eq!(spec.slurm.submit_args.len(), 2);
}

#[test]
fn rendezvous_validation_rejects_invalid_names_protocols_paths_and_empty_discovery() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "empty discovery",
            r#"
x-slurm:
  rendezvous:
    discover: []
services:
  app:
    image: redis:7
"#,
            "x-slurm.rendezvous.discover must contain at least one name",
        ),
        (
            "invalid discovery name",
            r#"
x-slurm:
  rendezvous:
    discover:
      - "bad name"
services:
  app:
    image: redis:7
"#,
            "x-slurm.rendezvous.discover[0] must contain only ASCII letters",
        ),
        (
            "invalid provider name",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: "bad name"
          port: 8080
"#,
            "x-slurm.rendezvous.register.name must contain only ASCII letters",
        ),
        (
            "invalid provider port",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: api
          port: 0
"#,
            "x-slurm.rendezvous.register.port must be at least 1",
        ),
        (
            "invalid provider protocol",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: api
          port: 8080
          protocol: "http://bad"
"#,
            "x-slurm.rendezvous.register.protocol must contain only ASCII letters",
        ),
        (
            "invalid provider path",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: api
          port: 8080
          path: v1
"#,
            "x-slurm.rendezvous.register.path must be empty or start with '/'",
        ),
        (
            "invalid provider ttl",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: api
          port: 8080
          ttl_seconds: 0
"#,
            "x-slurm.rendezvous.register.ttl_seconds must be at least 1",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid rendezvous config"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

#[test]
fn rendezvous_validation_rejects_timeout_zero_and_bad_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "timeout zero",
            r#"
x-slurm:
  rendezvous:
    discover: api
    timeout_seconds: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.rendezvous.timeout_seconds must be at least 1",
        ),
        (
            "metadata key",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: api
          port: 8080
          metadata:
            "bad key": value
"#,
            "x-slurm.rendezvous.register.metadata key must contain only ASCII",
        ),
        (
            "metadata value",
            "services:\n  app:\n    image: redis:7\n    x-slurm:\n      rendezvous:\n        register:\n          name: api\n          port: 8080\n          metadata:\n            version: \"bad\\0value\"\n",
            "x-slurm.rendezvous.register.metadata.version must not contain null bytes",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid rendezvous config"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(text.contains(expected), "{label}: {text}");
    }
}

#[test]
fn slurm_stage_scratch_and_burst_buffer_validation_rejects_invalid_paths_and_directives() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "scratch empty base",
            r#"
x-slurm:
  scratch:
    base: " "
    mount: /scratch
services:
  app:
    image: redis:7
"#,
            "x-slurm.scratch.base must not be empty",
        ),
        (
            "scratch relative mount",
            r#"
x-slurm:
  scratch:
    base: /scratch
    mount: scratch
services:
  app:
    image: redis:7
"#,
            "x-slurm.scratch.mount must be an absolute container path",
        ),
        (
            "stage-in empty source",
            r#"
x-slurm:
  stage_in:
    - from: " "
      to: /hpc-compose/job/input
services:
  app:
    image: redis:7
"#,
            "x-slurm.stage_in[0].from must not be empty",
        ),
        (
            "stage-out nul destination",
            "x-slurm:\n  stage_out:\n    - from: /hpc-compose/job/out\n      to: \"bad\\0path\"\nservices:\n  app:\n    image: redis:7\n",
            "x-slurm.stage_out[0].to must not contain null bytes",
        ),
        (
            "burst-buffer prefix",
            r##"
x-slurm:
  burst_buffer:
    directives:
      - "#BAD capacity=10G"
services:
  app:
    image: redis:7
"##,
            "x-slurm.burst_buffer.directives[0] must start with '#BB ' or '#DW '",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid staging config"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

#[test]
fn stage_in_hf_uri_validation_rejects_missing_or_floating_rev() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "missing revision",
            r#"
x-slurm:
  stage_in:
    - to: /models/llama
      hf:
        repo: meta-llama/Llama-3.1-8B
        revision: " "
services:
  app:
    image: redis:7
"#,
            "x-slurm.stage_in[0].hf",
        ),
        (
            "floating ref main",
            r#"
x-slurm:
  stage_in:
    - to: /models/llama
      hf:
        repo: meta-llama/Llama-3.1-8B
        revision: main
services:
  app:
    image: redis:7
"#,
            "floating ref",
        ),
        (
            "both from and hf",
            r#"
x-slurm:
  stage_in:
    - from: /shared/in
      to: /models/llama
      hf:
        repo: meta-llama/Llama-3.1-8B
        revision: abc1234
services:
  app:
    image: redis:7
"#,
            "exactly one of 'from'",
        ),
        (
            "neither from nor hf",
            r#"
x-slurm:
  stage_in:
    - to: /models/llama
services:
  app:
    image: redis:7
"#,
            "must set either 'from'",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid hf stage_in"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

#[test]
fn stage_in_hf_uri_validation_accepts_pinned_rev_and_preserves_path_mode() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    // A pinned commit-SHA-shaped revision validates as a dataset.
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  stage_in:
    - to: /data/cifar
      hf:
        repo: org/cifar10
        revision: abc1234def5678
        kind: dataset
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&path).expect("hf stage_in should validate");
    let entry = &spec.slurm.stage_in[0];
    assert!(entry.from.is_none());
    let hf = entry.hf.as_ref().expect("hf source");
    assert_eq!(hf.repo, "org/cifar10");
    assert_eq!(hf.revision, "abc1234def5678");
    assert_eq!(hf.kind, crate::spec::HfStageKind::Dataset);

    // A filesystem-path stage_in still validates and round-trips unchanged.
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  stage_in:
    - from: /shared/input
      to: /scratch/input
      mode: copy
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&path).expect("path stage_in should validate");
    let entry = &spec.slurm.stage_in[0];
    assert_eq!(entry.from.as_deref(), Some("/shared/input"));
    assert_eq!(entry.to, "/scratch/input");
    assert!(entry.hf.is_none());
}

#[test]
fn slurm_notify_and_dependency_helpers_normalize_defaults_and_interpolate_ids() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  after_job:
    id: ${UPSTREAM_JOB:-67890}
    condition: afterok
  dependency: singleton
  notify:
    email:
      to: ${MAIL_TO:-ops@example.com}
services:
  app:
    image: redis:7
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load spec");
    assert!(spec.slurm.has_scheduler_dependency());
    assert_eq!(
        spec.slurm.dependency_cli_value().as_deref(),
        Some("afterok:67890,singleton")
    );
    assert_eq!(
        spec.slurm.notify_email_events(),
        vec![NotifyEvent::End, NotifyEvent::Fail]
    );
    assert_eq!(
        spec.slurm.notify_mail_type_value().as_deref(),
        Some("END,FAIL")
    );
    assert_eq!(spec.slurm.notify_email_recipient(), Some("ops@example.com"));
}

#[test]
fn rendezvous_config_accepts_shorthand_and_interpolates_provider_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  rendezvous:
    discover: ${SERVICE_NAME:-api}
    timeout_seconds: 15
    require: true
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: ${SERVICE_NAME:-api}
          port: 8080
          protocol: http+tcp
          path: /v1
          ttl_seconds: 30
          metadata:
            version: ${VERSION:-canary}
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load spec");
    let client = spec.slurm.rendezvous.expect("client rendezvous");
    assert_eq!(client.discover, vec!["api"]);
    assert_eq!(client.timeout_seconds, Some(15));
    assert_eq!(client.require, Some(true));

    let service = spec.services.get("app").expect("app");
    let register = service
        .slurm
        .rendezvous
        .as_ref()
        .and_then(|rendezvous| rendezvous.register.as_ref())
        .expect("register");
    assert_eq!(register.name, "api");
    assert_eq!(register.port, 8080);
    assert_eq!(register.protocol.as_deref(), Some("http+tcp"));
    assert_eq!(register.path.as_deref(), Some("/v1"));
    assert_eq!(register.ttl_seconds, Some(30));
    assert_eq!(
        register.metadata.get("version").map(String::as_str),
        Some("canary")
    );
}

#[test]
fn rendezvous_provider_interpolates_fields_and_metadata_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      rendezvous:
        register:
          name: ${RDZV_NAME:-api}
          port: 8080
          protocol: ${RDZV_PROTOCOL:-http}
          path: ${RDZV_PATH:-/ready}
          metadata:
            role: ${RDZV_ROLE:-primary}
"#,
    );

    let spec = ComposeSpec::load(&path).expect("load spec");
    let register = spec.services["app"]
        .slurm
        .rendezvous
        .as_ref()
        .and_then(|config| config.register.as_ref())
        .expect("register config");
    assert_eq!(register.name, "api");
    assert_eq!(register.protocol.as_deref(), Some("http"));
    assert_eq!(register.path.as_deref(), Some("/ready"));
    assert_eq!(
        register.metadata.get("role").map(String::as_str),
        Some("primary")
    );
}

#[test]
fn service_placement_validation_rejects_selector_conflicts_and_invalid_bounds() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "missing selector",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement: {}
"#,
            "must set exactly one of node_range, node_count, node_percent, or share_with",
        ),
        (
            "conflicting selectors",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "0"
        node_count: 1
"#,
            "must set exactly one of node_range, node_count, node_percent, or share_with",
        ),
        (
            "zero node count",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_count: 0
"#,
            "x-slurm.placement.node_count must be at least 1",
        ),
        (
            "percent too large",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_percent: 101
"#,
            "x-slurm.placement.node_percent must be between 1 and 100",
        ),
        (
            "start index with range",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        node_range: "0-1"
        start_index: 1
"#,
            "x-slurm.placement.start_index is only valid with node_count or node_percent",
        ),
        (
            "share with exclude",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        share_with: other
        exclude: "0"
"#,
            "x-slurm.placement.share_with cannot be combined with start_index or exclude",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid placement"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

#[test]
fn service_placement_rejects_blank_share_with() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      placement:
        share_with: " "
"#,
    );

    let err = ComposeSpec::load(&path).expect_err("blank share_with");
    assert!(
        err.to_string()
            .contains("x-slurm.placement.share_with must not be empty"),
        "{err:#}"
    );
}

#[test]
fn notify_empty_on_defaults_to_end_fail_and_all_collapses_to_all() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let default_events = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  notify:
    email:
      to: ops@example.com
      on: []
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&default_events).expect("default notify events");
    assert_eq!(
        spec.slurm.notify_email_events(),
        vec![NotifyEvent::End, NotifyEvent::Fail]
    );
    assert_eq!(
        spec.slurm.notify_mail_type_value().as_deref(),
        Some("END,FAIL")
    );

    let all_events = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  notify:
    email:
      to: ops@example.com
      on:
        - start
        - all
        - fail
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&all_events).expect("all notify events");
    assert_eq!(spec.slurm.notify_email_events(), vec![NotifyEvent::All]);
    assert_eq!(spec.slurm.notify_mail_type_value().as_deref(), Some("ALL"));
}

// --- richer mail events: canonical ordering, `all` modifier, serde renames,
// and the `array_tasks`-requires-`array` guard ---

#[test]
fn notify_email_events_apply_stable_canonical_order_and_dedupe_full_variant_set() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // Every non-`all` token, scrambled and with duplicates, and `array` set so
    // the `array_tasks` guard is satisfied. Output must be canonical + deduped.
    let spec = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  array: "0-9"
  notify:
    email:
      to: ops@example.com
      on:
        - array_tasks
        - time_limit_50
        - fail
        - start
        - time_limit
        - requeue
        - end
        - time_limit_80
        - stage_out
        - invalid_depend
        - time_limit_90
        - fail
        - array_tasks
        - start
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&spec).expect("full notify event set");
    assert_eq!(
        spec.slurm.notify_email_events(),
        vec![
            NotifyEvent::Start,
            NotifyEvent::End,
            NotifyEvent::Fail,
            NotifyEvent::Requeue,
            NotifyEvent::InvalidDepend,
            NotifyEvent::StageOut,
            NotifyEvent::TimeLimit,
            NotifyEvent::TimeLimit90,
            NotifyEvent::TimeLimit80,
            NotifyEvent::TimeLimit50,
            NotifyEvent::ArrayTasks,
        ]
    );
    assert_eq!(
        spec.slurm.notify_mail_type_value().as_deref(),
        Some(
            "BEGIN,END,FAIL,REQUEUE,INVALID_DEPEND,STAGE_OUT,TIME_LIMIT,TIME_LIMIT_90,TIME_LIMIT_80,TIME_LIMIT_50,ARRAY_TASKS"
        )
    );
}

#[test]
fn notify_all_shorthand_preserves_explicit_array_tasks_modifier() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // `all` collapses everything else, but `array_tasks` is an independent
    // modifier Slurm accepts alongside `ALL`, so it must survive.
    let with_modifier = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  array: "0-3"
  notify:
    email:
      to: ops@example.com
      on:
        - start
        - all
        - array_tasks
        - fail
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&with_modifier).expect("all + array_tasks");
    assert_eq!(
        spec.slurm.notify_email_events(),
        vec![NotifyEvent::All, NotifyEvent::ArrayTasks]
    );
    assert_eq!(
        spec.slurm.notify_mail_type_value().as_deref(),
        Some("ALL,ARRAY_TASKS")
    );

    let plain_all = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  notify:
    email:
      to: ops@example.com
      on:
        - all
        - end
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&plain_all).expect("all only");
    assert_eq!(spec.slurm.notify_email_events(), vec![NotifyEvent::All]);
    assert_eq!(spec.slurm.notify_mail_type_value().as_deref(), Some("ALL"));
}

#[test]
fn notify_time_limit_variants_deserialize_with_underscored_digits() {
    // Guards the explicit `#[serde(rename = "time_limit_NN")]`: serde's
    // snake_case does NOT insert `_` before digits, so without the renames
    // `time_limit_90` would fail to deserialize.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let spec = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  notify:
    email:
      to: ops@example.com
      on:
        - time_limit_90
        - time_limit_80
        - time_limit_50
services:
  app:
    image: redis:7
"#,
    );
    let spec = ComposeSpec::load(&spec).expect("time_limit_NN tokens deserialize");
    assert_eq!(
        spec.slurm.notify_email_events(),
        vec![
            NotifyEvent::TimeLimit90,
            NotifyEvent::TimeLimit80,
            NotifyEvent::TimeLimit50,
        ]
    );
    assert_eq!(
        spec.slurm.notify_mail_type_value().as_deref(),
        Some("TIME_LIMIT_90,TIME_LIMIT_80,TIME_LIMIT_50")
    );
}

#[test]
fn notify_array_tasks_without_array_is_rejected() {
    let config = SlurmConfig {
        notify: Some(NotifyConfig {
            email: Some(EmailNotifyConfig {
                to: "ops@example.com".to_string(),
                on: vec![NotifyEvent::End, NotifyEvent::ArrayTasks],
            }),
        }),
        ..SlurmConfig::default()
    };
    let err = config
        .validate()
        .expect_err("array_tasks without x-slurm.array is rejected");
    assert!(
        err.downcast_ref::<SpecError>()
            .is_some_and(|se| matches!(se, SpecError::ArrayTasksRequiresArray)),
        "expected ArrayTasksRequiresArray, got {err:#}",
    );

    // The same config with an array index range set validates cleanly.
    let config = SlurmConfig {
        array: Some("0-9".to_string()),
        notify: Some(NotifyConfig {
            email: Some(EmailNotifyConfig {
                to: "ops@example.com".to_string(),
                on: vec![NotifyEvent::End, NotifyEvent::ArrayTasks],
            }),
        }),
        ..SlurmConfig::default()
    };
    assert!(
        config.validate().is_ok(),
        "array_tasks with x-slurm.array set should validate",
    );
}

#[test]
fn service_mpi_host_config_validation_covers_bind_paths_env_and_profile_conflicts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "relative container bind",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        host_mpi:
          bind_paths:
            - /opt/mpi:opt/mpi
"#,
            "container path must be absolute",
        ),
        (
            "bad bind mode",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        host_mpi:
          bind_paths:
            - /opt/mpi:/opt/mpi:cached
"#,
            "unsupported mode 'cached'",
        ),
        (
            "bad env name",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        host_mpi:
          env:
            BAD-NAME: value
"#,
            "x-slurm.mpi.host_mpi.env.BAD-NAME",
        ),
        (
            "profile implementation conflict",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        profile: openmpi
        implementation: mpich
"#,
            "x-slurm.mpi.profile=openmpi conflicts with x-slurm.mpi.implementation=mpich",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = match ComposeSpec::load(&path) {
            Ok(_) => panic!("{label} should reject invalid MPI config"),
            Err(err) => err,
        };
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
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
        vec![
            MetricsCollector::Gpu,
            MetricsCollector::Slurm,
            MetricsCollector::Cpu
        ]
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
    export_dir: ./results/${SLURM_JOB_ID}
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
    assert_eq!(
        artifacts.export_dir.as_deref(),
        Some("./results/${SLURM_JOB_ID}")
    );
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
fn service_assert_contract_parses_and_validates() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  train:
    image: trainer:latest
    assert:
      exit_code: 0
      artifacts_contain: "model/*.pt"
      max_duration_seconds: 7200
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let train = spec.services.get("train").expect("service");
    assert_eq!(
        train
            .assertions
            .as_ref()
            .and_then(ServiceAssertSpec::normalized_artifacts_contain)
            .as_deref(),
        Some("/hpc-compose/job/model/*.pt")
    );
    let config = spec
        .effective_config(&tmpdir.path().join("cache"), &BTreeMap::new())
        .expect("effective config");
    let value = serde_json::to_value(config).expect("config json");
    assert_eq!(value["services"]["train"]["assert"]["exit_code"], 0);
    assert_eq!(
        value["services"]["train"]["assert"]["artifacts_contain"],
        "model/*.pt"
    );
    assert_eq!(
        value["services"]["train"]["assert"]["max_duration_seconds"],
        7200
    );

    for (yaml, needle) in [
        (
            "services:\n  train:\n    image: trainer:latest\n    assert:\n      exit_code: 256\n",
            "assert.exit_code",
        ),
        (
            "services:\n  train:\n    image: trainer:latest\n    assert:\n      artifacts_contain: ''\n",
            "assert.artifacts_contain",
        ),
        (
            "services:\n  train:\n    image: trainer:latest\n    assert:\n      artifacts_contain: /outside/*.pt\n",
            "under /hpc-compose/job",
        ),
        (
            "services:\n  train:\n    image: trainer:latest\n    assert:\n      max_duration_seconds: 0\n",
            "assert.max_duration_seconds",
        ),
    ] {
        let path = write_spec(tmpdir.path(), yaml);
        let err = ComposeSpec::load(&path).expect_err("invalid assert");
        assert!(
            err.to_string().contains(needle),
            "expected error containing {needle:?}, got {err}"
        );
    }
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
    assert!(err.to_string().contains("'app' must be a mapping"));

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
    assert!(err.to_string().contains("unsupported key 'networks'"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.help()
            .is_some_and(|h| h.to_string().contains("custom container networking"))
    }));

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
    assert!(err.to_string().contains("unsupported key 'restart'"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.help()
            .is_some_and(|h| h.to_string().contains("x-slurm.failure_policy"))
    }));

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
    assert!(err.to_string().contains("unsupported key 'deploy'"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.help()
            .is_some_and(|h| h.to_string().contains("long-running orchestrator"))
    }));
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
fn referenced_variable_scanner_tracks_only_scalar_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: ${IMAGE:-redis:7}
    environment:
      "${IGNORED_KEY:-key}": fixed
      TOKEN: "${API_TOKEN}"
      FALLBACK: "${A:-${B:-fallback}}"
      ESCAPED: "$${IGNORED_ESCAPED:-literal}"
    # ${IGNORED_COMMENT:-comment}
"#,
    );

    let referenced = referenced_variables(&path, &BTreeMap::new()).expect("scan");
    assert_eq!(
        referenced,
        BTreeSet::from([
            "A".to_string(),
            "API_TOKEN".to_string(),
            "B".to_string(),
            "IMAGE".to_string()
        ])
    );
}

#[test]
fn referenced_variable_scanner_ignores_unused_default_branch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: "${IMAGE:-${FALLBACK_IMAGE:-redis:7}}"
"#,
    );

    let referenced =
        referenced_variables(&path, &BTreeMap::from([("IMAGE".into(), "redis:7".into())]))
            .expect("scan");
    assert_eq!(referenced, BTreeSet::from(["IMAGE".to_string()]));
}

#[test]
fn referenced_variables_sees_required_variable_names_and_their_message_vars() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    environment:
      A: "${REQUIRED_A:?msg}"
      B: "${REQUIRED_B?msg}"
      C: "${REQUIRED_C:?need ${NESTED_REF}}"
"#,
    );

    let referenced = referenced_variables(&path, &BTreeMap::new()).expect("scan");
    assert_eq!(
        referenced,
        BTreeSet::from([
            "REQUIRED_A".to_string(),
            "REQUIRED_B".to_string(),
            "REQUIRED_C".to_string(),
            "NESTED_REF".to_string(),
        ])
    );
}

#[test]
fn missing_defaulted_variables_does_not_report_required_variables_but_walks_their_messages() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    environment:
      A: "${REQUIRED_VAR:?please set me}"
      B: "${REQUIRED_VAR2:?fallback is ${NESTED:-default-text}}"
"#,
    );

    let missing = missing_defaulted_variables(&path, &BTreeMap::new()).expect("scan");
    assert!(!missing.contains("REQUIRED_VAR"));
    assert!(!missing.contains("REQUIRED_VAR2"));
    assert_eq!(missing, BTreeSet::from(["NESTED".to_string()]));
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

    let outcome = std::panic::catch_unwind(|| missing_defaulted_variables(&path, &BTreeMap::new()));
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
    let message = format!("{err:#}");
    assert!(
        message.contains("use one service-level MPI source"),
        "got {message}"
    );
}

#[test]
fn service_mpi_rejects_unknown_profile() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let unknown_profile = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-slurm:
      mpi:
        type: pmix
        profile: vendor_mpi
"#,
    );
    let err = ComposeSpec::load(&unknown_profile).expect_err("unknown mpi profile");
    let message = format!("{err:#}");
    assert!(message.contains("unknown variant"), "got {message}");
    assert!(message.contains("vendor_mpi"), "got {message}");
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
        reservation: Some("bad\nreservation".to_string()),
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("newline in reservation");
    assert!(err.to_string().contains("x-slurm.reservation"));

    let config = SlurmConfig {
        licenses: Some("bad\0licenses".to_string()),
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("null in licenses");
    assert!(err.to_string().contains("x-slurm.licenses"));

    let config = SlurmConfig {
        submit_args: vec!["--comment=ok".to_string(), "--comment=bad\narg".to_string()],
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("line break in submit arg");
    assert!(err.to_string().contains("x-slurm.submit_args[1]"));
}

#[test]
fn slurm_config_accepts_reservation_and_licenses_pass_through() {
    let config = SlurmConfig {
        reservation: Some("maint_2026".to_string()),
        licenses: Some("ansys:2,comsol:1".to_string()),
        ..SlurmConfig::default()
    };
    assert!(
        config.validate().is_ok(),
        "comma/colon license grammar and a plain reservation name should pass transport-safety validation"
    );
}

#[test]
fn cleanup_runtime_cache_policy_parses_and_defaults_to_never() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let default_path = write_spec(
        tmpdir.path(),
        "services:\n  app:\n    image: app:latest\n    command: run\n",
    );
    let spec = ComposeSpec::load(&default_path).expect("load default");
    assert_eq!(
        spec.slurm.cleanup.runtime_cache,
        RuntimeCacheCleanupPolicy::Never
    );

    let path = write_spec(
        tmpdir.path(),
        "x-slurm:\n  cleanup:\n    runtime_cache: on_success\nservices:\n  app:\n    image: app:latest\n    command: run\n",
    );
    let spec = ComposeSpec::load(&path).expect("load on_success");
    assert_eq!(
        spec.slurm.cleanup.runtime_cache,
        RuntimeCacheCleanupPolicy::OnSuccess
    );

    let bad = write_spec(
        tmpdir.path(),
        "x-slurm:\n  cleanup:\n    bogus: true\nservices:\n  app:\n    image: app:latest\n    command: run\n",
    );
    assert!(
        ComposeSpec::load(&bad).is_err(),
        "unknown key under x-slurm.cleanup must be rejected"
    );
}

#[test]
fn slurm_config_validates_output_error_log_patterns() {
    // Valid relative and absolute patterns with specifiers are accepted.
    for value in ["logs/%x-%j.out", "/shared/logs/%x-%j.out", "%j.out"] {
        let config = SlurmConfig {
            output: Some(value.to_string()),
            ..SlurmConfig::default()
        };
        assert!(
            config.validate().is_ok(),
            "expected '{value}' to be accepted"
        );
    }

    // Literal `..` traversal is rejected before or after Slurm specifiers.
    for value in ["../escape/%j.out", "%j/../../escape.out"] {
        let config = SlurmConfig {
            output: Some(value.to_string()),
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("traversal in output");
        assert!(err.to_string().contains("path traversal"));
    }

    // Empty error pattern is rejected.
    let config = SlurmConfig {
        error: Some("   ".to_string()),
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("empty error");
    assert!(err.to_string().contains("x-slurm.error"));
}

#[test]
fn slurm_resource_counts_must_be_positive() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for (name, body, needle) in [
        (
            "top-level-cpus-per-task",
            r#"
x-slurm:
  cpus_per_task: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.cpus_per_task",
        ),
        (
            "top-level-gpus",
            r#"
x-slurm:
  gpus: 0
services:
  app:
    image: redis:7
"#,
            "x-slurm.gpus",
        ),
        (
            "service-cpus-per-task",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      cpus_per_task: 0
"#,
            "service 'app' x-slurm.cpus_per_task",
        ),
        (
            "service-gpus",
            r#"
services:
  app:
    image: redis:7
    x-slurm:
      gpus: 0
"#,
            "service 'app' x-slurm.gpus",
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

    let config = SlurmConfig {
        array: Some("0-3".into()),
        submit_args: vec!["--array=0-9".into()],
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("array conflict");
    assert!(err.to_string().contains("array"));

    let config = SlurmConfig {
        after_job: Some(JobDependencySpec::Id("12345".into())),
        submit_args: vec!["--dependency=afterok:999".into()],
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("dependency conflict");
    assert!(err.to_string().contains("after_job"));
}

#[test]
fn slurm_array_spec_validation_accepts_supported_forms() {
    for array in ["0", "1-10", "1-10:2", "0,3,8-12", "0-99%10"] {
        let config = SlurmConfig {
            array: Some(array.to_string()),
            ..SlurmConfig::default()
        };
        config
            .validate()
            .unwrap_or_else(|err| panic!("{array} should validate: {err:#}"));
    }
}

#[test]
fn slurm_array_spec_validation_rejects_malformed_forms() {
    for array in [
        "", "1 2", "1\0", "1-", "-1", "1-10:0", "0-9%0", "9-1", "1,,2",
    ] {
        let config = SlurmConfig {
            array: Some(array.to_string()),
            ..SlurmConfig::default()
        };
        assert!(
            config.validate().is_err(),
            "{array:?} should fail validation"
        );
    }
}

#[test]
fn slurm_dependency_parses_shorthand_mapping_and_singleton() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let shorthand = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  after_job: "12345"
services:
  app:
    image: alpine
"#,
    );
    let spec = ComposeSpec::load(&shorthand).expect("shorthand");
    assert_eq!(
        spec.slurm.dependency_cli_value().as_deref(),
        Some("afterany:12345")
    );

    let mapping = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  after_job:
    id: "12345_7"
    condition: afterok
  dependency: singleton
services:
  app:
    image: alpine
"#,
    );
    let spec = ComposeSpec::load(&mapping).expect("mapping");
    assert_eq!(
        spec.slurm.dependency_cli_value().as_deref(),
        Some("afterok:12345_7,singleton")
    );
}

#[test]
fn slurm_dependency_rejects_bad_conditions_and_job_ids() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let bad_condition = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  after_job:
    id: "12345"
    condition: after
services:
  app:
    image: alpine
"#,
    );
    assert!(ComposeSpec::load(&bad_condition).is_err());

    let bad_id = write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  after_job: "abc"
services:
  app:
    image: alpine
"#,
    );
    let err = ComposeSpec::load(&bad_id).expect_err("bad id");
    assert!(err.to_string().contains("x-slurm.after_job"));
}

#[test]
fn slurm_time_limit_accepts_valid_forms() {
    assert_eq!(parse_slurm_time_limit("30").expect("MM"), 30 * 60);
    assert_eq!(parse_slurm_time_limit("01:30").expect("MM:SS"), 90);
    assert_eq!(parse_slurm_time_limit("01:30:00").expect("HH:MM:SS"), 5_400);
    assert_eq!(
        parse_slurm_time_limit("2-00:00:00").expect("D-HH:MM:SS"),
        2 * 86_400
    );
    assert_eq!(
        parse_slurm_time_limit("1-12").expect("D-HH"),
        86_400 + 12 * 3_600
    );
    // Leading field is unbounded: large minutes (MM) and large hours (HH:MM:SS).
    assert_eq!(parse_slurm_time_limit("90").expect("MM > 59"), 90 * 60);
    assert_eq!(
        parse_slurm_time_limit("100:00:00").expect("HH > 23"),
        100 * 3_600
    );
}

#[test]
fn slurm_time_limit_rejects_out_of_range_components() {
    // Seconds out of range in MM:SS.
    let err = parse_slurm_time_limit("00:90").expect_err("00:90");
    assert!(err.to_string().contains("0-59"), "unexpected error: {err}");
    // Minutes out of range in HH:MM:SS.
    let err = parse_slurm_time_limit("1:90:00").expect_err("1:90:00");
    assert!(err.to_string().contains("0-59"), "unexpected error: {err}");
    // Seconds out of range in HH:MM:SS.
    assert!(parse_slurm_time_limit("01:00:60").is_err());
    // Hours out of range once a day prefix carries the magnitude.
    assert!(parse_slurm_time_limit("1-24:00:00").is_err());
    assert!(parse_slurm_time_limit("1-24").is_err());
}

#[test]
fn memory_bytes_parser_handles_units_decimals_and_sentinels() {
    assert_eq!(parse_memory_bytes("1048576"), Some(1_048_576));
    assert_eq!(parse_memory_bytes("512M"), Some(512 * 1_024 * 1_024));
    assert_eq!(parse_memory_bytes("2GiB"), Some(2 * GIB));
    assert_eq!(parse_memory_bytes("1.5G"), Some(1_610_612_736));
    // sacct sentinels and empty values map to None.
    assert_eq!(parse_memory_bytes("unknown"), None);
    assert_eq!(parse_memory_bytes("UNKNOWN"), None);
    assert_eq!(parse_memory_bytes("   "), None);
    // Unsupported units and missing magnitudes are rejected.
    assert_eq!(parse_memory_bytes("4Gc"), None);
    assert_eq!(parse_memory_bytes("G"), None);
    // Integer and decimal forms of the same size round-trip to the same bytes.
    assert_eq!(parse_memory_bytes("2G"), parse_memory_bytes("2.0G"));
    // Saturates instead of overflowing.
    assert_eq!(parse_memory_bytes("99999999999P"), Some(u64::MAX));
}

proptest! {
    #![proptest_config(prop_config())]

    #[test]
    fn property_memory_bytes_parser_is_total(
        value in string_regex("[0-9]{0,6}(\\.[0-9]{0,3})?\\s*[KMGTPkmgtpiIbB]{0,3}")
            .expect("memory regex")
    ) {
        // The parser must be total: never panic, regardless of the input shape.
        let parsed = parse_memory_bytes(&value);
        // Re-parsing a successfully parsed integer byte count is idempotent.
        if let Some(bytes) = parsed {
            prop_assert_eq!(parse_memory_bytes(&bytes.to_string()), Some(bytes));
        }
    }

    #[test]
    fn property_rejects_unsupported_root_keys(
        key in key_strategy().prop_filter("unsupported root key", |key| {
            !matches!(
                key.as_str(),
                "name" | "modules" | "runtime" | "services" | "steps" | "sweep" | "version" | "x-env" | "x-slurm"
            )
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
                    | "script"
                    | "environment"
                    | "modules"
                    | "volumes"
                    | "working_dir"
                    | "depends_on"
                    | "readiness"
                    | "healthcheck"
                    | "x-env"
                    | "x-slurm"
                    | "x-runtime"
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
        version in prop::option::of(Just("1".to_string())),
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

#[test]
fn duplicate_prepare_hook_is_rejected() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    x-runtime:
      prepare:
        commands: ["echo a"]
    x-enroot:
      prepare:
        commands: ["echo b"]
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("both prepare hooks should be rejected");
    assert!(
        err.to_string()
            .contains("both x-runtime.prepare and x-enroot.prepare"),
        "unexpected: {err}"
    );
}

#[test]
fn enroot_prepare_requires_pyxis_backend() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
runtime:
  backend: apptainer
services:
  app:
    image: redis:7
    x-enroot:
      prepare:
        commands: ["echo hi"]
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("enroot prepare on apptainer should be rejected");
    assert!(
        err.to_string()
            .contains("x-enroot.prepare with runtime.backend=apptainer"),
        "unexpected: {err}"
    );
}

#[test]
fn array_entrypoint_with_multiline_string_command_is_rejected() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        "
services:
  app:
    image: redis:7
    entrypoint: [\"bash\", \"-lc\"]
    command: \"echo a\\npython train.py\\n\"
",
    );
    let err = ComposeSpec::load(&path).expect_err("array entrypoint + multiline command rejected");
    assert!(
        err.to_string().contains("mixes array-form entrypoint"),
        "unexpected: {err}"
    );
}

#[test]
fn secret_requires_a_source() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
secrets:
  tok: {}
services:
  app:
    image: redis:7
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("secret with neither file nor env rejected");
    assert!(
        err.to_string().contains("must set either 'file' or 'env'"),
        "unexpected: {err}"
    );
}

#[test]
fn slurm_job_id_validation_rejects_boundary_forms() {
    let field = "x-slurm.after_job";
    let cases = [
        ("", "must not be empty"),
        ("12\u{0}34", "must not contain null bytes"),
        ("123 45", "must not contain whitespace"),
        ("12345_7_8", "must be a Slurm job id like 12345"),
        ("0", "job id must be greater than 0"),
        ("0_7", "job id must be greater than 0"),
    ];
    for (input, needle) in cases {
        let err = validate_slurm_job_id(input, field)
            .expect_err(&format!("{input:?} should be rejected"));
        assert!(
            err.to_string().contains(needle),
            "for {input:?} expected {needle:?}, got: {err}"
        );
    }
    assert!(validate_slurm_job_id("_7", field).is_err());
    assert!(validate_slurm_job_id("12345_", field).is_err());
    validate_slurm_job_id("12345", field).expect("plain job id");
    validate_slurm_job_id("12345_7", field).expect("array task id");
}

#[test]
fn artifact_path_rejects_traversal_and_non_container_roots() {
    assert!(
        validate_artifact_path("/../x")
            .unwrap_err()
            .to_string()
            .contains("escapes the root path")
    );
    assert!(validate_artifact_path("/hpc-compose/job/artifacts").is_err());
    assert!(validate_artifact_path("/etc/passwd").is_err());
    assert!(validate_artifact_path("relative/x").is_err());
    validate_artifact_path("/hpc-compose/job/data").expect("valid container path");
}

#[test]
fn resume_path_rejects_traversal_and_container_roots() {
    assert!(
        validate_resume_path("/shared/../../etc")
            .unwrap_err()
            .to_string()
            .contains("escapes the root path")
    );
    assert!(
        validate_resume_path("")
            .unwrap_err()
            .to_string()
            .contains("must not be empty")
    );
    assert!(validate_resume_path("relative/run").is_err());
    assert!(validate_resume_path("/hpc-compose/run").is_err());
    validate_resume_path("/shared/runs/../demo").expect("valid host resume path");
}

#[test]
fn slurm_log_pattern_handles_specifiers_and_traversal() {
    let field = "x-slurm.output";
    validate_slurm_log_pattern(Some("100%%done/%08j.out"), field).expect("specifiers ok");
    validate_slurm_log_pattern(None, field).expect("none ok");
    assert!(
        validate_slurm_log_pattern(Some("a\u{0}b"), field)
            .unwrap_err()
            .to_string()
            .contains("must not contain null bytes")
    );
    assert!(
        validate_slurm_log_pattern(Some("   "), field)
            .unwrap_err()
            .to_string()
            .contains("must not be empty")
    );
    assert!(
        validate_slurm_log_pattern(Some("%%/../x"), field)
            .unwrap_err()
            .to_string()
            .contains("must not use '..' path traversal")
    );
}

#[test]
fn service_assert_artifact_pattern_rejects_bad_forms() {
    let field = "x-slurm.assert.artifacts_contain";
    assert!(
        validate_service_assert_artifact_pattern("out\n/x", field)
            .unwrap_err()
            .to_string()
            .contains("must not contain line breaks")
    );
    assert!(
        validate_service_assert_artifact_pattern("a\u{0}b", field)
            .unwrap_err()
            .to_string()
            .contains("must not contain null bytes")
    );
    assert!(
        validate_service_assert_artifact_pattern("/hpc-compose/job/../etc", field)
            .unwrap_err()
            .to_string()
            .contains("must not contain '..' path components")
    );
    assert!(
        validate_service_assert_artifact_pattern("/etc/passwd", field)
            .unwrap_err()
            .to_string()
            .contains("must be relative or rooted under")
    );
    validate_service_assert_artifact_pattern("relative/ok.txt", field).expect("relative ok");
    validate_service_assert_artifact_pattern("/hpc-compose/job/out.txt", field).expect("rooted ok");
}

#[test]
fn interpolate_string_collapses_double_dollar_and_nested_brace_defaults() {
    let empty = BTreeMap::<String, String>::new();
    assert_eq!(
        interpolate_string("${MISSING:-a$$b}", &empty).expect("double-dollar default"),
        "a$b"
    );
    let inner = BTreeMap::from([("INNER".to_string(), "deep".to_string())]);
    assert_eq!(
        interpolate_string("${MISSING:-${INNER}}", &inner).expect("nested-brace default"),
        "deep"
    );
    let err = interpolate_string("${A:-${B}", &empty).expect_err("unterminated nested default");
    assert!(
        err.to_string().contains("unterminated variable expression"),
        "unexpected error: {err}"
    );
}

#[test]
fn strict_env_scanner_collects_no_colon_nested_defaults_and_ignores_present_empty() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let nested = write_spec(
        tmpdir.path(),
        "services:\n  app:\n    image: redis:7\n    environment:\n      KEEP: \"${A-${B-fb}}\"\n",
    );
    let missing = missing_defaulted_variables(&nested, &BTreeMap::new()).expect("scan nested");
    assert_eq!(missing, BTreeSet::from(["A".to_string(), "B".to_string()]));

    let present_empty = write_spec(
        tmpdir.path(),
        "services:\n  app:\n    image: redis:7\n    environment:\n      KEEP: \"${A-fb}\"\n",
    );
    let missing = missing_defaulted_variables(
        &present_empty,
        &BTreeMap::from([("A".to_string(), String::new())]),
    )
    .expect("scan present-empty");
    assert!(
        missing.is_empty(),
        "present-empty must not be missing: {missing:?}"
    );
}

#[test]
fn parallelism_parses_at_both_scopes_and_rejects_unknown_keys() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
name: tp-pp
x-slurm:
  nodes: 2
  gpus_per_node: 2
  parallelism:
    tensor: 2
    pipeline: 2
services:
  app:
    image: redis:7
    command: run
    x-slurm:
      gpus_per_node: 4
      parallelism:
        tensor: 4
        pipeline: 1
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load parallelism");
    let top = spec
        .slurm
        .parallelism
        .as_ref()
        .expect("top-level parallelism");
    assert_eq!(top.tensor, 2);
    assert_eq!(top.pipeline, 2);
    let service = spec
        .services
        .get("app")
        .expect("service")
        .slurm
        .parallelism
        .as_ref()
        .expect("service parallelism");
    assert_eq!(service.tensor, 4);
    assert_eq!(service.pipeline, 1);

    let bad = write_spec(
        tmpdir.path(),
        "x-slurm:\n  parallelism:\n    tensor: 1\n    pipeline: 1\n    bogus: 2\nservices:\n  app:\n    image: redis:7\n    command: run\n",
    );
    assert!(
        ComposeSpec::load(&bad).is_err(),
        "unknown key under parallelism must be rejected"
    );
}

#[test]
fn parallelism_cross_check_passes_skips_and_fails() {
    // Pass: tensor * pipeline == nodes * gpus_per_node (4 == 4).
    let ok = SlurmConfig {
        nodes: Some(2),
        gpus_per_node: Some(2),
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 2,
        }),
        ..SlurmConfig::default()
    };
    assert!(ok.validate().is_ok(), "matching geometry should validate");

    // Skip: no gpus_per_node => only positivity is enforced, no product check.
    let skip = SlurmConfig {
        nodes: Some(8),
        gpus_per_node: None,
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 2,
        }),
        ..SlurmConfig::default()
    };
    assert!(
        skip.validate().is_ok(),
        "missing gpus_per_node should skip the product cross-check"
    );

    // Fail: 4 != 2.
    let bad = SlurmConfig {
        nodes: Some(1),
        gpus_per_node: Some(2),
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 2,
        }),
        ..SlurmConfig::default()
    };
    let err = bad.validate().expect_err("mismatched geometry");
    assert!(err.to_string().contains("x-slurm"));
    assert!(err.to_string().contains("gpus_per_node"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.code().is_some_and(|c| {
            c.to_string()
                .contains("hpc_compose::spec::parallelism_gpu_mismatch")
        })
    }));
}

#[test]
fn parallelism_service_scope_cross_check_is_scoped_and_defaults_nodes_to_one() {
    // Service nodes default to 1 when unset: 2 * 1 != 4.
    let service = ServiceSlurmConfig {
        gpus_per_node: Some(4),
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 1,
        }),
        ..ServiceSlurmConfig::default()
    };
    let err = service
        .validate("trainer")
        .expect_err("service mismatch with implicit nodes=1");
    assert!(err.to_string().contains("service 'trainer' x-slurm"));

    // With nodes explicit so 2 * 2 == 1 node would mismatch; set nodes=4 => 4 == 4.
    let service_ok = ServiceSlurmConfig {
        nodes: Some(2),
        gpus_per_node: Some(2),
        parallelism: Some(ParallelismConfig {
            tensor: 2,
            pipeline: 2,
        }),
        ..ServiceSlurmConfig::default()
    };
    assert!(service_ok.validate("trainer").is_ok());
}

#[test]
fn parallelism_rejects_non_positive_axes() {
    let bad_tensor = SlurmConfig {
        gpus_per_node: Some(4),
        parallelism: Some(ParallelismConfig {
            tensor: 0,
            pipeline: 1,
        }),
        ..SlurmConfig::default()
    };
    let err = bad_tensor.validate().expect_err("tensor must be >= 1");
    assert!(err.to_string().contains("tensor"));
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.code().is_some_and(|c| {
            c.to_string()
                .contains("hpc_compose::spec::parallelism_non_positive")
        })
    }));

    let bad_pipeline = SlurmConfig {
        parallelism: Some(ParallelismConfig {
            tensor: 1,
            pipeline: 0,
        }),
        ..SlurmConfig::default()
    };
    let err = bad_pipeline.validate().expect_err("pipeline must be >= 1");
    assert!(err.to_string().contains("pipeline"));
}

#[test]
fn parallelism_cross_check_does_not_overflow_u32() {
    // tensor * pipeline overflows u32 (would panic on u32 mul) but is fine in u64.
    let config = SlurmConfig {
        nodes: Some(1),
        gpus_per_node: Some(1),
        parallelism: Some(ParallelismConfig {
            tensor: u32::MAX,
            pipeline: u32::MAX,
        }),
        ..SlurmConfig::default()
    };
    // Must not panic; product != expected so it reports a mismatch.
    let err = config.validate().expect_err("huge product mismatch");
    assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
        se.code().is_some_and(|c| {
            c.to_string()
                .contains("hpc_compose::spec::parallelism_gpu_mismatch")
        })
    }));
}

#[test]
fn slurm_signal_name_accepts_string_and_numeric_forms() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let by_name = ComposeSpec::load(&write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  signal:
    name: USR1
    at_seconds: 60
services:
  app:
    image: redis:7
"#,
    ))
    .expect("named signal");
    let by_number = ComposeSpec::load(&write_spec(
        tmpdir.path(),
        r#"
x-slurm:
  signal:
    name: 10
    at_seconds: 60
services:
  app:
    image: redis:7
"#,
    ))
    .expect("numeric signal");
    let name_signal = by_name.slurm.signal.clone().expect("named present");
    let number_signal = by_number.slurm.signal.clone().expect("numeric present");
    assert_eq!(name_signal.name, SignalName::Usr1);
    assert_eq!(name_signal.name, number_signal.name);
    // Both spellings render the identical directive value, defaulting to step.
    assert_eq!(name_signal.shell, SignalShellTarget::Step);
    assert_eq!(
        by_name.slurm.signal_directive_value().as_deref(),
        Some("USR1@60")
    );
    assert_eq!(
        by_number.slurm.signal_directive_value().as_deref(),
        Some("USR1@60")
    );
}

#[test]
fn slurm_signal_name_rejects_unknown_value() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for bad in ["BOGUS", "99"] {
        let body = format!(
            r#"
x-slurm:
  signal:
    name: {bad}
    at_seconds: 60
services:
  app:
    image: redis:7
"#
        );
        let err = ComposeSpec::load(&write_spec(tmpdir.path(), &body))
            .expect_err("unknown signal name must be rejected");
        let text = format!("{err:#}");
        assert!(
            text.contains("unsupported x-slurm.signal.name") && text.contains("USR1"),
            "{bad} should surface the whitelist; got {text}"
        );
    }
}

#[test]
fn signal_at_seconds_rejects_zero_and_ceiling() {
    for value in [0_u64, 65_536] {
        let config = SlurmConfig {
            signal: Some(SignalConfig {
                name: SignalName::Usr1,
                at_seconds: value,
                shell: SignalShellTarget::Step,
            }),
            ..SlurmConfig::default()
        };
        let err = config.validate().expect_err("at_seconds out of range");
        assert!(err.to_string().contains("between 1 and 65535"), "{err:#}");
        assert!(err.downcast_ref::<SpecError>().is_some_and(|se| {
            se.code().is_some_and(|c| {
                c.to_string()
                    .contains("hpc_compose::spec::signal_delay_out_of_range")
            })
        }));
    }
}

#[test]
fn slurm_requeue_and_signal_reject_raw_submit_arg_conflicts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "requeue plus raw --requeue",
            r#"
x-slurm:
  requeue: true
  submit_args:
    - "--requeue"
services:
  app:
    image: redis:7
"#,
            "x-slurm.requeue cannot be combined with raw --requeue",
        ),
        (
            "requeue false plus raw --no-requeue",
            r#"
x-slurm:
  requeue: false
  submit_args:
    - "--no-requeue"
services:
  app:
    image: redis:7
"#,
            "x-slurm.requeue cannot be combined with raw --no-requeue",
        ),
        (
            "signal plus raw --signal",
            r#"
x-slurm:
  signal:
    name: USR1
    at_seconds: 60
  submit_args:
    - "--signal=USR1@60"
services:
  app:
    image: redis:7
"#,
            "x-slurm.signal cannot be combined with raw --signal",
        ),
    ];

    for (label, body, expected) in cases {
        let path = write_spec(tmpdir.path(), body);
        let err = ComposeSpec::load(&path).expect_err(label);
        let text = format!("{err:#}");
        assert!(
            text.contains(expected),
            "{label} should mention {expected}; got {text}"
        );
    }
}

/// `ComposeSpec::validate` normalizes services (script/command promotion,
/// healthcheck-into-readiness) as well as validating them. Because the planner
/// now re-runs it as an enforcement chokepoint after `load` already ran it once
/// on the CLI path, validation+normalization MUST be idempotent: a second pass
/// over an already-normalized spec must change nothing and error nothing.
///
/// This exercises every normalize step at once: a `script`, a multi-line
/// `command` string with a string `entrypoint` (the array-promotion path), a
/// healthcheck consumed into readiness, and a runtime prepare hook.
#[test]
fn validate_is_idempotent_across_repeated_passes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  worker:
    image: redis:7
    script: |
      echo starting
      exec worker
    x-runtime:
      prepare:
        commands:
          - echo prep
  api:
    image: python:3.11
    entrypoint: "python -u"
    command: "app.py\n--serve"
    healthcheck:
      test:
        - CMD-SHELL
        - curl --silent --fail http://127.0.0.1:8080/health
      timeout: 2m
"#,
    );
    // `load_raw_spec` parses without running the semantic validate/normalize, so
    // the first `validate` call below performs the real normalization work.
    let mut spec = load_raw_spec(&path).expect("parse raw spec");

    spec.validate()
        .expect("first validate/normalize must succeed");
    let after_first = format!("{spec:?}");

    // Sanity: normalization actually happened on the first pass, so the second
    // pass is genuinely re-running over already-normalized state.
    let worker = spec.services.get("worker").expect("worker");
    assert!(worker.script.is_none(), "script must be consumed");
    assert!(
        matches!(worker.command, Some(CommandSpec::Vec(_))),
        "script must be promoted to a /bin/sh -lc array"
    );
    let api = spec.services.get("api").expect("api");
    assert!(api.healthcheck.is_none(), "healthcheck must be consumed");
    assert!(api.readiness.is_some(), "healthcheck must become readiness");
    assert!(
        matches!(api.command, Some(CommandSpec::Vec(_))) && api.entrypoint.is_none(),
        "multi-line command+entrypoint must be promoted to a single array"
    );

    spec.validate()
        .expect("second validate/normalize must also succeed (idempotent)");
    let after_second = format!("{spec:?}");

    assert_eq!(
        after_first, after_second,
        "re-running validate/normalize must be a no-op"
    );
}

/// The planner is the enforcement chokepoint: a `ComposeSpec` built by any route
/// other than `load` (here, a direct serde deserialize that skips the semantic
/// `validate`) must still be fully validated when it reaches `build_plan`. This
/// pins that a spec `load` would reject is rejected by the planner too, with the
/// same diagnostic.
#[test]
fn build_plan_enforces_full_spec_validation() {
    // Both x-runtime.prepare and x-enroot.prepare set: `load` rejects this with
    // DuplicatePrepareHook. serde deserializes it fine, bypassing validation.
    let yaml = r#"
services:
  app:
    image: redis:7
    x-runtime:
      prepare:
        commands: ["echo runtime"]
    x-enroot:
      prepare:
        commands: ["echo enroot"]
"#;
    let spec: ComposeSpec = serde_norway::from_str(yaml).expect("deserialize spec");

    let err = crate::planner::build_plan(Path::new("."), spec)
        .expect_err("planner must reject the duplicate prepare hook");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("prepare"),
        "planner should surface the duplicate-prepare-hook diagnostic, got: {rendered}"
    );
    assert!(
        err.downcast_ref::<SpecError>()
            .is_some_and(|se| matches!(se, SpecError::DuplicatePrepareHook { .. })),
        "expected DuplicatePrepareHook, got: {rendered}"
    );
}

// --- F1: x-slurm.time walltime format guarding ---

#[test]
fn slurm_time_accepts_full_sbatch_grammar() {
    // Every documented sbatch --time shape, including edge-of-grammar values.
    for value in [
        "90",         // minutes
        "0",          // zero minutes (Slurm treats --time=0 as unlimited)
        "90:00",      // MM:SS
        "1:00:00",    // HH:MM:SS
        "100:00:00",  // HH unbounded in the leading slot
        "1-00",       // D-HH
        "1-23",       // D-HH at the hour bound
        "1-00:30",    // D-HH:MM
        "1-00:30:00", // D-HH:MM:SS
    ] {
        let config = SlurmConfig {
            time: Some(value.to_string()),
            ..SlurmConfig::default()
        };
        assert!(
            config.validate().is_ok(),
            "time '{value}' should be accepted",
        );
    }
}

#[test]
fn slurm_time_rejects_bare_unit_suffixes_with_actionable_help() {
    for value in ["1h", "30m", "2d", "1hour"] {
        let config = SlurmConfig {
            time: Some(value.to_string()),
            ..SlurmConfig::default()
        };
        let err = config
            .validate()
            .expect_err("bare-unit time must be rejected");
        let se = err
            .downcast_ref::<SpecError>()
            .expect("time rejection should be a SpecError");
        assert!(
            se.code()
                .is_some_and(|c| c.to_string() == "hpc_compose::spec::invalid_slurm_time"),
            "time '{value}' should use the invalid_slurm_time code",
        );
        assert!(
            se.help().is_some_and(|h| h.to_string().contains("1:00:00")),
            "help should show the 1h -> 1:00:00 fix",
        );
    }
}

// --- F2: service volume mount-syntax guarding ---

#[test]
fn service_volumes_are_mount_syntax_validated() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // Positive: valid host:container[:ro|rw] forms, including a relative host
    // path (resolved at plan time) and both modes.
    let ok = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: redis:7
    volumes:
      - ./data:/workspace/data
      - /scratch/models:/models:ro
      - /out:/out:rw
"#,
    );
    assert!(ComposeSpec::load(&ok).is_ok(), "valid volumes should load");

    // Negative cases, each with the distinct rejection reason.
    let cases = [
        ("- /only-host", "must use host_path:container_path"),
        (
            "- ./data:relative/container",
            "container path must be absolute",
        ),
        ("- ./data:/mnt:cached", "unsupported mode 'cached'"),
        ("- ':/mnt'", "host path must not be empty"),
    ];
    for (entry, expected) in cases {
        let body = format!("services:\n  app:\n    image: redis:7\n    volumes:\n      {entry}\n");
        let path = write_spec(tmpdir.path(), &body);
        let err = ComposeSpec::load(&path).expect_err("bad volume must be rejected");
        let se = err
            .downcast_ref::<SpecError>()
            .expect("volume rejection should be a SpecError");
        assert!(
            se.code()
                .is_some_and(|c| c.to_string() == "hpc_compose::spec::invalid_mount_syntax"),
            "volume '{entry}' should use the invalid_mount_syntax code",
        );
        assert!(
            se.to_string().contains(expected),
            "volume '{entry}' should mention '{expected}'; got {se}",
        );
    }
}

// --- F3: contradictory gpus + gres guard ---

#[test]
fn gpus_and_gpu_gres_together_are_rejected() {
    let config = SlurmConfig {
        gpus: Some(2),
        gres: Some("gpu:a100:2".to_string()),
        ..SlurmConfig::default()
    };
    let err = config
        .validate()
        .expect_err("gpus + gpu gres is contradictory");
    assert!(
        err.downcast_ref::<SpecError>()
            .is_some_and(|se| matches!(se, SpecError::GpusGresConflict { .. })),
        "expected GpusGresConflict",
    );

    // Per-service scope is guarded too.
    let service = ServiceSlurmConfig {
        gpus: Some(1),
        gres: Some("gpu:1".to_string()),
        ..ServiceSlurmConfig::default()
    };
    let err = service
        .validate("trainer")
        .expect_err("service gpus + gpu gres is contradictory");
    assert!(err.to_string().contains("service 'trainer' x-slurm"));
}

#[test]
fn gpus_with_non_gpu_gres_is_allowed() {
    // A non-GPU gres (e.g. a licensed feature) does not conflict with gpus.
    let config = SlurmConfig {
        gpus: Some(2),
        gres: Some("bandwidth:lustre:100".to_string()),
        ..SlurmConfig::default()
    };
    assert!(
        config.validate().is_ok(),
        "gpus with a non-gpu gres should be allowed",
    );
}

// --- F6: submit_args / extra_srun_args conflict coverage for newly-audited flags ---

#[test]
fn submit_args_conflict_covers_mem_time_partition_qos_and_short_forms() {
    let cases = [
        SlurmConfig {
            mem: Some("4G".into()),
            submit_args: vec!["--mem=8G".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            time: Some("2:00:00".into()),
            submit_args: vec!["--time=1:00:00".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            partition: Some("cpu".into()),
            submit_args: vec!["--partition=gpu".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            qos: Some("low".into()),
            submit_args: vec!["--qos=high".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            reservation: Some("maint_2026".into()),
            submit_args: vec!["--reservation=debug".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            licenses: Some("ansys:2".into()),
            submit_args: vec!["--licenses=comsol:1".into()],
            ..SlurmConfig::default()
        },
        // Short forms are matched too.
        SlurmConfig {
            time: Some("2:00:00".into()),
            submit_args: vec!["-t 1:00:00".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            partition: Some("cpu".into()),
            submit_args: vec!["-p gpu".into()],
            ..SlurmConfig::default()
        },
        SlurmConfig {
            licenses: Some("ansys:2".into()),
            submit_args: vec!["-L comsol:1".into()],
            ..SlurmConfig::default()
        },
    ];
    for config in cases {
        let raw = config.submit_args[0].clone();
        let err = config
            .validate()
            .expect_err("first-class field plus raw submit arg must conflict");
        assert!(
            err.to_string().contains("cannot be combined with raw"),
            "raw '{raw}' should conflict; got {err}",
        );
    }
}

#[test]
fn service_extra_srun_args_conflict_covers_nodes_ntasks_gres() {
    let cases = [
        ServiceSlurmConfig {
            nodes: Some(1),
            extra_srun_args: vec!["--nodes=2".into()],
            ..ServiceSlurmConfig::default()
        },
        ServiceSlurmConfig {
            ntasks: Some(2),
            extra_srun_args: vec!["--ntasks=4".into()],
            ..ServiceSlurmConfig::default()
        },
        ServiceSlurmConfig {
            gres: Some("gpu:2".into()),
            extra_srun_args: vec!["--gres=gpu:1".into()],
            ..ServiceSlurmConfig::default()
        },
        ServiceSlurmConfig {
            cpus_per_task: Some(2),
            extra_srun_args: vec!["-c 4".into()],
            ..ServiceSlurmConfig::default()
        },
    ];
    for config in cases {
        let raw = config.extra_srun_args[0].clone();
        let err = config
            .validate("worker")
            .expect_err("service first-class field plus raw srun arg must conflict");
        assert!(
            err.to_string().contains("cannot be combined with raw"),
            "raw '{raw}' should conflict; got {err}",
        );
    }
}

#[test]
fn env_file_string_form_merges_into_environment() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("service.env"),
        "# base config\nexport BASE_ONLY=from-base\nSHARED='shared value'\n",
    )
    .expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    // env_file is resolved away into `environment` at load time.
    assert!(app.env_file.is_none());
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![
            ("BASE_ONLY".to_string(), "from-base".to_string()),
            ("SHARED".to_string(), "shared value".to_string()),
        ]
    );
}

#[test]
fn env_file_redacts_like_inline_environment_not_in_bulk() {
    // env_file entries become `environment` pairs, so they redact exactly like
    // inline `environment:`: a sensitive-looking key is masked by name while a
    // benign value is shown verbatim. This locks the policy that env_file values
    // are NOT redacted in bulk just because they were externalized.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("service.env"),
        "API_TOKEN=abc12345\nLOG_LEVEL=info\n",
    )
    .expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    let merged: BTreeMap<String, String> = app
        .environment
        .to_pairs()
        .expect("env")
        .into_iter()
        .collect();
    // No declared `secrets:` values, so only name-based redaction applies.
    let redacted = crate::redaction::redact_env_map(&merged, &BTreeSet::new(), false);
    assert_eq!(redacted["API_TOKEN"], "<redacted>");
    assert_eq!(redacted["LOG_LEVEL"], "info");
}

#[test]
fn env_file_list_form_later_file_wins() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("base.env"),
        "BASE_ONLY=from-base\nOVERRIDDEN=from-base\n",
    )
    .expect("base env");
    fs::write(
        tmpdir.path().join("override.env"),
        "OVERRIDDEN=from-override\nOVERRIDE_ONLY=yes\n",
    )
    .expect("override env");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: [base.env, override.env]
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![
            ("BASE_ONLY".to_string(), "from-base".to_string()),
            ("OVERRIDDEN".to_string(), "from-override".to_string()),
            ("OVERRIDE_ONLY".to_string(), "yes".to_string()),
        ]
    );
}

#[test]
fn env_file_inline_environment_overrides_env_file() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("service.env"),
        "OVERRIDDEN=from-env-file\nENV_FILE_ONLY=kept\n",
    )
    .expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
    environment:
      OVERRIDDEN: from-inline
      INLINE_ONLY: yes
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![
            ("ENV_FILE_ONLY".to_string(), "kept".to_string()),
            ("INLINE_ONLY".to_string(), "yes".to_string()),
            ("OVERRIDDEN".to_string(), "from-inline".to_string()),
        ]
    );
}

#[test]
fn env_file_values_are_literal_not_interpolated() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // The dollar-sign expression must survive verbatim; if env_file contents
    // were interpolated, `${BAR}` (BAR is unset) would either error or expand.
    fs::write(tmpdir.path().join("service.env"), "FOO=${BAR}\n").expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![("FOO".to_string(), "${BAR}".to_string())]
    );
}

#[test]
fn env_file_path_is_interpolated_but_contents_are_not() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let config_dir = tmpdir.path().join("config");
    fs::create_dir(&config_dir).expect("config dir");
    fs::write(config_dir.join("prod.env"), "STAGE_VALUE=${NOT_EXPANDED}\n").expect("stage env");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: config/${STAGE}.env
"#,
    );
    // Provide STAGE explicitly (no process-env or .env dependency).
    let vars = BTreeMap::from([("STAGE".to_string(), "prod".to_string())]);
    let spec = ComposeSpec::load_with_interpolation_vars(&path, &vars).expect("load");
    let app = spec.services.get("app").expect("app");
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![("STAGE_VALUE".to_string(), "${NOT_EXPANDED}".to_string())]
    );
}

#[test]
fn env_file_missing_file_reports_spec_error_with_submit_host_help() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: does-not-exist.env
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("missing env_file");
    assert!(
        err.downcast_ref::<SpecError>()
            .is_some_and(|se| matches!(se, SpecError::EnvFileNotFound { .. })),
        "{err:#}"
    );
    assert!(err.to_string().contains("does-not-exist.env"), "{err}");
    let help = err
        .downcast_ref::<SpecError>()
        .and_then(Diagnostic::help)
        .map(|h| h.to_string())
        .unwrap_or_default();
    assert!(help.contains("submit host"), "help was: {help}");
}

#[test]
fn env_file_malformed_line_reports_spec_error_with_help() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(tmpdir.path().join("service.env"), "BROKEN\n").expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("malformed env_file");
    assert!(
        err.downcast_ref::<SpecError>()
            .is_some_and(|se| matches!(se, SpecError::EnvFileMalformedLine { line: 1, .. })),
        "{err:#}"
    );
    assert!(err.to_string().contains("line 1"), "{err}");
    let help = err
        .downcast_ref::<SpecError>()
        .and_then(Diagnostic::help)
        .map(|h| h.to_string())
        .unwrap_or_default();
    assert!(help.contains("KEY=VALUE"), "help was: {help}");
}

#[test]
fn env_file_paths_resolve_relative_to_compose_directory() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let project_dir = tmpdir.path().join("project");
    fs::create_dir(&project_dir).expect("project dir");
    fs::write(project_dir.join("service.env"), "FROM_PROJECT=yes\n").expect("env file");
    let path = write_spec(
        &project_dir,
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    // Load by absolute path; the env_file resolves next to the compose file,
    // not next to the process working directory.
    let spec = ComposeSpec::load(&path).expect("load");
    let app = spec.services.get("app").expect("app");
    assert_eq!(
        app.environment.to_pairs().expect("env"),
        vec![("FROM_PROJECT".to_string(), "yes".to_string())]
    );
}

#[test]
fn env_file_rejects_unsafe_variable_names_same_as_inline() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // A leading digit is rejected by validate_safe_env_name; this only fires if
    // env_file entries are merged BEFORE name validation runs.
    fs::write(tmpdir.path().join("service.env"), "1BAD=x\n").expect("env file");
    let path = write_spec(
        tmpdir.path(),
        r#"
services:
  app:
    image: alpine:latest
    env_file: service.env
"#,
    );
    let err = ComposeSpec::load(&path).expect_err("unsafe env name");
    assert!(
        err.to_string()
            .contains("is not a safe environment variable name"),
        "{err}"
    );
}
