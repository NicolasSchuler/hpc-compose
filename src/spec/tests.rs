use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::sync::{Mutex, OnceLock};

use miette::Diagnostic;
use proptest::prelude::*;
use proptest::string::string_regex;

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
            "uses unsupported mode 'cached'",
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
        submit_args: vec![
            "--reservation=ok".to_string(),
            "--comment=bad\narg".to_string(),
        ],
        ..SlurmConfig::default()
    };
    let err = config.validate().expect_err("line break in submit arg");
    assert!(err.to_string().contains("x-slurm.submit_args[1]"));
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
