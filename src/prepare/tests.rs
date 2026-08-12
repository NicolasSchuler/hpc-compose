use std::cell::RefCell;
use std::env;
use std::fs;
use std::io::{BufReader, Cursor};
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::rc::Rc;
use std::thread::{self, ThreadId};

use super::*;
use crate::planner::{
    ExecutionSpec, ImageSource, Plan, PlannedService, PreparedImageSpec, ServicePlacement,
};
use crate::runtime_plan::{image_label, prepared_image_cache_key_from_plan};
use crate::spec::{
    ReadinessSpec, RuntimeConfig, ServiceDependency, ServiceFailurePolicy, ServiceSlurmConfig,
    SlurmConfig,
};
use crate::test_support::env_lock;

fn fake_service(tmpdir: &Path) -> RuntimeService {
    RuntimeService {
        name: "svc".into(),
        runtime_image: tmpdir.join("prepared/svc.sqsh"),
        execution: ExecutionSpec::Shell("echo ready".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: Some(PreparedImageSpec {
            commands: vec!["echo setup".into()],
            mounts: Vec::new(),
            env: vec![("A".into(), "B".into())],
            root: true,
            force_rebuild: false,
        }),
        source: ImageSource::Remote("docker://redis:7".into()),
    }
}

#[test]
fn runtime_plan_conversion_preserves_planned_service_contract() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let prepare = PreparedImageSpec {
        commands: vec!["echo setup".into()],
        mounts: vec!["/host/input:/input:ro".into()],
        env: vec![("A".into(), "B".into())],
        root: true,
        force_rebuild: true,
    };
    let planned = PlannedService {
        name: "app".into(),
        image: ImageSource::Remote("docker://python:3.11-slim".into()),
        execution: ExecutionSpec::Exec(vec!["python".into(), "-m".into(), "app".into()]),
        environment: vec![("ENV".into(), "prod".into())],
        volumes: vec!["/host/app:/app".into()],
        working_dir: Some("/app".into()),
        depends_on: vec![ServiceDependency {
            name: "db".into(),
            condition: crate::spec::DependencyCondition::ServiceStarted,
            implicit: false,
        }],
        readiness: Some(ReadinessSpec::Sleep { seconds: 1 }),
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement {
            nodes: 2,
            ntasks: Some(4),
            node_indices: Some(vec![0, 1]),
            ..ServicePlacement::default()
        },
        slurm: ServiceSlurmConfig {
            cpus_per_task: Some(2),
            ..ServiceSlurmConfig::default()
        },
        prepare: Some(prepare),
    };
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: tmpdir.path().join("compose.yaml"),
        runtime: RuntimeConfig::default(),
        cache_dir: tmpdir.path().join("cache"),
        slurm: SlurmConfig {
            time: Some("00:10:00".into()),
            ..SlurmConfig::default()
        },
        ordered_services: vec![planned.clone()],
    };

    let runtime_plan = build_runtime_plan(&plan);
    assert_eq!(runtime_plan.name, plan.name);
    assert_eq!(runtime_plan.cache_dir, plan.cache_dir);
    assert_eq!(runtime_plan.runtime.backend, plan.runtime.backend);
    assert_eq!(runtime_plan.slurm.time, plan.slurm.time);
    let runtime = runtime_plan.ordered_services.first().expect("service");
    assert_eq!(runtime.name, planned.name);
    assert_eq!(runtime.execution, planned.execution);
    assert_eq!(runtime.environment, planned.environment);
    assert_eq!(runtime.volumes, planned.volumes);
    assert_eq!(runtime.working_dir, planned.working_dir);
    assert_eq!(runtime.depends_on, planned.depends_on);
    assert_eq!(runtime.readiness, planned.readiness);
    assert_eq!(runtime.failure_policy, planned.failure_policy);
    assert_eq!(runtime.placement, planned.placement);
    assert_eq!(runtime.slurm.cpus_per_task, planned.slurm.cpus_per_task);
    assert_eq!(runtime.prepare, planned.prepare);
    assert_eq!(runtime.source, planned.image);
    assert!(
        runtime
            .runtime_image
            .starts_with(plan.cache_dir.join("prepared"))
    );
}

fn write_fake_enroot(tmpdir: &Path, log_path: &Path) -> PathBuf {
    write_fake_enroot_with_export_body(tmpdir, log_path, "touch \"$output\"")
}

fn write_fake_enroot_with_export_body(
    tmpdir: &Path,
    log_path: &Path,
    export_body: &str,
) -> PathBuf {
    let script = tmpdir.join("fake-enroot.sh");
    let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="$1"
shift
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name)
          name="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          image="$1"
          shift
          ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      exit 41
    fi
    ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output|--output=*)
          if [[ "$1" == *=* ]]; then
            output="${1#*=}"
            shift
          else
            output="$2"
            shift 2
          fi
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    __EXPORT_BODY__
    ;;
  remove)
    while (($#)); do
      case "$1" in
        -f|--force)
          shift
          ;;
        *)
          rm -rf "$ENROOT_DATA_PATH/$1"
          shift
          ;;
      esac
    done
    ;;
esac
"#;
    let content = template
        .replace(
            "__LOG_PATH__",
            &shell_quote_for_test(&log_path.display().to_string()),
        )
        .replace("__EXPORT_BODY__", export_body);
    fs::write(&script, content).expect("write fake enroot");
    let mut perms = fs::metadata(&script).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn write_fake_import_failure(
    tmpdir: &Path,
    counter_path: &Path,
    marker: &str,
    succeeds_on_retry: bool,
) -> PathBuf {
    let script = tmpdir.join("fake-importer.sh");
    let template = r#"#!/bin/bash
set -euo pipefail
count=0
if [[ -f __COUNTER__ ]]; then
  count="$(cat __COUNTER__)"
fi
count=$((count + 1))
printf '%s\n' "$count" > __COUNTER__

output=""
while (($#)); do
  case "$1" in
    -o|--output)
      output="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [[ __SUCCEEDS_ON_RETRY__ == 1 && "$count" -ge 2 ]]; then
  mkdir -p "$(dirname "$output")"
  printf 'complete' > "$output"
  exit 0
fi

printf '%s\n' __MARKER__ >&2
for ((i=0; i<2500; i++)); do
  printf 'filler-%05d-abcdefghijklmnopqrstuvwxyz0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZ\n' "$i" >&2
done
exit 41
"#;
    let content = template
        .replace(
            "__COUNTER__",
            &shell_quote_for_test(&counter_path.display().to_string()),
        )
        .replace("__MARKER__", &shell_quote_for_test(marker))
        .replace(
            "__SUCCEEDS_ON_RETRY__",
            if succeeds_on_retry { "1" } else { "0" },
        );
    fs::write(&script, content).expect("write fake importer");
    let mut perms = fs::metadata(&script).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn write_fake_sif_runtime(tmpdir: &Path, log_path: &Path) -> PathBuf {
    let script = tmpdir.join("fake-sif-runtime.sh");
    let template = r#"#!/bin/bash
set -euo pipefail
echo "$@" >> __LOG_PATH__
cmd="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi
case "$cmd" in
  build)
    sandbox=0
    target=""
    while (($#)); do
      case "$1" in
        --sandbox)
          sandbox=1
          shift
          ;;
        --force|--fakeroot)
          shift
          ;;
        *)
          target="$1"
          break
          ;;
      esac
    done
    if [[ -z "$target" ]]; then
      echo "missing build target" >&2
      exit 64
    fi
    if (( sandbox )); then
      mkdir -p "$target"
    else
      mkdir -p "$(dirname "$target")"
      touch "$target"
    fi
    ;;
  exec)
    if printf '%s\n' "$@" | grep -q "fail-me"; then
      echo "prepare failed" >&2
      exit 41
    fi
    ;;
esac
"#;
    let content = template.replace(
        "__LOG_PATH__",
        &shell_quote_for_test(&log_path.display().to_string()),
    );
    fs::write(&script, content).expect("write fake sif runtime");
    let mut perms = fs::metadata(&script).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn shell_quote_for_test(value: &str) -> String {
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedPrepareEvent {
    Started {
        service: String,
        phase: String,
        thread: ThreadId,
    },
    Output {
        service: String,
        line: String,
        thread: ThreadId,
    },
    Bytes {
        service: String,
        bytes: u64,
        thread: ThreadId,
    },
}

#[derive(Debug, Clone, Default)]
struct RecordingReporter {
    events: Rc<RefCell<Vec<RecordedPrepareEvent>>>,
}

impl RecordingReporter {
    fn events(&self) -> Vec<RecordedPrepareEvent> {
        self.events.borrow().clone()
    }
}

impl PrepareReporter for RecordingReporter {
    fn step_started(&self, service: &str, phase: &str) {
        self.events
            .borrow_mut()
            .push(RecordedPrepareEvent::Started {
                service: service.to_string(),
                phase: phase.to_string(),
                thread: thread::current().id(),
            });
    }

    fn step_output(&self, service: &str, line: &str) {
        self.events.borrow_mut().push(RecordedPrepareEvent::Output {
            service: service.to_string(),
            line: line.to_string(),
            thread: thread::current().id(),
        });
    }

    fn step_bytes(&self, service: &str, bytes: u64) {
        self.events.borrow_mut().push(RecordedPrepareEvent::Bytes {
            service: service.to_string(),
            bytes,
            thread: thread::current().id(),
        });
    }
}

fn stream_test_command(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", script]);
    command
}

fn assert_runtime_artifact_status(
    summary: &PrepareSummary,
    expected_action: ArtifactAction,
    expected_note: Option<&str>,
) {
    let status = &summary
        .services
        .first()
        .expect("prepared service")
        .runtime_image;
    assert_eq!(status.action, expected_action);
    assert_eq!(status.note.as_deref(), expected_note);
}

fn fake_sif_prepared_plan(root: &Path, forced_by_mounts: bool) -> RuntimePlan {
    fs::create_dir_all(root).expect("sif fixture root");
    let local_sif = root.join("base.sif");
    fs::write(&local_sif, "sif").expect("local sif");
    RuntimePlan {
        name: "demo".into(),
        cache_dir: root.join("cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "svc".into(),
            runtime_image: root.join("cache/prepared/svc.sif"),
            execution: ExecutionSpec::Shell("echo ready".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: Some(PreparedImageSpec {
                commands: vec!["echo setup".into()],
                mounts: forced_by_mounts
                    .then(|| "/host:/mnt".to_string())
                    .into_iter()
                    .collect(),
                env: Vec::new(),
                root: true,
                force_rebuild: forced_by_mounts,
            }),
            source: ImageSource::LocalSif(local_sif),
        }],
    }
}

#[test]
fn prepare_pipeline_imports_and_exports() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![fake_service(tmpdir.path())],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    let summary = prepare_runtime_plan(&plan, &options).expect("prepare");
    assert!(plan.ordered_services[0].runtime_image.exists());
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("import"));
    assert!(log_content.contains("create --force --name"));
    assert!(log_content.contains("export --force --output"));
    assert!(crate::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists());
}

#[test]
fn cached_prepared_image_skips_rebuild_without_mounts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect("prepare once");
    fs::write(&log, "").expect("clear log");
    fs::write(&runtime_image, "cached").expect("seed");
    prepare_runtime_plan(&plan, &options).expect("prepare twice");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(!log_content.contains("create --force"));
}

#[test]
fn failed_export_target_is_not_reused_without_committed_manifest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let first_attempt = tmpdir.path().join("first-export-attempted");
    let export_body = format!(
        r#"if [[ ! -e {attempt} ]]; then
      touch {attempt}
      printf 'partial' > "$output"
      exit 41
    fi
    printf 'complete' > "$output""#,
        attempt = shell_quote_for_test(&first_attempt.display().to_string()),
    );
    let fake = write_fake_enroot_with_export_body(tmpdir.path(), &log, &export_body);
    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect_err("first export fails after writing output");
    let second = prepare_runtime_plan(&plan, &options).expect("second prepare rebuilds");

    assert_eq!(
        second.services[0].runtime_image.action,
        ArtifactAction::Built,
        "an artifact without its matching manifest is not committed cache state"
    );
    assert_eq!(
        fs::read_to_string(&runtime_image).expect("committed runtime image"),
        "complete"
    );
    let manifest = crate::cache::read_manifest(&runtime_image).expect("committed manifest");
    assert_eq!(manifest.kind, crate::cache::CacheEntryKind::Prepared);
}

#[test]
fn concurrent_same_key_prepares_wait_for_one_committed_artifact() {
    use std::sync::Arc;
    use std::sync::mpsc::TryRecvError;
    use std::thread;
    use std::time::{Duration, Instant};

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let export_started = tmpdir.path().join("export-started");
    let export_calls = tmpdir.path().join("export-calls");
    let release_export = tmpdir.path().join("release-export");
    let export_body = format!(
        r#"printf 'start\n' >> {calls}
    printf 'partial' > "$output"
    touch {started}
    while [[ ! -e {release} ]]; do sleep 0.02; done
    printf 'complete' > "$output""#,
        calls = shell_quote_for_test(&export_calls.display().to_string()),
        started = shell_quote_for_test(&export_started.display().to_string()),
        release = shell_quote_for_test(&release_export.display().to_string()),
    );
    let fake = write_fake_enroot_with_export_body(tmpdir.path(), &log, &export_body);
    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = Arc::new(RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    });
    let options = Arc::new(PrepareOptions {
        enroot_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    });
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();

    let first_plan = Arc::clone(&plan);
    let first_options = Arc::clone(&options);
    let first = thread::spawn(move || prepare_runtime_plan(&first_plan, &first_options));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !export_started.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(export_started.exists(), "first export did not start");

    let second_plan = Arc::clone(&plan);
    let second_options = Arc::clone(&options);
    let second = thread::spawn(move || {
        let result = prepare_runtime_plan(&second_plan, &second_options);
        let _ = finished_tx.send(());
        result
    });
    thread::sleep(Duration::from_millis(250));
    let second_waited = matches!(finished_rx.try_recv(), Err(TryRecvError::Empty));
    let export_count_before_release = fs::read_to_string(&export_calls)
        .expect("export calls")
        .lines()
        .count();

    fs::write(&release_export, "go").expect("release export");
    let first_result = first.join().expect("first thread").expect("first prepare");
    let second_result = second
        .join()
        .expect("second thread")
        .expect("second prepare");

    assert!(second_waited, "a same-key prepare observed in-flight state");
    assert_eq!(
        export_count_before_release, 1,
        "only the lock holder may build before the artifact is committed"
    );
    assert_eq!(
        first_result.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_eq!(
        second_result.services[0].runtime_image.action,
        ArtifactAction::Reused
    );
    assert_eq!(
        fs::read_to_string(runtime_image).expect("runtime image"),
        "complete"
    );
}

#[test]
fn replacing_local_image_bytes_invalidates_prepared_reuse() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);
    let local_base = tmpdir.path().join("local-base.sqsh");
    fs::write(&local_base, "first image bytes").expect("local base");
    let mut service = fake_service(tmpdir.path());
    service.source = ImageSource::LocalSqsh(local_base.clone());
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect("first prepare");
    let first_key = crate::cache::read_manifest(&plan.ordered_services[0].runtime_image)
        .expect("first manifest")
        .cache_key;
    fs::write(&log, "").expect("clear log");
    fs::write(&local_base, "replacement image bytes").expect("replace local base");

    let second = prepare_runtime_plan(&plan, &options).expect("second prepare");
    let second_key = crate::cache::read_manifest(&plan.ordered_services[0].runtime_image)
        .expect("second manifest")
        .cache_key;
    assert_eq!(
        second.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_ne!(
        first_key, second_key,
        "the source byte identity is part of the key"
    );
    assert!(
        fs::read_to_string(log)
            .expect("log")
            .contains("create --force --name")
    );
}

#[test]
fn failing_prepare_retains_only_a_bounded_stderr_tail() {
    const EXPECTED_STDERR_TAIL_BYTES: usize = 64 * 1024;
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let helper = tmpdir.path().join("chatty-failure.sh");
    fs::write(
        &helper,
        r#"#!/bin/bash
set -euo pipefail
for ((i=0; i<12000; i++)); do
  printf 'diagnostic-%05d-abcdefghijklmnopqrstuvwxyz0123456789\n' "$i" >&2
done
printf 'FINAL-DIAGNOSTIC-MARKER\n' >&2
exit 47
"#,
    )
    .expect("helper");
    let mut perms = fs::metadata(&helper).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&helper, perms).expect("chmod");

    let error = run_enroot(
        helper.to_str().expect("helper path"),
        &[],
        Vec::new(),
        "run chatty failing prepare",
        &StreamCtx::quiet(&NoopPrepareReporter, "test"),
    )
    .expect_err("helper fails");
    let message = error.to_string();
    assert!(message.contains("FINAL-DIAGNOSTIC-MARKER"));
    assert!(
        message.len() <= EXPECTED_STDERR_TAIL_BYTES + 1024,
        "retained diagnostic was {} bytes",
        message.len()
    );
}

#[test]
fn streamed_command_reports_start_before_spawn_and_uses_the_caller_thread() {
    let caller_thread = thread::current().id();
    let spawn_reporter = RecordingReporter::default();
    let missing_bin = "/definitely/missing/hpc-compose-stream-test";
    let spawn_error = run_streamed_command(
        Command::new(missing_bin),
        missing_bin,
        "spawn missing streamed tool",
        &StreamCtx {
            reporter: &spawn_reporter,
            service: "spawn-service",
            phase: "spawn-phase",
            target: None,
        },
    )
    .expect_err("missing command must fail to spawn");
    assert!(spawn_error.to_string().contains("failed to execute"));
    assert_eq!(
        spawn_reporter.events(),
        vec![RecordedPrepareEvent::Started {
            service: "spawn-service".to_string(),
            phase: "spawn-phase".to_string(),
            thread: caller_thread,
        }],
        "the non-Send reporter is called synchronously before spawn"
    );

    let reporter = RecordingReporter::default();
    run_streamed_command(
        stream_test_command(
            "printf 'stdout padded \\t  \\n'; printf '\\n'; printf 'stdout final'; \
             printf 'stderr padded \\t  \\n' >&2; printf '\\n' >&2; printf 'stderr final' >&2",
        ),
        "/bin/sh",
        "record output",
        &StreamCtx {
            reporter: &reporter,
            service: "svc",
            phase: "streaming",
            target: None,
        },
    )
    .expect("streaming command");

    let events = reporter.events();
    assert_eq!(
        events.first(),
        Some(&RecordedPrepareEvent::Started {
            service: "svc".to_string(),
            phase: "streaming".to_string(),
            thread: caller_thread,
        })
    );
    assert!(events.iter().all(|event| {
        let event_thread = match event {
            RecordedPrepareEvent::Started { thread, .. }
            | RecordedPrepareEvent::Output { thread, .. }
            | RecordedPrepareEvent::Bytes { thread, .. } => thread,
        };
        *event_thread == caller_thread
    }));
    let mut output = events
        .iter()
        .filter_map(|event| match event {
            RecordedPrepareEvent::Output { line, .. } => Some(line.clone()),
            RecordedPrepareEvent::Started { .. } | RecordedPrepareEvent::Bytes { .. } => None,
        })
        .collect::<Vec<_>>();
    output.sort();
    assert_eq!(
        output,
        vec![
            "stderr final".to_string(),
            "stderr padded".to_string(),
            "stdout final".to_string(),
            "stdout padded".to_string(),
        ],
        "stdout and stderr are forwarded, trailing whitespace is trimmed, and blank lines are skipped"
    );

    let event_count = events.len();
    run_streamed_command(
        stream_test_command("printf 'quiet stdout\\n'; printf 'quiet stderr\\n' >&2"),
        "/bin/sh",
        "quiet output",
        &StreamCtx::quiet(&reporter, "svc"),
    )
    .expect("quiet command");
    assert_eq!(reporter.events().len(), event_count);
}

#[test]
fn streamed_command_failure_boundary_is_stderr_only_and_fully_drained() {
    let stream = StreamCtx::quiet(&NoopPrepareReporter, "test");
    let error = run_streamed_command(
        stream_test_command(
            "printf 'STDOUT-MUST-NOT-APPEAR\\n'; printf 'first stderr\\n' >&2; \
             (sleep 0.05; printf 'final stderr\\n' >&2) & exit 23",
        ),
        "/bin/sh",
        "exercise streamed failure",
        &stream,
    )
    .expect_err("command must fail");
    assert_eq!(
        error.to_string(),
        "failed to exercise streamed failure: first stderr\nfinal stderr"
    );
    assert!(!error.to_string().contains("STDOUT-MUST-NOT-APPEAR"));

    run_streamed_command(
        stream_test_command("printf 'successful stderr is diagnostic-only\\n' >&2"),
        "/bin/sh",
        "successful stderr",
        &stream,
    )
    .expect("stderr does not make a successful command fail");

    let empty = run_streamed_command(
        stream_test_command("exit 17"),
        "/bin/sh",
        "empty stderr",
        &stream,
    )
    .expect_err("empty stderr failure");
    assert_eq!(empty.to_string(), "failed to empty stderr: ");

    let nonempty = run_streamed_command(
        stream_test_command("printf '  first bytes  \\nlast bytes\\t\\n' >&2; exit 18"),
        "/bin/sh",
        "nonempty stderr",
        &stream,
    )
    .expect_err("nonempty stderr failure");
    assert_eq!(
        nonempty.to_string(),
        "failed to nonempty stderr: first bytes  \nlast bytes"
    );
}

#[test]
fn newline_free_prepare_output_is_split_into_byte_bounded_chunks() {
    use std::io::Cursor;

    const EXPECTED_OUTPUT_CHUNK_BYTES: usize = 16 * 1024;
    let input = vec![b'x'; EXPECTED_OUTPUT_CHUNK_BYTES * 3 + 17];
    let mut chunks = Vec::new();
    for_each_line_lossy(BufReader::new(Cursor::new(&input)), |chunk| {
        chunks.push(chunk);
    });

    assert!(chunks.len() > 1, "an unterminated line must be chunked");
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.len() <= EXPECTED_OUTPUT_CHUNK_BYTES),
        "every queued ASCII chunk must obey the byte bound"
    );
    assert_eq!(chunks.concat().as_bytes(), input);
}

#[test]
fn byte_bounded_output_preserves_utf8_split_at_the_chunk_boundary() {
    use std::io::Cursor;

    const OUTPUT_CHUNK_BYTES: usize = 16 * 1024;
    let mut input = vec![b'x'; OUTPUT_CHUNK_BYTES - 1];
    input.extend_from_slice("€".as_bytes());
    input.push(b'\n');
    let mut chunks = Vec::new();
    for_each_line_lossy(BufReader::new(Cursor::new(&input)), |chunk| {
        chunks.push(chunk);
    });

    let decoded = chunks.concat();
    assert_eq!(decoded.as_bytes(), &input[..input.len() - 1]);
    assert!(!decoded.contains('\u{fffd}'), "valid UTF-8 was corrupted");
}

#[test]
fn byte_bounded_output_preserves_crlf_at_the_chunk_boundary() {
    const OUTPUT_CHUNK_BYTES: usize = 16 * 1024;
    let mut input = vec![b'x'; OUTPUT_CHUNK_BYTES - 1];
    input.extend_from_slice(b"\r\nnext\r\n");
    let mut chunks = Vec::new();
    for_each_line_lossy(BufReader::new(Cursor::new(input)), |chunk| {
        chunks.push(chunk);
    });

    assert_eq!(
        chunks,
        vec!["x".repeat(OUTPUT_CHUNK_BYTES - 1), "next".to_string()]
    );
}

#[test]
fn lossy_decoder_preserves_empty_invalid_and_exact_boundary_contracts() {
    fn decode(input: &[u8]) -> Vec<String> {
        let mut lines = Vec::new();
        for_each_line_lossy(Cursor::new(input), |line| lines.push(line));
        lines
    }

    assert!(decode(&[]).is_empty());
    assert_eq!(decode(b"\n"), vec![String::new()]);
    assert_eq!(decode(b"invalid-\xff-byte\n"), vec!["invalid-�-byte"]);
    assert_eq!(decode(b"final unterminated"), vec!["final unterminated"]);
    assert_eq!(
        decode(b"trailing carriage return\r"),
        vec!["trailing carriage return"]
    );

    const OUTPUT_CHUNK_BYTES: usize = 16 * 1024;
    let exact_cap = vec![b'x'; OUTPUT_CHUNK_BYTES];
    let chunks = decode(&exact_cap);
    assert_eq!(chunks, vec!["x".repeat(OUTPUT_CHUNK_BYTES)]);
}

#[test]
fn failure_markers_are_detected_when_phrases_cross_observe_boundaries() {
    let mut stale_phrase = StreamFailureSignals::default();
    stale_phrase.observe("prefix STALE FI");
    stale_phrase.observe("LE HANDLE suffix");
    assert!(stale_phrase.is_stale_handle());

    let mut read_because = StreamFailureSignals::default();
    read_because.observe("read fai");
    read_because.observe("led bec");
    read_because.observe("ause the filesystem changed");
    assert!(read_because.is_stale_handle());

    let mut squashfs_read = StreamFailureSignals::default();
    squashfs_read.observe("squash");
    squashfs_read.observe("fs writer: read fai");
    squashfs_read.observe("led");
    assert!(squashfs_read.is_stale_handle());

    let mut missing_manifest = StreamFailureSignals::default();
    missing_manifest.observe("mani");
    missing_manifest.observe("fest un");
    missing_manifest.observe("known");
    assert!(missing_manifest.is_missing_image());

    let mut missing_tag = StreamFailureSignals::default();
    missing_tag.observe("mani");
    missing_tag.observe("fest was not fo");
    missing_tag.observe("und");
    assert!(missing_tag.is_missing_image());

    let mut unauthorized = StreamFailureSignals::default();
    unauthorized.observe("status 401 unau");
    unauthorized.observe("thorized");
    assert!(unauthorized.is_missing_image());

    let mut denied = StreamFailureSignals::default();
    denied.observe("access to the res");
    denied.observe("ource is denied");
    assert!(denied.is_missing_image());
}

#[test]
fn streamed_command_reads_verbose_env_per_call_and_preserves_failure_mode() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_verbose = env::var_os(PREPARE_VERBOSE_ENV);
    let reporter = RecordingReporter::default();
    let stream = |phase: &'static str| StreamCtx {
        reporter: &reporter,
        service: "svc",
        phase,
        target: None,
    };

    unsafe { env::remove_var(PREPARE_VERBOSE_ENV) };
    let captured_first = run_streamed_command(
        stream_test_command("printf 'captured first\\n'"),
        "/bin/sh",
        "captured first",
        &stream("captured-first"),
    );
    unsafe { env::set_var(PREPARE_VERBOSE_ENV, "1") };
    let verbose_success = run_streamed_command(
        stream_test_command("printf 'verbose stdout\\n'; printf 'verbose stderr\\n' >&2"),
        "/bin/sh",
        "verbose success",
        &stream("verbose-success"),
    );
    unsafe { env::remove_var(PREPARE_VERBOSE_ENV) };
    let captured_second = run_streamed_command(
        stream_test_command("printf 'captured second\\n'"),
        "/bin/sh",
        "captured second",
        &stream("captured-second"),
    );
    let captured_failure = run_streamed_command(
        stream_test_command("printf 'CAPTURED-TAIL\\n' >&2; exit 19"),
        "/bin/sh",
        "captured failure",
        &stream("captured-failure"),
    );
    unsafe { env::set_var(PREPARE_VERBOSE_ENV, "true") };
    let verbose_failure = run_streamed_command(
        stream_test_command("printf 'VERBOSE-TAIL\\n' >&2; exit 20"),
        "/bin/sh",
        "verbose failure",
        &stream("verbose-failure"),
    );

    match previous_verbose {
        Some(value) => unsafe { env::set_var(PREPARE_VERBOSE_ENV, value) },
        None => unsafe { env::remove_var(PREPARE_VERBOSE_ENV) },
    }

    captured_first.expect("first captured call");
    verbose_success.expect("verbose success");
    captured_second.expect("second captured call");
    assert_eq!(
        captured_failure.expect_err("captured failure").to_string(),
        "failed to captured failure: CAPTURED-TAIL"
    );
    let verbose_error = verbose_failure.expect_err("verbose failure").to_string();
    assert_eq!(
        verbose_error,
        "failed to verbose failure (see the streamed output above)"
    );
    assert!(!verbose_error.contains("VERBOSE-TAIL"));

    let events = reporter.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RecordedPrepareEvent::Started { .. }))
            .count(),
        5
    );
    let mut output = events
        .iter()
        .filter_map(|event| match event {
            RecordedPrepareEvent::Output { line, .. } => Some(line.as_str()),
            RecordedPrepareEvent::Started { .. } | RecordedPrepareEvent::Bytes { .. } => None,
        })
        .collect::<Vec<_>>();
    output.sort_unstable();
    assert_eq!(
        output,
        vec!["CAPTURED-TAIL", "captured first", "captured second"]
    );
}

#[test]
fn stale_handle_marker_survives_tail_eviction_and_triggers_retry() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let counter = tmpdir.path().join("attempts");
    let importer = write_fake_import_failure(
        tmpdir.path(),
        &counter,
        "Read failed because Stale file handle",
        true,
    );
    let target = tmpdir.path().join("base.sqsh");
    let temp_dir = tmpdir.path().join("scratch");

    import_base_image(
        importer.to_str().expect("importer path"),
        &[],
        "docker://example.invalid/image:missing",
        &target,
        &temp_dir,
        "svc",
        &NoopPrepareReporter,
    )
    .expect("early stale marker must trigger one retry");

    assert_eq!(fs::read_to_string(counter).expect("attempt count"), "2\n");
    assert_eq!(
        fs::read_to_string(target).expect("imported image"),
        "complete"
    );
}

#[test]
fn missing_image_marker_survives_tail_eviction_and_adds_remediation() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let counter = tmpdir.path().join("attempts");
    let importer = write_fake_import_failure(
        tmpdir.path(),
        &counter,
        "manifest unknown: manifest not found",
        false,
    );
    let target = tmpdir.path().join("base.sqsh");
    let temp_dir = tmpdir.path().join("scratch");

    let error = import_base_image(
        importer.to_str().expect("importer path"),
        &[],
        "docker://example.invalid/image:missing",
        &target,
        &temp_dir,
        "svc",
        &NoopPrepareReporter,
    )
    .expect_err("import must fail");

    assert!(
        error
            .to_string()
            .contains("the container image could not be pulled"),
        "early registry marker was lost: {error:#}"
    );
    assert_eq!(fs::read_to_string(counter).expect("attempt count"), "1\n");
}

#[test]
fn prepare_mounts_force_rebuild_even_with_existing_image() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").mounts = vec!["/host:/mnt".into()];
    service.prepare.as_mut().expect("prepare").force_rebuild = true;
    fs::create_dir_all(service.runtime_image.parent().expect("parent")).expect("mkdir");
    fs::write(&service.runtime_image, "cached").expect("seed");

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&plan, &options).expect("prepare");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("create --force --name"));
}

#[test]
fn identical_remote_images_share_base_cache_path() {
    let service_a = RuntimeService {
        name: "a".into(),
        runtime_image: PathBuf::from("/tmp/a.sqsh"),
        execution: ExecutionSpec::ImageDefault,
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: ImageSource::Remote("docker://redis:7".into()),
    };
    let service_b = RuntimeService {
        name: "b".into(),
        ..service_a.clone()
    };
    assert_eq!(
        base_image_path(Path::new("/shared/cache"), &service_a),
        base_image_path(Path::new("/shared/cache"), &service_b)
    );
}

#[test]
fn force_rebuild_refreshes_one_shared_remote_base_per_backend() {
    for backend in [RuntimeBackend::Pyxis, RuntimeBackend::Apptainer] {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let root = tmpdir.path().join(backend.as_str());
        fs::create_dir_all(&root).expect("backend root");
        let log = root.join("runtime.log");
        let runtime_bin = match backend {
            RuntimeBackend::Pyxis => write_fake_enroot(&root, &log),
            RuntimeBackend::Apptainer => write_fake_sif_runtime(&root, &log),
            RuntimeBackend::Singularity | RuntimeBackend::Host => unreachable!("test backend"),
        };
        let compose = root.join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let remote_service = |name: &str| PlannedService {
            name: name.to_string(),
            image: ImageSource::Remote("docker://example.com/shared:1".into()),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
        };
        let runtime_plan = build_runtime_plan(&Plan {
            name: "demo".into(),
            project_dir: root.clone(),
            spec_path: compose,
            cache_dir: root.join("cache"),
            runtime: RuntimeConfig {
                backend,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![remote_service("svc-a"), remote_service("svc-b")],
        });
        assert_eq!(
            runtime_plan.ordered_services[0].runtime_image,
            runtime_plan.ordered_services[1].runtime_image,
            "the fixture must exercise one shared base artifact"
        );

        let mut options = PrepareOptions {
            force_rebuild: true,
            ..PrepareOptions::default()
        };
        match backend {
            RuntimeBackend::Pyxis => options.enroot_bin = runtime_bin.display().to_string(),
            RuntimeBackend::Apptainer => options.apptainer_bin = runtime_bin.display().to_string(),
            RuntimeBackend::Singularity | RuntimeBackend::Host => unreachable!("test backend"),
        }
        let summary = prepare_runtime_plan(&runtime_plan, &options).expect("shared base prepare");

        let runtime_log = fs::read_to_string(&log).expect("runtime log");
        let build_prefix = match backend {
            RuntimeBackend::Pyxis => "import ",
            RuntimeBackend::Apptainer => "build --force ",
            RuntimeBackend::Singularity | RuntimeBackend::Host => unreachable!("test backend"),
        };
        assert_eq!(
            runtime_log
                .lines()
                .filter(|line| line.starts_with(build_prefix))
                .count(),
            1,
            "CLI force must refresh a shared base only once for backend {backend:?}"
        );

        let expected_note = match backend {
            RuntimeBackend::Pyxis => "base cache artifact is used directly at runtime",
            RuntimeBackend::Apptainer => "base SIF cache artifact is used directly at runtime",
            RuntimeBackend::Singularity | RuntimeBackend::Host => unreachable!("test backend"),
        };
        for (service, expected_action) in summary
            .services
            .iter()
            .zip([ArtifactAction::Built, ArtifactAction::Reused])
        {
            let base = service.base_image.as_ref().expect("remote base status");
            assert_eq!(base.action, expected_action);
            assert_eq!(base.note, None);
            assert_eq!(service.runtime_image.action, expected_action);
            assert_eq!(service.runtime_image.note.as_deref(), Some(expected_note));
            assert_eq!(service.runtime_image.path, base.path);
        }

        let manifest = crate::cache::read_manifest(&runtime_plan.ordered_services[0].runtime_image)
            .expect("shared base manifest");
        assert_eq!(manifest.kind, crate::cache::CacheEntryKind::Base);
        assert_eq!(
            manifest.service_names,
            vec!["svc-a".to_string(), "svc-b".to_string()]
        );
    }
}

#[test]
fn sif_backends_use_sif_cache_paths_for_remote_images() {
    let service = RuntimeService {
        name: "app".into(),
        runtime_image: PathBuf::from("/tmp/app.sif"),
        execution: ExecutionSpec::ImageDefault,
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: ImageSource::Remote("docker://ubuntu:24.04".into()),
    };

    let cache_dir = Path::new("/shared/cache");
    assert!(
        base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Apptainer)
            .display()
            .to_string()
            .ends_with(".sif")
    );
    assert!(
        base_image_path_for_backend(cache_dir, &service, RuntimeBackend::Pyxis)
            .display()
            .to_string()
            .ends_with(".sqsh")
    );
}

#[test]
fn failed_prepare_cleans_up_by_default() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let options = PrepareOptions {
        enroot_bin: fake.display().to_string(),
        keep_failed_prep: false,
        force_rebuild: false,
        ..PrepareOptions::default()
    };

    let err = prepare_runtime_plan(&plan, &options).expect_err("should fail");
    assert!(err.to_string().contains("prepare command"));
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("remove --force"));
}

#[test]
fn force_rebuild_option_rebuilds_prepared_images() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let service = fake_service(tmpdir.path());
    let runtime_image = service.runtime_image.clone();
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };

    prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("prepare once");
    fs::write(&log, "").expect("clear log");
    fs::write(&runtime_image, "cached").expect("seed");

    let summary = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: true,
            ..PrepareOptions::default()
        },
    )
    .expect("prepare twice");
    let log_content = fs::read_to_string(log).expect("log");
    assert!(log_content.contains("create --force --name"));
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("rebuilt because --force/--force-rebuild was requested")
    );
}

#[test]
fn pyxis_prepared_action_and_note_contract_covers_reuse_force_and_mounts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let normal_root = tmpdir.path().join("normal");
    fs::create_dir_all(&normal_root).expect("normal root");
    let normal_log = normal_root.join("enroot.log");
    let normal_fake = write_fake_enroot(&normal_root, &normal_log);
    let normal_plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: normal_root.join("cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![fake_service(&normal_root)],
    };
    let normal_options = PrepareOptions {
        enroot_bin: normal_fake.display().to_string(),
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&normal_plan, &normal_options).expect("initial Pyxis prepare");
    let reused = prepare_runtime_plan(&normal_plan, &normal_options).expect("cached Pyxis prepare");
    assert_runtime_artifact_status(&reused, ArtifactAction::Reused, None);

    let forced = prepare_runtime_plan(
        &normal_plan,
        &PrepareOptions {
            force_rebuild: true,
            ..normal_options.clone()
        },
    )
    .expect("forced Pyxis prepare");
    assert_runtime_artifact_status(
        &forced,
        ArtifactAction::Built,
        Some("rebuilt because --force/--force-rebuild was requested"),
    );

    let mount_root = tmpdir.path().join("mounts");
    fs::create_dir_all(&mount_root).expect("mount root");
    let mount_log = mount_root.join("enroot.log");
    let mount_fake = write_fake_enroot(&mount_root, &mount_log);
    let mut mount_service = fake_service(&mount_root);
    let prepare = mount_service.prepare.as_mut().expect("prepare");
    prepare.mounts = vec!["/host:/mnt".into()];
    prepare.force_rebuild = true;
    let mount_plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: mount_root.join("cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![mount_service],
    };
    let mount_options = PrepareOptions {
        enroot_bin: mount_fake.display().to_string(),
        ..PrepareOptions::default()
    };
    prepare_runtime_plan(&mount_plan, &mount_options).expect("initial mounted Pyxis prepare");
    let mount_forced =
        prepare_runtime_plan(&mount_plan, &mount_options).expect("cached mounted Pyxis prepare");
    assert_runtime_artifact_status(
        &mount_forced,
        ArtifactAction::Built,
        Some("rebuilt because prepare.mounts are present"),
    );
    let both_forced = prepare_runtime_plan(
        &mount_plan,
        &PrepareOptions {
            force_rebuild: true,
            ..mount_options
        },
    )
    .expect("CLI-forced mounted Pyxis prepare");
    assert_runtime_artifact_status(
        &both_forced,
        ArtifactAction::Built,
        Some("rebuilt because --force/--force-rebuild was requested"),
    );
}

#[test]
fn sif_prepared_action_and_note_contract_covers_reuse_force_and_mounts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let normal_plan = fake_sif_prepared_plan(&tmpdir.path().join("normal"), false);
    let normal_options = PrepareOptions {
        apptainer_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    };

    prepare_runtime_plan(&normal_plan, &normal_options).expect("initial SIF prepare");
    let reused = prepare_runtime_plan(&normal_plan, &normal_options).expect("cached SIF prepare");
    assert_runtime_artifact_status(&reused, ArtifactAction::Reused, None);

    let forced = prepare_runtime_plan(
        &normal_plan,
        &PrepareOptions {
            force_rebuild: true,
            ..normal_options.clone()
        },
    )
    .expect("forced SIF prepare");
    assert_runtime_artifact_status(
        &forced,
        ArtifactAction::Built,
        Some("rebuilt because --force/--force-rebuild was requested"),
    );

    let mount_plan = fake_sif_prepared_plan(&tmpdir.path().join("mounts"), true);
    prepare_runtime_plan(&mount_plan, &normal_options).expect("initial mounted SIF prepare");
    let mount_forced =
        prepare_runtime_plan(&mount_plan, &normal_options).expect("cached mounted SIF prepare");
    assert_runtime_artifact_status(
        &mount_forced,
        ArtifactAction::Built,
        Some("rebuilt because prepare.mounts are present"),
    );
    let both_forced = prepare_runtime_plan(
        &mount_plan,
        &PrepareOptions {
            force_rebuild: true,
            ..normal_options
        },
    )
    .expect("CLI-forced mounted SIF prepare");
    assert_runtime_artifact_status(
        &both_forced,
        ArtifactAction::Built,
        Some("rebuilt because --force/--force-rebuild was requested"),
    );
}

#[test]
fn helper_defaults_and_paths_cover_remaining_prepare_helpers() {
    let defaults = PrepareOptions::default();
    assert_eq!(defaults.enroot_bin, "enroot");
    assert_eq!(defaults.huggingface_cli_bin, "huggingface-cli");
    assert!(!defaults.keep_failed_prep);
    assert!(!defaults.force_rebuild);

    let cache_dir = Path::new("/shared/cache");
    let service = RuntimeService {
        name: "svc/name".into(),
        runtime_image: PathBuf::from("/tmp/runtime.sqsh"),
        execution: ExecutionSpec::ImageDefault,
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare: None,
        source: ImageSource::LocalSqsh(PathBuf::from("/tmp/local-image.sqsh")),
    };
    assert_eq!(base_image_cache_key(&service).len(), 64);
    assert!(temporary_rootfs_name(&service).starts_with("hpc-compose-svc_name-"));
    assert_eq!(
        crate::domain::short_digest_prefix("1234567890abcdef1234"),
        "1234567890abcdef"
    );
    assert_eq!(sanitize_name("svc/name"), "svc_name");
    assert_eq!(image_label(&service.source), "local-image");
    let temp = cache_dir.join("enroot/tmp");
    let data = cache_dir.join("enroot/data");
    let envs = enroot_env(cache_dir, &data, &temp, false);
    assert_eq!(envs.len(), 3);
    assert!(envs[0].1.contains("enroot/cache"));
    assert!(
        envs.iter()
            .any(|(key, value)| key == "ENROOT_DATA_PATH" && value.contains("enroot/data"))
    );
    assert!(
        envs.iter()
            .any(|(key, value)| key == "ENROOT_TEMP_PATH" && value.contains("enroot/tmp"))
    );
    assert!(!envs.iter().any(|(key, _)| key == "NVIDIA_VISIBLE_DEVICES"));
    let envs_no_gpu = enroot_env(cache_dir, &data, &temp, true);
    assert_eq!(envs_no_gpu.len(), 4);
    assert!(
        envs_no_gpu
            .iter()
            .any(|(key, value)| key == "NVIDIA_VISIBLE_DEVICES" && value == "void")
    );
}

#[test]
fn enroot_data_dir_follows_scratch_redirect() {
    let cache = Path::new("/shared/cache");
    // Default scratch keeps the prepare rootfs on the persistent shared cache.
    assert_eq!(
        enroot_data_dir(&cache.join("enroot/tmp"), cache),
        cache.join("enroot/data")
    );
    // A redirected (node-local) scratch moves the transient rootfs node-local too,
    // in an hpc-compose-owned per-process subdir of the scratch root.
    let local = Path::new("/tmp/me-hpc-compose-enroot");
    let data = enroot_data_dir(local, cache);
    assert!(
        data.starts_with(local),
        "data dir {data:?} should be node-local"
    );
    assert!(
        data.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("hpc-compose-enroot-data-")),
        "data dir {data:?} should be an hpc-compose-owned per-process subdir"
    );
}

#[test]
fn prepare_runtime_plan_covers_local_missing_and_remote_without_prepare() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let local_present_path = tmpdir.path().join("present.sqsh");
    fs::write(&local_present_path, "x").expect("present local");
    let local_present = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache-local"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-present".into(),
            runtime_image: local_present_path.clone(),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::LocalSqsh(local_present_path.clone()),
        }],
    };
    let local_summary = prepare_runtime_plan(
        &local_present,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("local present");
    assert_eq!(
        local_summary.services[0].runtime_image.action,
        ArtifactAction::Present
    );
    assert_eq!(
        local_summary.services[0].runtime_image.note.as_deref(),
        Some("uses local .sqsh directly")
    );

    let local_missing = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local".into(),
            runtime_image: tmpdir.path().join("local.sqsh"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
        }],
    };
    let err = prepare_runtime_plan(
        &local_missing,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect_err("local missing");
    assert!(err.to_string().contains("does not exist"));

    let remote_no_prepare = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache2"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "redis".into(),
            runtime_image: tmpdir.path().join("cache2/base/redis.sqsh"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::Remote("docker://redis:7".into()),
        }],
    };
    let summary = prepare_runtime_plan(
        &remote_no_prepare,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("remote no prepare");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("base cache artifact is used directly at runtime")
    );
}

#[test]
fn local_sqsh_prepare_and_helper_failures_cover_remaining_branches() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);
    let local_base = tmpdir.path().join("local-base.sqsh");
    fs::write(&local_base, "x").expect("local base");

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-prepared".into(),
            runtime_image: tmpdir.path().join("cache/prepared/local-prepared.sqsh"),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: Some(PreparedImageSpec {
                commands: vec!["echo local".into()],
                mounts: vec!["/host:/mnt".into()],
                env: vec![("KEY".into(), "VALUE".into())],
                root: false,
                force_rebuild: false,
            }),
            source: ImageSource::LocalSqsh(local_base),
        }],
    };
    let summary = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: false,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect("local prepare");
    assert!(summary.services[0].base_image.is_none());
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    let log_content = fs::read_to_string(&log).expect("log");
    assert!(!log_content.contains("import"));
    assert!(log_content.contains("--mount /host:/mnt"));
    assert!(!log_content.contains("start --root --rw"));

    let err = ensure_parent_dir(Path::new("/")).expect_err("root has no parent");
    assert!(err.to_string().contains("does not have a parent directory"));

    let err = run_enroot(
        "/definitely/missing/enroot",
        &[],
        vec!["version".to_string()],
        "probe missing binary",
        &StreamCtx::quiet(&NoopPrepareReporter, "test"),
    )
    .expect_err("missing binary execution");
    assert!(err.to_string().contains("failed to execute"));
}

#[test]
fn keep_failed_prep_and_binary_errors_cover_failure_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("enroot.log");
    let fake = write_fake_enroot(tmpdir.path(), &log);

    let mut service = fake_service(tmpdir.path());
    service.prepare.as_mut().expect("prepare").commands = vec!["fail-me".into()];
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service],
    };
    let err = prepare_runtime_plan(
        &plan,
        &PrepareOptions {
            enroot_bin: fake.display().to_string(),
            keep_failed_prep: true,
            force_rebuild: false,
            ..PrepareOptions::default()
        },
    )
    .expect_err("should fail");
    assert!(err.to_string().contains("prepare command"));
    let log_content = fs::read_to_string(log).expect("log");
    let remove_count = log_content.matches("remove --force").count();
    assert_eq!(remove_count, 1);

    let err = ensure_binary_available("/definitely/missing/enroot", "missing")
        .expect_err("missing binary");
    assert!(err.to_string().contains("missing"));
}

#[test]
fn helper_paths_binary_search_and_run_failures_are_reported() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: cache_dir.clone(),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: Vec::new(),
    };
    create_cache_dirs(&plan).expect("create cache dirs");
    for suffix in [
        "base",
        "prepared",
        "enroot/cache",
        "enroot/data",
        "enroot/tmp",
    ] {
        assert!(cache_dir.join(suffix).exists(), "{suffix} missing");
    }

    let _guard = env_lock().lock().expect("env lock");
    let bin_dir = tmpdir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let helper = bin_dir.join("enroot-ok");
    fs::write(
            &helper,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == fail ]]; then echo boom >&2; exit 7; fi\nexit 0\n",
        )
        .expect("helper");
    let mut perms = fs::metadata(&helper).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&helper, perms).expect("chmod");
    let old_path = env::var_os("PATH");
    let joined = env::join_paths(
        std::iter::once(bin_dir.clone())
            .chain(old_path.as_ref().into_iter().flat_map(env::split_paths)),
    )
    .expect("join path");
    unsafe {
        env::set_var("PATH", joined);
    }
    ensure_binary_available("enroot-ok", "missing in path").expect("binary on path");
    let err = run_enroot(
        helper.to_str().expect("helper"),
        &[],
        vec!["fail".to_string()],
        "run failing command",
        &StreamCtx::quiet(&NoopPrepareReporter, "test"),
    )
    .expect_err("failing helper");
    assert!(
        err.to_string()
            .contains("failed to run failing command: boom")
    );
    match old_path {
        Some(value) => unsafe {
            env::set_var("PATH", value);
        },
        None => unsafe {
            env::remove_var("PATH");
        },
    }
}

#[test]
fn sif_remote_base_builds_reuses_and_writes_manifest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![PlannedService {
            name: "app".into(),
            image: ImageSource::Remote("docker://example.com/app:1".into()),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
        }],
    };
    let runtime_plan = build_runtime_plan(&plan);
    let options = PrepareOptions {
        apptainer_bin: fake.display().to_string(),
        ..PrepareOptions::default()
    };

    let first = prepare_runtime_plan(&runtime_plan, &options).expect("first prepare");
    assert_eq!(
        first.services[0].base_image.as_ref().expect("base").action,
        ArtifactAction::Built
    );
    assert_eq!(
        first.services[0].runtime_image.note.as_deref(),
        Some("base SIF cache artifact is used directly at runtime")
    );
    assert!(runtime_plan.ordered_services[0].runtime_image.exists());
    let manifest = crate::cache::read_manifest(&runtime_plan.ordered_services[0].runtime_image)
        .expect("base manifest");
    assert_eq!(manifest.kind, crate::cache::CacheEntryKind::Base);
    assert!(
        fs::read_to_string(&log)
            .expect("log")
            .contains("docker://example.com/app:1")
    );

    fs::write(&log, "").expect("clear log");
    let second = prepare_runtime_plan(&runtime_plan, &options).expect("second prepare");
    assert_eq!(
        second.services[0].base_image.as_ref().expect("base").action,
        ArtifactAction::Reused
    );
    assert!(
        !fs::read_to_string(&log)
            .expect("log")
            .contains("build --force")
    );
}

#[test]
fn sif_local_images_are_validated_for_sif_backends() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let local_sif = tmpdir.path().join("local.sif");
    fs::write(&local_sif, "sif").expect("local sif");
    let local_sqsh = tmpdir.path().join("local.sqsh");
    fs::write(&local_sqsh, "sqsh").expect("local sqsh");

    let present = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache-present"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Singularity,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![RuntimeService {
            name: "local-sif".into(),
            runtime_image: local_sif.clone(),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: None,
            source: ImageSource::LocalSif(local_sif.clone()),
        }],
    };
    let summary = prepare_runtime_plan(
        &present,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect("local sif present");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Present
    );
    assert_eq!(
        summary.services[0].runtime_image.note.as_deref(),
        Some("uses local .sif directly")
    );

    let missing = RuntimePlan {
        cache_dir: tmpdir.path().join("cache-missing"),
        ordered_services: vec![RuntimeService {
            name: "missing-sif".into(),
            runtime_image: tmpdir.path().join("missing.sif"),
            source: ImageSource::LocalSif(tmpdir.path().join("missing.sif")),
            ..present.ordered_services[0].clone()
        }],
        ..present.clone()
    };
    let err = prepare_runtime_plan(
        &missing,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect_err("missing local sif");
    assert!(err.to_string().contains("does not exist"));

    let wrong_format = RuntimePlan {
        cache_dir: tmpdir.path().join("cache-sqsh"),
        ordered_services: vec![RuntimeService {
            name: "local-sqsh".into(),
            runtime_image: local_sqsh.clone(),
            source: ImageSource::LocalSqsh(local_sqsh),
            ..present.ordered_services[0].clone()
        }],
        ..present
    };
    let err = prepare_runtime_plan(
        &wrong_format,
        &PrepareOptions {
            singularity_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect_err("sqsh rejected by sif backend");
    assert!(err.to_string().contains("requires SIF images"));
}

#[test]
fn sif_prepare_sequence_uses_sandbox_flags_and_backend_cache_key() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("sif-runtime.log");
    let fake = write_fake_sif_runtime(tmpdir.path(), &log);
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let prepare = PreparedImageSpec {
        commands: vec!["echo setup".into()],
        mounts: vec!["/host:/mnt".into()],
        env: vec![("KEY".into(), "VALUE".into())],
        root: true,
        force_rebuild: false,
    };
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig {
            backend: RuntimeBackend::Apptainer,
            ..RuntimeConfig::default()
        },
        slurm: SlurmConfig::default(),
        ordered_services: vec![PlannedService {
            name: "prepared-sif".into(),
            image: ImageSource::Remote("docker://example.com/prepared:1".into()),
            execution: ExecutionSpec::ImageDefault,
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare: Some(prepare.clone()),
        }],
    };
    let runtime_plan = build_runtime_plan(&plan);
    let service = &runtime_plan.ordered_services[0];

    let summary = prepare_runtime_plan(
        &runtime_plan,
        &PrepareOptions {
            apptainer_bin: fake.display().to_string(),
            ..PrepareOptions::default()
        },
    )
    .expect("sif prepare");
    assert_eq!(
        summary.services[0].runtime_image.action,
        ArtifactAction::Built
    );
    assert!(service.runtime_image.exists());

    let log_content = fs::read_to_string(&log).expect("log");
    assert!(log_content.contains("build --force --sandbox --fakeroot"));
    assert!(log_content.contains("exec --writable --fakeroot"));
    assert!(log_content.contains("--bind /host:/mnt"));
    assert!(log_content.contains("--env KEY=VALUE"));
    assert!(
        log_content.contains(".hpc-compose-stage-"),
        "the image tool writes a sibling staging artifact before publication"
    );
    assert!(
        !fs::read_dir(runtime_plan.cache_dir.join("prepared"))
            .expect("prepared dir")
            .any(|entry| entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".sandbox"))
    );

    let manifest = crate::cache::read_manifest(&service.runtime_image).expect("manifest");
    let expected_from_plan = prepared_image_cache_key_from_plan(
        &plan.ordered_services[0],
        &prepare,
        RuntimeBackend::Apptainer,
    );
    assert_eq!(manifest.cache_key, expected_from_plan);
    assert_eq!(
        manifest.cache_key,
        prepared_image_cache_key(service, &prepare, RuntimeBackend::Apptainer)
    );
}

#[test]
fn failed_sif_prepare_cleanup_respects_keep_failed_prep() {
    for (keep_failed_prep, should_keep_sandbox) in [(false, false), (true, true)] {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("sif-runtime.log");
        let fake = write_fake_sif_runtime(tmpdir.path(), &log);
        let local_sif = tmpdir.path().join("base.sif");
        fs::write(&local_sif, "sif").expect("local sif");
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            runtime: RuntimeConfig {
                backend: RuntimeBackend::Apptainer,
                ..RuntimeConfig::default()
            },
            slurm: SlurmConfig::default(),
            ordered_services: vec![RuntimeService {
                name: "bad-prepare".into(),
                runtime_image: tmpdir.path().join("cache/prepared/bad-prepare.sif"),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(PreparedImageSpec {
                    commands: vec!["fail-me".into()],
                    mounts: Vec::new(),
                    env: Vec::new(),
                    root: false,
                    force_rebuild: false,
                }),
                source: ImageSource::LocalSif(local_sif),
            }],
        };
        let err = prepare_runtime_plan(
            &plan,
            &PrepareOptions {
                apptainer_bin: fake.display().to_string(),
                keep_failed_prep,
                ..PrepareOptions::default()
            },
        )
        .expect_err("prepare failure");
        assert!(err.to_string().contains("run prepare command"));
        let sandbox_left = fs::read_dir(plan.cache_dir.join("prepared"))
            .expect("prepared dir")
            .any(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".sandbox")
            });
        assert_eq!(sandbox_left, should_keep_sandbox);
    }
}

#[test]
fn runtime_path_and_command_helpers_cover_remaining_branches() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let plan = Plan {
        name: "demo".into(),
        project_dir: tmpdir.path().to_path_buf(),
        spec_path: compose,
        cache_dir: tmpdir.path().join("cache"),
        runtime: RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![
            PlannedService {
                name: "local".into(),
                image: ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh")),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
            },
            PlannedService {
                name: "prepared".into(),
                image: ImageSource::LocalSqsh(PathBuf::from("/tmp/base.sqsh")),
                execution: ExecutionSpec::ImageDefault,
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                assertions: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: Some(PreparedImageSpec {
                    commands: vec!["echo hi".into()],
                    mounts: Vec::new(),
                    env: Vec::new(),
                    root: true,
                    force_rebuild: false,
                }),
            },
        ],
    };
    let runtime = build_runtime_plan(&plan);
    assert_eq!(
        runtime.ordered_services[0].runtime_image,
        PathBuf::from("/tmp/local.sqsh")
    );
    assert!(
        runtime.ordered_services[1]
            .runtime_image
            .display()
            .to_string()
            .contains("/prepared/")
    );
    assert!(
        prepared_image_cache_key_from_plan(
            &plan.ordered_services[1],
            plan.ordered_services[1].prepare.as_ref().expect("prepare"),
            plan.runtime.backend
        )
        .len()
            > 10
    );
    assert!(
        prepared_image_cache_key(
            &runtime.ordered_services[1],
            runtime.ordered_services[1]
                .prepare
                .as_ref()
                .expect("prepare"),
            plan.runtime.backend
        )
        .len()
            > 10
    );
}

#[test]
fn resolve_enroot_temp_dir_applies_precedence_and_default() {
    let cache = Path::new("/shared/cache");
    assert_eq!(
        resolve_enroot_temp_dir(None, None, None, cache),
        cache.join("enroot/tmp")
    );
    assert_eq!(
        resolve_enroot_temp_dir(None, None, Some("/local/from-settings"), cache),
        PathBuf::from("/local/from-settings")
    );
    assert_eq!(
        resolve_enroot_temp_dir(
            None,
            Some("/local/from-spec"),
            Some("/local/from-settings"),
            cache
        ),
        PathBuf::from("/local/from-spec")
    );
    assert_eq!(
        resolve_enroot_temp_dir(
            Some("/local/from-env"),
            Some("/local/from-spec"),
            Some("/local/from-settings"),
            cache
        ),
        PathBuf::from("/local/from-env")
    );
    // Blank values fall through to the next layer.
    assert_eq!(
        resolve_enroot_temp_dir(Some("  "), None, None, cache),
        cache.join("enroot/tmp")
    );
}

#[test]
fn prepare_truthy_flags_preserve_case_whitespace_and_false_value_parity() {
    let _guard = env_lock().lock().expect("env lock");
    let previous_gpu = env::var_os(PREPARE_GPU_ENV);
    let previous_verbose = env::var_os(PREPARE_VERBOSE_ENV);

    unsafe {
        env::remove_var(PREPARE_GPU_ENV);
        env::remove_var(PREPARE_VERBOSE_ENV);
    }
    let defaults = (
        gpu_flag_enabled(None),
        prepare_gpu_enabled(),
        prepare_verbose_enabled(),
    );

    let mut observations = Vec::new();
    for (value, expected) in [
        ("1", true),
        ("true", true),
        ("TRUE", true),
        ("yes", true),
        ("On", true),
        ("  true  ", true),
        ("0", false),
        ("false", false),
        (" FALSE ", false),
        ("no", false),
        ("", false),
        ("  ", false),
        ("maybe", false),
    ] {
        unsafe {
            env::set_var(PREPARE_GPU_ENV, value);
            env::set_var(PREPARE_VERBOSE_ENV, value);
        }
        observations.push((
            value,
            expected,
            gpu_flag_enabled(Some(value)),
            prepare_gpu_enabled(),
            prepare_verbose_enabled(),
        ));
    }

    match previous_gpu {
        Some(value) => unsafe { env::set_var(PREPARE_GPU_ENV, value) },
        None => unsafe { env::remove_var(PREPARE_GPU_ENV) },
    }
    match previous_verbose {
        Some(value) => unsafe { env::set_var(PREPARE_VERBOSE_ENV, value) },
        None => unsafe { env::remove_var(PREPARE_VERBOSE_ENV) },
    }

    assert_eq!(defaults, (false, false, false));
    for (value, expected, pure, gpu_env, verbose_env) in observations {
        assert_eq!(pure, expected, "pure {value:?}");
        assert_eq!(gpu_env, expected, "gpu env {value:?}");
        assert_eq!(verbose_env, expected, "verbose env {value:?}");
    }
}

#[test]
fn is_stale_handle_error_detects_estale_signatures() {
    assert!(is_stale_handle_error(&anyhow::Error::msg(
        "failed to import base image: Read failed because Stale file handle"
    )));
    assert!(is_stale_handle_error(&anyhow::Error::msg(
        "Creating squashfs filesystem... read failed because stale file handle"
    )));
    assert!(!is_stale_handle_error(&anyhow::Error::msg(
        "failed to import base image: manifest unknown"
    )));
}

#[test]
fn is_missing_image_error_detects_registry_rejections() {
    assert!(is_missing_image_error(&anyhow::Error::msg(
        "failed to import base image: manifest unknown: manifest unknown"
    )));
    assert!(is_missing_image_error(&anyhow::Error::msg(
        "Error reading manifest 2.3.1-cuda12.1-cudnn9-runtime: manifest not found"
    )));
    assert!(is_missing_image_error(&anyhow::Error::msg(
        "unexpected http status 401 Unauthorized"
    )));
    // A stale-handle failure is a filesystem problem, not a missing image.
    assert!(!is_missing_image_error(&anyhow::Error::msg(
        "Read failed because Stale file handle"
    )));
}
