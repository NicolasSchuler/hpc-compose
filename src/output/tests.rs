use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use super::*;
use crate::commands::run_command;
use hpc_compose::cache::{CacheEntryKind, CacheEntryManifest};
use hpc_compose::cli::{CacheCommands, Commands, HoldOnExit, RuntimeLaunchArgs, WatchMode};
use hpc_compose::job::{
    ArtifactExportReport, ArtifactManifest, BatchLogStatus, CleanupJobReport, CleanupReport,
    CollectorStatus, GpuDeviceSample, GpuProcessSample, GpuSnapshot, JobInventoryEntry,
    JobInventoryScan, QueueDiagnostics, SamplerSnapshot, SchedulerSource, SchedulerStatus,
    ServiceAssertionStatus, ServiceLogStatus, StatsSnapshot, StatusSnapshot, StepStats,
    SubmissionKind, SubmissionRecord,
};
use hpc_compose::planner::{ExecutionSpec, ImageSource, PreparedImageSpec, ServicePlacement};
use hpc_compose::spec::{
    DependencyCondition, ReadinessSpec, ServiceDependency, ServiceFailurePolicy,
    ServiceSlurmConfig, SlurmConfig,
};

fn runtime_service(
    source: ImageSource,
    runtime_image: PathBuf,
    prepare: Option<PreparedImageSpec>,
) -> hpc_compose::prepare::RuntimeService {
    hpc_compose::prepare::RuntimeService {
        name: "svc/name".into(),
        runtime_image,
        execution: ExecutionSpec::Shell("echo hi".into()),
        environment: Vec::new(),
        volumes: Vec::new(),
        working_dir: None,
        depends_on: Vec::new(),
        readiness: None,
        assertions: None,
        failure_policy: ServiceFailurePolicy::default(),
        placement: ServicePlacement::default(),
        slurm: ServiceSlurmConfig::default(),
        prepare,
        source,
    }
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn strip_ansi(text: &str) -> String {
    let mut output = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for code in chars.by_ref() {
                if code.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        output.push(ch);
    }
    output
}

fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("fake-enroot.sh");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name) name="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start) exit 0 ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove) exit 0 ;;
esac
"#,
    );
    path
}

fn write_fake_sbatch(tmpdir: &Path, success: bool) -> PathBuf {
    let path = tmpdir.join(if success { "sbatch-ok" } else { "sbatch-fail" });
    let body = if success {
        "#!/bin/bash\nset -euo pipefail\necho 'Submitted batch job 54321'\n"
    } else {
        "#!/bin/bash\nset -euo pipefail\necho 'boom' >&2\nexit 2\n"
    };
    write_script(&path, body);
    path
}

fn write_fake_srun(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage --container-image'; fi\n",
    );
    path
}

fn write_compose(tmpdir: &Path, body: &str) -> PathBuf {
    let path = tmpdir.join("compose.yaml");
    fs::write(&path, body).expect("compose");
    path
}

fn safe_cache_dir() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
    fs::create_dir_all(&root).expect("cache root");
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(root)
        .expect("cache tempdir")
}

fn write_valid_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app");
    fs::write(tmpdir.join("app/main.py"), "print('hi')\n").expect("main.py");
    write_compose(
        tmpdir,
        &format!(
            r#"
name: demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install click
"#,
            cache_dir.display()
        ),
    )
}

fn submission_record(tmpdir: &Path, plan: &RuntimePlan, job_id: &str) -> SubmissionRecord {
    hpc_compose::job::build_submission_record(
        &tmpdir.join("compose.yaml"),
        tmpdir,
        &tmpdir.join("job.sbatch"),
        plan,
        job_id,
    )
    .expect("record")
}

fn sample_step() -> StepStats {
    let mut alloc_tres_map = BTreeMap::new();
    alloc_tres_map.insert("gres/gpu".into(), "1".into());
    let mut usage_tres_map = BTreeMap::new();
    usage_tres_map.insert("gres/gpuutil".into(), "87".into());
    usage_tres_map.insert("gres/gpumem".into(), "4096M".into());
    StepStats {
        step_id: "12345.0".into(),
        ntasks: "1".into(),
        ave_cpu: "00:00:03".into(),
        ave_rss: "128M".into(),
        max_rss: "256M".into(),
        alloc_tres: "cpu=1,gres/gpu=1".into(),
        tres_usage_in_ave: "cpu=00:00:03,gres/gpuutil=87,gres/gpumem=4096M".into(),
        alloc_tres_map,
        usage_tres_in_ave_map: usage_tres_map,
        gpu_count: Some("1".into()),
        gpu_util: Some("87".into()),
        gpu_mem: Some("4096M".into()),
    }
}

fn sample_service_status(path: PathBuf) -> ServiceLogStatus {
    ServiceLogStatus {
        service_name: "svc/name".into(),
        path,
        present: false,
        updated_at: None,
        updated_age_seconds: None,
        log_path: None,
        step_name: None,
        launch_index: None,
        launcher_pid: None,
        healthy: None,
        completed_successfully: None,
        readiness_configured: None,
        status: None,
        failure_policy_mode: None,
        restart_count: None,
        max_restarts: None,
        window_seconds: None,
        max_restarts_in_window: None,
        restart_failures_in_window: None,
        last_exit_code: None,
        started_at: None,
        finished_at: None,
        duration_seconds: None,
        assertions: None,
        placement_mode: None,
        nodes: None,
        ntasks: None,
        ntasks_per_node: None,
        nodelist: None,
    }
}

#[test]
fn action_and_label_helpers_cover_all_variants() {
    assert_eq!(action_label(ArtifactAction::Present), "OK");
    assert_eq!(action_label(ArtifactAction::Reused), "REUSE");
    assert_eq!(action_label(ArtifactAction::Built), "BUILD");
    assert_eq!(artifact_role_label("base"), "cache artifact");
    assert_eq!(artifact_role_label("runtime"), "artifact");
    assert_eq!(artifact_role_label("other"), "artifact");
    assert_eq!(hit_or_miss(true), "cache hit");
    assert_eq!(hit_or_miss(false), "cache miss");
    assert_eq!(yes_no(true), "yes");
    assert_eq!(yes_no(false), "no");
}

#[test]
fn sanitize_and_extract_job_id_work() {
    assert_eq!(
        log_file_name_for_service("svc/name.with spaces"),
        "svc_x2f_name_x2e_with_x20_spaces.log"
    );
    assert_eq!(extract_job_id("Submitted batch job 12345"), Some("12345"));
    assert_eq!(extract_job_id("no job id here"), None);
}

#[test]
fn finish_watch_requires_a_terminal_scheduler_result() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_valid_compose(tmpdir.path(), cache.path());
    let plan = load_runtime_plan(&compose).expect("runtime plan");
    let record = submission_record(tmpdir.path(), &plan, "12345");
    finish_watch(
        &record,
        WatchOutcome::Completed(hpc_compose::job::SchedulerStatus {
            state: "COMPLETED".into(),
            source: hpc_compose::job::SchedulerSource::Sacct,
            terminal: true,
            failed: false,
            detail: None,
        }),
    )
    .expect("completed watch");

    let err = finish_watch(
        &record,
        WatchOutcome::Unknown(hpc_compose::job::SchedulerStatus {
            state: "unknown".into(),
            source: hpc_compose::job::SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some("scheduler tools were unavailable".into()),
        }),
    )
    .expect_err("unknown watch should fail");
    assert!(err.to_string().contains("could not be tracked"));
    assert!(err.to_string().contains("scheduler tools were unavailable"));
}

#[test]
fn runtime_cache_state_covers_prepare_and_local_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_sqsh = tmpdir.path().join("local.sqsh");
    let remote_sqsh = tmpdir.path().join("remote.sqsh");
    std::fs::write(&local_sqsh, "x").expect("local");
    std::fs::write(&remote_sqsh, "x").expect("remote");

    let with_forced_prepare = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        remote_sqsh.clone(),
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: vec!["/host:/mnt".into()],
            env: Vec::new(),
            root: true,
            force_rebuild: true,
        }),
    );
    assert_eq!(
        runtime_cache_state(&with_forced_prepare),
        "rebuild on prepare"
    );

    let with_cached_prepare = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        remote_sqsh.clone(),
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: Vec::new(),
            env: Vec::new(),
            root: true,
            force_rebuild: false,
        }),
    );
    assert_eq!(runtime_cache_state(&with_cached_prepare), "cache hit");

    let missing_prepare = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        tmpdir.path().join("prepared-missing.sqsh"),
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: Vec::new(),
            env: Vec::new(),
            root: true,
            force_rebuild: false,
        }),
    );
    assert_eq!(runtime_cache_state(&missing_prepare), "cache miss");

    let local_present = runtime_service(
        ImageSource::LocalSqsh(local_sqsh.clone()),
        local_sqsh.clone(),
        None,
    );
    assert_eq!(runtime_cache_state(&local_present), "local image present");

    let local_missing = runtime_service(
        ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
        tmpdir.path().join("missing.sqsh"),
        None,
    );
    assert_eq!(runtime_cache_state(&local_missing), "local image missing");

    let remote_missing = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        tmpdir.path().join("missing-remote.sqsh"),
        None,
    );
    assert_eq!(runtime_cache_state(&remote_missing), "cache miss");
}

#[test]
fn service_names_collect_in_order() {
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: PathBuf::from("/cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![
            runtime_service(
                ImageSource::Remote("docker://redis:7".into()),
                PathBuf::from("/cache/a.sqsh"),
                None,
            ),
            hpc_compose::prepare::RuntimeService {
                name: "worker".into(),
                ..runtime_service(
                    ImageSource::Remote("docker://python:3.11-slim".into()),
                    PathBuf::from("/cache/b.sqsh"),
                    None,
                )
            },
        ],
    };
    assert_eq!(service_names(&plan), vec!["svc/name", "worker"]);
}

#[test]
fn path_helpers_return_expected_locations() {
    let path = PathBuf::from("/tmp/project/compose.yaml");
    assert_eq!(
        default_script_path(&path),
        PathBuf::from("/tmp/project/hpc-compose.sbatch")
    );
    assert_eq!(
        default_script_path(Path::new("compose.yaml")),
        PathBuf::from("hpc-compose.sbatch")
    );
    assert!(default_cache_dir().ends_with(".cache/hpc-compose"));
    let err = render_from_path(Path::new("/definitely/missing/compose.yaml")).expect_err("missing");
    assert!(err.to_string().contains("/definitely/missing/compose.yaml"));
}

#[test]
fn print_helpers_cover_manifest_and_summary_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let runtime_image = tmpdir.path().join("prepared.sqsh");
    std::fs::write(&runtime_image, "x").expect("runtime");
    let local_sqsh = tmpdir.path().join("local.sqsh");
    std::fs::write(&local_sqsh, "x").expect("local");
    let manifest = CacheEntryManifest {
        kind: CacheEntryKind::Prepared,
        artifact_path: runtime_image.display().to_string(),
        service_names: vec!["svc/name".into()],
        cache_key: "key".into(),
        source_image: "docker://redis:7".into(),
        registry: Some("registry-1.docker.io".into()),
        prepare_commands: Vec::new(),
        prepare_env: Vec::new(),
        prepare_root: Some(true),
        prepare_mounts: Vec::new(),
        force_rebuild_due_to_mounts: false,
        created_at: 1,
        last_used_at: 1,
        tool_version: "0.1.0".into(),
        uri: None,
        revision: None,
        content_digest: None,
    };
    let manifest_path = hpc_compose::cache::manifest_path_for(&runtime_image);
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest"),
    )
    .expect("write manifest");

    let service = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        runtime_image.clone(),
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: vec!["/host:/mnt".into()],
            env: Vec::new(),
            root: true,
            force_rebuild: true,
        }),
    );
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service.clone()],
    };
    let local_plan = RuntimePlan {
        name: "local-demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service(
            ImageSource::LocalSqsh(local_sqsh.clone()),
            local_sqsh,
            None,
        )],
    };

    print_report(&Report { items: Vec::new() }, false);
    print_report(
        &Report {
            items: vec![hpc_compose::preflight::Item {
                level: hpc_compose::preflight::Level::Warn,
                message: "warn".into(),
                remediation: None,
            }],
        },
        false,
    );
    print_prepare_summary(&PrepareSummary {
        services: vec![hpc_compose::prepare::ServicePrepareResult {
            service_name: service.name.clone(),
            base_image: Some(hpc_compose::prepare::ArtifactStatus {
                path: tmpdir.path().join("base.sqsh"),
                action: ArtifactAction::Built,
                note: None,
            }),
            runtime_image: hpc_compose::prepare::ArtifactStatus {
                path: runtime_image.clone(),
                action: ArtifactAction::Reused,
                note: Some("cached".into()),
            },
        }],
    });
    print_plan_inspect(&plan).expect("print plan inspect");
    print_plan_inspect(&local_plan).expect("print local plan inspect");
    print_cache_inspect(&build_cache_inspect_report(&plan, None).expect("inspect report"))
        .expect("inspect");
    print_cache_inspect(
        &build_cache_inspect_report(&plan, Some("other")).expect("inspect filtered report"),
    )
    .expect("inspect filtered");
    print_manifest_block(&runtime_image).expect("manifest block");
    print_manifest_block(&tmpdir.path().join("missing.sqsh")).expect("missing manifest block");
    print_prune_result(tmpdir.path(), &[]);
    print_prune_result(tmpdir.path(), std::slice::from_ref(&runtime_image));
    print_submit_details(&plan, Path::new("/tmp/job.sbatch"), "no job id").expect("submit details");
    print_submit_details(
        &plan,
        Path::new("/tmp/job.sbatch"),
        "Submitted batch job 99999",
    )
    .expect("submit details with job id");
    assert_eq!(source_image_display(&service.source), "docker://redis:7");
    assert_eq!(
        source_image_display(&ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh"))),
        "/tmp/local.sqsh"
    );
}

#[test]
fn writer_helpers_cover_status_stats_artifacts_and_verbose_inspect() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let runtime_image = tmpdir.path().join("prepared.sqsh");
    fs::write(&runtime_image, "x").expect("runtime");
    let mut service = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        runtime_image,
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: vec!["/host:/mnt".into()],
            env: Vec::new(),
            root: true,
            force_rebuild: true,
        }),
    );
    service.environment = vec![("TOKEN".into(), "secret".into())];
    service.volumes = vec!["./app:/workspace".into()];
    service.working_dir = Some("/workspace".into());
    service.readiness = Some(ReadinessSpec::Http {
        url: "http://127.0.0.1:8000/health".into(),
        status_code: 200,
        timeout_seconds: Some(30),
    });
    service.depends_on = vec![ServiceDependency {
        name: "db".into(),
        condition: DependencyCondition::ServiceHealthy,
        implicit: false,
    }];

    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![service.clone()],
    };
    let record = submission_record(tmpdir.path(), &plan, "12345");
    let status = StatusSnapshot {
        record: record.clone(),
        scheduler: SchedulerStatus {
            state: "COMPLETED".into(),
            source: SchedulerSource::Sacct,
            terminal: true,
            failed: false,
            detail: Some("finished".into()),
        },
        queue_diagnostics: Some(QueueDiagnostics {
            pending_reason: None,
            eligible_time: Some("2026-04-06T10:00:00".into()),
            start_time: Some("2026-04-06T10:05:00".into()),
        }),
        array: None,
        log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
        batch_log: BatchLogStatus {
            path: tmpdir.path().join("slurm-12345.out"),
            present: true,
            updated_at: Some(1),
            updated_age_seconds: Some(70),
        },
        services: vec![ServiceLogStatus {
            failure_policy_mode: Some("restart_on_failure".into()),
            restart_count: Some(1),
            max_restarts: Some(3),
            window_seconds: Some(60),
            max_restarts_in_window: Some(3),
            restart_failures_in_window: Some(1),
            last_exit_code: Some(0),
            assertions: Some(ServiceAssertionStatus {
                configured: true,
                status: Some("failed".into()),
                expected_exit_code: Some(0),
                artifacts_contain: Some("/hpc-compose/job/model/*.pt".into()),
                max_duration_seconds: Some(7200),
                duration_seconds: Some(7210),
                failures: vec!["expected max_duration_seconds <= 7200, got 7210".into()],
            }),
            placement_mode: Some("distributed".into()),
            nodes: Some(2),
            ntasks: Some(4),
            ntasks_per_node: Some(2),
            nodelist: Some("node01 node02".into()),
            step_name: Some("hpc-compose:svc_name".into()),
            launcher_pid: Some(4242),
            healthy: Some(true),
            completed_successfully: Some(false),
            readiness_configured: Some(true),
            status: Some("ready".into()),
            ..sample_service_status(tmpdir.path().join(".hpc-compose/12345/logs/svc.log"))
        }],
        attempt: Some(1),
        is_resume: Some(true),
        resume_dir: Some(PathBuf::from("/shared/runs/demo")),
    };
    let mut status_out = Vec::new();
    write_status_snapshot(&mut status_out, &status).expect("status");
    let status_text = String::from_utf8(status_out).expect("utf8");
    let status_plain = strip_ansi(&status_text);
    assert!(status_plain.contains("Scheduler:"));
    assert!(status_plain.contains("  state: COMPLETED (sacct)"));
    assert!(status_plain.contains("Service outcomes:"));
    assert!(status_plain.contains("passed"));
    assert!(status_plain.contains("  note: finished"));
    assert!(status_plain.contains("  eligible time: 2026-04-06T10:00:00"));
    assert!(status_plain.contains("  start time: 2026-04-06T10:05:00"));
    assert!(status_plain.contains("Runtime:"));
    assert!(status_plain.contains("  attempt: 1"));
    assert!(status_plain.contains("  is resume: yes"));
    assert!(status_plain.contains("  resume dir: /shared/runs/demo"));
    assert!(status_plain.contains("updated: 1m ago"));
    assert!(status_plain.contains("updated: unknown"));
    assert!(status_plain.contains(
            "  state service 'svc/name': failure_policy=restart_on_failure restarts=1/3 window=1/3@60s last_exit=0"
        ));
    assert!(status_plain.contains(
            "  assert service 'svc/name': status=failed exit_code=0 artifacts_contain=/hpc-compose/job/model/*.pt duration=7210/7200s"
        ));
    assert!(
        status_plain.contains("    assertion: expected max_duration_seconds <= 7200, got 7210")
    );
    assert!(status_plain.contains(
            "  placement service 'svc/name': mode=distributed nodes=2 ntasks=4 ntasks_per_node=2 nodelist=node01 node02"
        ));

    let waiting = StatusSnapshot {
        record: record.clone(),
        scheduler: SchedulerStatus {
            state: "WAITING_FOR_ACCOUNTING".into(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "job just disappeared from squeue and has not appeared in sacct yet".into(),
            ),
        },
        queue_diagnostics: None,
        array: None,
        log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
        batch_log: BatchLogStatus {
            path: tmpdir.path().join("slurm-12345.out"),
            present: false,
            updated_at: None,
            updated_age_seconds: None,
        },
        services: Vec::new(),
        attempt: None,
        is_resume: None,
        resume_dir: None,
    };
    let mut waiting_out = Vec::new();
    write_status_snapshot(&mut waiting_out, &waiting).expect("waiting");
    let waiting_text = String::from_utf8(waiting_out).expect("utf8");
    let waiting_plain = strip_ansi(&waiting_text);
    assert!(waiting_plain.contains("  state: WAITING_FOR_ACCOUNTING (local-only)"));
    assert!(
        waiting_plain
            .contains("  note: job just disappeared from squeue and has not appeared in sacct yet")
    );
    assert!(!waiting_plain.contains("pending reason:"));
    assert!(!waiting_plain.contains("eligible time:"));
    assert!(!waiting_plain.contains("start time:"));

    let stats = StatsSnapshot {
        job_id: "12345".into(),
        record: Some(record.clone()),
        metrics_dir: Some(tmpdir.path().join(".hpc-compose/12345/metrics")),
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: Some("visible".into()),
        },
        available: true,
        reason: Some("ignored once available".into()),
        source: "sampler+sstat".into(),
        notes: vec!["note one".into()],
        sampler: Some(SamplerSnapshot {
            interval_seconds: 5,
            collectors: vec![
                CollectorStatus {
                    name: "gpu".into(),
                    enabled: true,
                    available: true,
                    note: None,
                    last_sampled_at: Some("2026-04-05T10:00:10Z".into()),
                },
                CollectorStatus {
                    name: "slurm".into(),
                    enabled: false,
                    available: false,
                    note: None,
                    last_sampled_at: None,
                },
            ],
            gpu: Some(GpuSnapshot {
                sampled_at: "2026-04-05T10:00:10Z".into(),
                nodes: vec![hpc_compose::job::GpuNodeSummary {
                    node: Some("node01".into()),
                    gpu_count: 1,
                    avg_utilization_gpu: Some(87.0),
                    memory_used_mib: Some(4096),
                    memory_total_mib: Some(8192),
                }],
                gpus: vec![GpuDeviceSample {
                    node: Some("node01".into()),
                    rank: None,
                    local_rank: None,
                    service: None,
                    collector: Some("nvidia-smi".into()),
                    index: Some("0".into()),
                    uuid: Some("GPU-0".into()),
                    name: Some("A100".into()),
                    utilization_gpu: Some("87".into()),
                    utilization_memory: Some("73".into()),
                    memory_used_mib: Some("4096".into()),
                    memory_total_mib: Some("8192".into()),
                    temperature_c: Some("55".into()),
                    power_draw_w: Some("220".into()),
                    power_limit_w: Some("300".into()),
                }],
                processes: vec![GpuProcessSample {
                    node: Some("node01".into()),
                    rank: None,
                    local_rank: None,
                    service: None,
                    collector: Some("nvidia-smi".into()),
                    gpu_uuid: Some("GPU-0".into()),
                    pid: Some("4242".into()),
                    process_name: Some("python".into()),
                    used_memory_mib: Some("2048".into()),
                }],
            }),
            slurm: None,
        }),
        steps: vec![sample_step()],
        accounting: None,
        first_failure: Some(hpc_compose::job::FirstFailure {
            service: "trainer".into(),
            exit_code: 42,
            at_unix: Some(1_774_000_000),
            node: Some("node01".into()),
            rank: None,
        }),
        attempt: Some(1),
        is_resume: Some(true),
        resume_dir: Some(PathBuf::from("/shared/runs/demo")),
    };
    let mut stats_out = Vec::new();
    write_stats_snapshot(&mut stats_out, &stats).expect("stats");
    let stats_text = String::from_utf8(stats_out).expect("utf8");
    assert!(stats_text.contains("collector 'gpu': available"));
    assert!(stats_text.contains("attempt: 1"));
    assert!(stats_text.contains("is resume: yes"));
    assert!(stats_text.contains("resume dir: /shared/runs/demo"));
    assert!(!stats_text.contains("collector 'slurm'"));
    assert!(stats_text.contains("gpu snapshot: 2026-04-05T10:00:10Z"));
    assert!(stats_text.contains("gpu node node01"));
    assert!(stats_text.contains("first failure: service=trainer"));
    assert!(stats_text.contains("gpu process: pid=4242"));
    assert!(stats_text.contains("gpu count: 1"));

    let mut csv_out = Vec::new();
    write_stats_snapshot_csv(&mut csv_out, &stats).expect("csv");
    let csv_text = String::from_utf8(csv_out).expect("utf8");
    assert!(csv_text.contains("job_id,scheduler_state,scheduler_source,stats_source"));
    assert!(csv_text.contains("\"12345\",\"RUNNING\",\"squeue\",\"sampler+sstat\""));
    assert!(csv_text.contains("\"12345.0\""));

    let mut jsonl_out = Vec::new();
    write_stats_snapshot_jsonl(&mut jsonl_out, &stats).expect("jsonl");
    let jsonl_text = String::from_utf8(jsonl_out).expect("utf8");
    assert!(jsonl_text.contains("\"record_type\":\"summary\""));
    assert!(jsonl_text.contains("\"record_type\":\"collector\""));
    assert!(jsonl_text.contains("\"record_type\":\"gpu_device\""));
    assert!(jsonl_text.contains("\"record_type\":\"gpu_process\""));
    assert!(jsonl_text.contains("\"record_type\":\"step\""));
    assert!(jsonl_text.contains("\"attempt\":1"));
    assert!(jsonl_text.contains("\"is_resume\":true"));

    let unavailable_stats = StatsSnapshot {
        available: false,
        sampler: None,
        steps: Vec::new(),
        accounting: None,
        first_failure: None,
        source: "sstat".into(),
        notes: Vec::new(),
        reason: Some("job is pending".into()),
        metrics_dir: None,
        record: None,
        job_id: "12345".into(),
        scheduler: SchedulerStatus {
            state: "PENDING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: None,
        },
        attempt: None,
        is_resume: None,
        resume_dir: None,
    };
    let mut unavailable_out = Vec::new();
    write_stats_snapshot(&mut unavailable_out, &unavailable_stats).expect("stats");
    let unavailable_text = String::from_utf8(unavailable_out).expect("utf8");
    assert!(unavailable_text.contains("stats reason: job is pending"));
    assert!(!unavailable_text.contains("step: "));

    let mut unavailable_csv = Vec::new();
    write_stats_snapshot_csv(&mut unavailable_csv, &unavailable_stats).expect("csv");
    assert_eq!(
        String::from_utf8(unavailable_csv).expect("utf8"),
        "job_id,scheduler_state,scheduler_source,stats_source,step_id,ntasks,ave_cpu,ave_rss,max_rss,alloc_tres,tres_usage_in_ave,gpu_count,gpu_util,gpu_mem,alloc_tres_map,usage_tres_in_ave_map\n"
    );

    let report = ArtifactExportReport {
        record: record.clone(),
        manifest_path: tmpdir.path().join("manifest.json"),
        payload_dir: tmpdir.path().join("payload"),
        export_dir: tmpdir.path().join("results"),
        manifest: ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: Some(1),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/runs/demo")),
            declared_source_patterns: vec!["/x/**".into()],
            matched_source_paths: vec!["/x/a".into()],
            copied_relative_paths: vec!["a".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([(
                "default".into(),
                hpc_compose::job::ArtifactBundleManifest {
                    declared_source_patterns: vec!["/x/**".into()],
                    matched_source_paths: vec!["/x/a".into()],
                    copied_relative_paths: vec!["a".into()],
                    warnings: Vec::new(),
                },
            )]),
        },
        selected_bundles: vec!["default".into()],
        bundles: Vec::new(),
        exported_paths: vec![tmpdir.path().join("results/a")],
        tarball_paths: Vec::new(),
        warnings: vec!["missing optional path".into()],
    };
    let mut report_out = Vec::new();
    write_artifact_export_report(&mut report_out, &report).expect("artifacts");
    let report_text = String::from_utf8(report_out).expect("utf8");
    let report_plain = strip_ansi(&report_text);
    assert!(report_plain.contains("collect policy: always"));
    assert!(report_plain.contains("attempt: 1"));
    assert!(report_plain.contains("is resume: yes"));
    assert!(report_plain.contains("resume dir: /shared/runs/demo"));
    assert!(report_plain.contains("warning: missing optional path"));
    assert!(report_plain.contains("exported: "));

    let plan_model = hpc_compose::planner::Plan {
        spec_path: tmpdir.path().join("compose.yaml"),
        project_dir: tmpdir.path().to_path_buf(),
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: hpc_compose::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![hpc_compose::planner::PlannedService {
            name: service.name.clone(),
            image: service.source.clone(),
            execution: service.execution.clone(),
            environment: service.environment.clone(),
            volumes: service.volumes.clone(),
            working_dir: service.working_dir.clone(),
            depends_on: service.depends_on.clone(),
            readiness: service.readiness.clone(),
            assertions: service.assertions.clone(),
            failure_policy: service.failure_policy.clone(),
            placement: service.placement.clone(),
            slurm: service.slurm.clone(),
            prepare: service.prepare.clone(),
        }],
    };
    let mut inspect_out = Vec::new();
    write_plan_inspect_verbose(&mut inspect_out, &plan_model, &plan, None).expect("inspect");
    let inspect_text = String::from_utf8(inspect_out).expect("utf8");
    assert!(inspect_text.contains("execution form: shell"));
    assert!(inspect_text.contains("depends_on: db(service_healthy)"));
    assert!(
        inspect_text
            .contains("readiness: http http://127.0.0.1:8000/health (status 200 timeout 30s)")
    );
    assert!(inspect_text.contains("rebuild reason: prepare.mounts are present"));
}

#[test]
fn helper_functions_cover_remaining_formatting_paths() {
    assert_eq!(display_stats_value(""), "unknown");
    assert_eq!(display_stats_value("5"), "5");
    assert_eq!(display_optional_stats_value(None), "unknown");
    assert_eq!(display_optional_stats_value(Some("")), "unknown");
    assert_eq!(display_optional_stats_value(Some("x")), "x");
    assert_eq!(
        execution_form_label(&ExecutionSpec::ImageDefault),
        "image-default"
    );
    assert_eq!(
        execution_form_label(&ExecutionSpec::Shell("echo".into())),
        "shell"
    );
    assert_eq!(
        execution_form_label(&ExecutionSpec::Exec(vec!["echo".into()])),
        "exec"
    );
    assert_eq!(readiness_description(None), "none");
    assert_eq!(
        readiness_description(Some(&ReadinessSpec::Sleep { seconds: 5 })),
        "sleep 5s"
    );
    assert_eq!(
        readiness_description(Some(&ReadinessSpec::Tcp {
            host: None,
            port: 5432,
            timeout_seconds: None,
        })),
        "tcp 127.0.0.1:5432 (timeout 60s)"
    );
    assert_eq!(
        readiness_description(Some(&ReadinessSpec::Log {
            pattern: "ready".into(),
            timeout_seconds: Some(9),
        })),
        "log 'ready' (timeout 9s)"
    );
    assert_eq!(format_age_seconds(59), "59s ago");
    assert_eq!(format_age_seconds(61), "1m ago");
    assert_eq!(format_age_seconds(7_200), "2h ago");
    assert_eq!(format_age_seconds(172_800), "2d ago");
    assert_eq!(
        format_dependencies(&[
            ServiceDependency {
                name: "db".into(),
                condition: DependencyCondition::ServiceStarted,
                implicit: false,
            },
            ServiceDependency {
                name: "cache".into(),
                condition: DependencyCondition::ServiceHealthy,
                implicit: false,
            },
        ]),
        "db(service_started),cache(service_healthy)"
    );

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let runtime_image = tmpdir.path().join("runtime.sqsh");
    let service = runtime_service(
        ImageSource::Remote("docker://redis:7".into()),
        runtime_image.clone(),
        Some(PreparedImageSpec {
            commands: vec!["echo hi".into()],
            mounts: Vec::new(),
            env: Vec::new(),
            root: true,
            force_rebuild: false,
        }),
    );
    assert_eq!(
        rebuild_reason(&service),
        Some("runtime cache artifact is missing")
    );
    fs::write(&runtime_image, "x").expect("runtime");
    assert_eq!(rebuild_reason(&service), None);
}

#[test]
fn resolve_init_answers_and_cancel_job_cover_remaining_paths() {
    let answers = resolve_init_answers(Some("dev-python-app".into()), None, None, || {
        unreachable!("template path should not prompt")
    })
    .expect("template answers without cache dir");
    assert_eq!(answers.cache_dir, None);

    let answers = resolve_init_answers(
        Some("dev-python-app".into()),
        None,
        Some("/cache".into()),
        || unreachable!("template path should not prompt"),
    )
    .expect("template answers");
    assert_eq!(answers.app_name, "dev-python-app");
    assert_eq!(answers.cache_dir, Some("/cache".into()));

    let err = resolve_init_answers(
        Some("dev-python-app".into()),
        None,
        Some("   ".into()),
        || unreachable!("template path should not prompt"),
    )
    .expect_err("blank cache dir");
    assert!(err.to_string().contains("--cache-dir cannot be empty"));

    let prompted =
        resolve_init_answers(None, Some("override".into()), Some("/cache".into()), || {
            Ok(hpc_compose::init::InitAnswers {
                template_name: "app-redis-worker".into(),
                app_name: "prompted".into(),
                cache_dir: Some("/default".into()),
            })
        })
        .expect("prompted");
    assert_eq!(prompted.app_name, "override");
    assert_eq!(prompted.cache_dir, Some("/cache".into()));

    let err = resolve_init_answers(None, None, Some("   ".into()), || {
        Ok(hpc_compose::init::InitAnswers {
            template_name: "app-redis-worker".into(),
            app_name: "prompted".into(),
            cache_dir: Some("/default".into()),
        })
    })
    .expect_err("blank prompted override");
    assert!(err.to_string().contains("--cache-dir cannot be empty"));

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let empty_fail = tmpdir.path().join("scancel-empty");
    write_script(&empty_fail, "#!/bin/bash\nset -euo pipefail\nexit 1\n");
    let err = cancel_job("42", empty_fail.to_str().expect("path")).expect_err("empty fail");
    assert_eq!(err.to_string(), "scancel failed for job 42");

    let stderr_fail = tmpdir.path().join("scancel-stderr");
    write_script(
        &stderr_fail,
        "#!/bin/bash\nset -euo pipefail\necho boom >&2\nexit 1\n",
    );
    let err = cancel_job("42", stderr_fail.to_str().expect("path")).expect_err("stderr fail");
    assert!(err.to_string().contains("scancel failed for job 42: boom"));

    let err = cancel_job(
        "42",
        tmpdir.path().join("missing-bin").to_str().expect("path"),
    )
    .expect_err("missing binary");
    assert!(err.to_string().contains("failed to execute"));
}

#[test]
fn stdout_entrypoints_cover_public_output_wrappers() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_valid_compose(tmpdir.path(), &cache_dir);
    let plan = load_plan(&compose).expect("plan");
    let runtime = build_runtime_plan(&plan);
    let record = submission_record(tmpdir.path(), &runtime, "12345");

    let status = StatusSnapshot {
        record: record.clone(),
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: Some("visible".into()),
        },
        queue_diagnostics: Some(QueueDiagnostics {
            pending_reason: None,
            eligible_time: Some("2026-04-06T10:00:00".into()),
            start_time: Some("2026-04-06T10:05:00".into()),
        }),
        array: None,
        log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
        batch_log: BatchLogStatus {
            path: tmpdir.path().join("slurm-12345.out"),
            present: false,
            updated_at: None,
            updated_age_seconds: None,
        },
        services: Vec::new(),
        attempt: Some(1),
        is_resume: Some(false),
        resume_dir: None,
    };

    let stats = StatsSnapshot {
        job_id: "12345".into(),
        record: Some(record.clone()),
        metrics_dir: Some(tmpdir.path().join(".hpc-compose/12345/metrics")),
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: None,
        },
        available: true,
        reason: None,
        source: "sstat".into(),
        notes: Vec::new(),
        sampler: None,
        steps: vec![sample_step()],
        accounting: None,
        first_failure: None,
        attempt: Some(1),
        is_resume: Some(false),
        resume_dir: None,
    };

    let artifact_report = ArtifactExportReport {
        record: record.clone(),
        manifest_path: tmpdir.path().join("manifest.json"),
        payload_dir: tmpdir.path().join("payload"),
        export_dir: tmpdir.path().join("results"),
        manifest: ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: Some(1),
            is_resume: Some(false),
            resume_dir: None,
            declared_source_patterns: vec!["/x/**".into()],
            matched_source_paths: vec!["/x/a".into()],
            copied_relative_paths: vec!["a".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::new(),
        },
        selected_bundles: vec!["default".into()],
        bundles: Vec::new(),
        exported_paths: vec![tmpdir.path().join("results/a")],
        tarball_paths: Vec::new(),
        warnings: Vec::new(),
    };

    let inventory = JobInventoryEntry {
        compose_file: compose.clone(),
        compose_metadata_root: tmpdir.path().join(".hpc-compose"),
        job_id: "12345".into(),
        kind: SubmissionKind::Main,
        is_latest: true,
        submitted_at: 1_775_807_600,
        age_seconds: 42,
        submit_dir: tmpdir.path().to_path_buf(),
        record_path: tmpdir.path().join(".hpc-compose/jobs/12345.json"),
        runtime_job_root: tmpdir.path().join(".hpc-compose/12345"),
        runtime_job_root_present: true,
        legacy_runtime_job_root: tmpdir.path().join(".hpc-compose/legacy/12345"),
        legacy_runtime_job_root_present: false,
        runtime_cache_dir: tmpdir.path().join("cache/runtime/12345"),
        runtime_cache_dir_present: false,
        batch_log: tmpdir
            .path()
            .join(".hpc-compose/logs/hpc-compose-12345.out"),
        batch_log_managed: true,
        disk_usage_bytes: Some(2_048),
    };
    let scan = JobInventoryScan {
        scan_root: tmpdir.path().to_path_buf(),
        jobs: vec![inventory.clone()],
    };
    let cleanup = CleanupReport {
        compose_file: compose,
        mode: "age".into(),
        dry_run: true,
        removed_job_ids: vec!["12345".into()],
        kept_job_ids: vec!["67890".into()],
        latest_pointer_job_id_before: Some("12345".into()),
        latest_job_id_before: Some("12345".into()),
        latest_job_id_after: Some("67890".into()),
        total_bytes_reclaimed: Some(2_048),
        jobs: vec![CleanupJobReport {
            inventory,
            selected: true,
            bytes_reclaimed: Some(2_048),
            removable_paths: vec![tmpdir.path().join(".hpc-compose/jobs/12345.json")],
        }],
    };

    assert_eq!(
        resolve_stats_output_format(None, false),
        StatsOutputFormat::Text
    );
    assert_eq!(
        resolve_stats_output_format(Some(StatsOutputFormat::Csv), false),
        StatsOutputFormat::Csv
    );
    assert_eq!(
        resolve_stats_output_format(Some(StatsOutputFormat::Text), true),
        StatsOutputFormat::Json
    );

    print_status_snapshot(&status).expect("print status snapshot");
    print_stats_snapshot(&stats).expect("print stats snapshot");
    print_artifact_export_report(&artifact_report).expect("print artifact export report");
    print_plan_inspect_verbose(&plan, &runtime).expect("print verbose inspect");
    print_job_inventory_scan(&scan, true).expect("print job inventory");
    print_cleanup_report(&cleanup, true).expect("print cleanup report");
    print_template_list();
    print_template_description("dev-python-app").expect("template description");
}

#[test]
fn run_command_covers_success_and_error_arms() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_valid_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch_ok = write_fake_sbatch(tmpdir.path(), true);
    let sbatch_fail = write_fake_sbatch(tmpdir.path(), false);
    let empty_cache = tmpdir.path().join("empty-cache");
    fs::create_dir_all(&empty_cache).expect("empty cache");
    let no_id_sbatch = tmpdir.path().join("sbatch-no-id");
    write_script(
        &no_id_sbatch,
        "#!/bin/bash\nset -euo pipefail\necho 'submitted without id'\n",
    );
    let scancel_ok = tmpdir.path().join("scancel-ok");
    write_script(
        &scancel_ok,
        "#!/bin/bash\nset -euo pipefail\necho 'cancel ok'\n",
    );
    let scancel_fail = tmpdir.path().join("scancel-fail");
    write_script(
        &scancel_fail,
        "#!/bin/bash\nset -euo pipefail\necho 'denied' >&2\nexit 1\n",
    );

    run_command(Commands::Validate {
        file: Some(compose.clone()),
        strict_env: false,
        format: None,
    })
    .expect("validate");
    run_command(Commands::Render {
        file: Some(compose.clone()),
        output: None,
        format: None,
    })
    .expect("render stdout");
    let rendered = tmpdir.path().join("rendered.sbatch");
    run_command(Commands::Render {
        file: Some(compose.clone()),
        output: Some(rendered.clone()),
        format: None,
    })
    .expect("render file");
    assert!(rendered.exists());
    let render_err = run_command(Commands::Render {
        file: Some(compose.clone()),
        output: Some(tmpdir.path().join("missing-parent/rendered.sbatch")),
        format: None,
    })
    .expect_err("render write failure");
    assert!(
        render_err
            .to_string()
            .contains("failed to write rendered script")
    );

    run_command(Commands::Prepare {
        file: Some(compose.clone()),
        enroot_bin: enroot.display().to_string(),
        apptainer_bin: "apptainer".into(),
        singularity_bin: "singularity".into(),
        huggingface_cli_bin: "huggingface-cli".into(),
        keep_failed_prep: false,
        force_rebuild: true,
        force_deprecated: false,
        format: None,
    })
    .expect("prepare");

    let err = run_command(Commands::Preflight {
        file: Some(compose.clone()),
        strict: true,
        verbose: false,
        format: None,
        enroot_bin: enroot.display().to_string(),
        apptainer_bin: "apptainer".into(),
        singularity_bin: "singularity".into(),
        sbatch_bin: sbatch_ok.display().to_string(),
        srun_bin: srun.display().to_string(),
        scontrol_bin: "scontrol".into(),
    })
    .expect_err("strict warnings");
    assert!(err.to_string().contains("preflight reported warnings"));
    run_command(Commands::Preflight {
        file: Some(compose.clone()),
        strict: false,
        verbose: false,
        format: None,
        enroot_bin: enroot.display().to_string(),
        apptainer_bin: "apptainer".into(),
        singularity_bin: "singularity".into(),
        sbatch_bin: sbatch_ok.display().to_string(),
        srun_bin: srun.display().to_string(),
        scontrol_bin: "scontrol".into(),
    })
    .expect("non-strict preflight");

    run_command(Commands::Inspect {
        file: Some(compose.clone()),
        verbose: false,
        tree: false,
        rightsize: false,
        dependencies: false,
        dependencies_format: hpc_compose::cli::DependencyOutputFormat::Text,
        job_id: None,
        sstat_bin: "sstat".into(),
        squeue_bin: "squeue".into(),
        sacct_bin: "sacct".into(),
        format: None,
    })
    .expect("inspect");

    let err = run_command(Commands::Up {
        launch: RuntimeLaunchArgs {
            file: Some(compose.clone()),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            huggingface_cli_bin: "huggingface-cli".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        },
        script_out: None,
        sbatch_bin: sbatch_fail.display().to_string(),
        srun_bin: srun.display().to_string(),
        squeue_bin: "squeue".into(),
        sacct_bin: "sacct".into(),
        local: false,
        allow_resume_changes: false,
        resume_diff_only: false,
        dry_run: false,
        detach: true,
        watch_queue: false,
        queue_warn_after: None,
        watch_mode: WatchMode::Auto,
        hold_on_exit: HoldOnExit::Failure,
        format: None,
        print_endpoints: false,
    })
    .expect_err("sbatch fail");
    assert!(err.to_string().contains("sbatch failed"));

    run_command(Commands::Up {
        launch: RuntimeLaunchArgs {
            file: Some(compose.clone()),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            huggingface_cli_bin: "huggingface-cli".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: false,
        },
        script_out: Some(tmpdir.path().join("submit.sbatch")),
        sbatch_bin: sbatch_ok.display().to_string(),
        srun_bin: srun.display().to_string(),
        squeue_bin: "squeue".into(),
        sacct_bin: "sacct".into(),
        local: false,
        allow_resume_changes: false,
        resume_diff_only: false,
        dry_run: false,
        detach: true,
        watch_queue: false,
        queue_warn_after: None,
        watch_mode: WatchMode::Auto,
        hold_on_exit: HoldOnExit::Failure,
        format: None,
        print_endpoints: false,
    })
    .expect("submit");
    run_command(Commands::Up {
        launch: RuntimeLaunchArgs {
            file: Some(compose.clone()),
            enroot_bin: enroot.display().to_string(),
            apptainer_bin: "apptainer".into(),
            singularity_bin: "singularity".into(),
            huggingface_cli_bin: "huggingface-cli".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        },
        script_out: Some(tmpdir.path().join("submit-no-id.sbatch")),
        sbatch_bin: no_id_sbatch.display().to_string(),
        srun_bin: srun.display().to_string(),
        squeue_bin: "squeue".into(),
        sacct_bin: "sacct".into(),
        local: false,
        allow_resume_changes: false,
        resume_diff_only: false,
        dry_run: false,
        detach: true,
        watch_queue: false,
        queue_warn_after: None,
        watch_mode: WatchMode::Auto,
        hold_on_exit: HoldOnExit::Failure,
        format: None,
        print_endpoints: false,
    })
    .expect("submit without id");

    run_command(Commands::Cache {
        command: CacheCommands::List {
            cache_dir: Some(cache_dir.clone()),
            format: None,
        },
    })
    .expect("cache list");
    run_command(Commands::Cache {
        command: CacheCommands::List {
            cache_dir: Some(empty_cache),
            format: None,
        },
    })
    .expect("cache list empty");
    run_command(Commands::Cache {
        command: CacheCommands::Inspect {
            file: Some(compose.clone()),
            service: Some("app".into()),
            format: None,
        },
    })
    .expect("cache inspect");
    let err = run_command(Commands::Cache {
        command: CacheCommands::Prune {
            file: None,
            cache_dir: Some(cache_dir.clone()),
            age: None,
            all_unused: true,
            yes: false,
            format: None,
        },
    })
    .expect_err("missing file");
    assert!(err.to_string().contains("--all-unused requires -f/--file"));
    let err = run_command(Commands::Cache {
        command: CacheCommands::Prune {
            file: Some(compose.clone()),
            cache_dir: Some(cache_dir.clone()),
            age: Some(7),
            all_unused: true,
            yes: false,
            format: None,
        },
    })
    .expect_err("conflicting strategies");
    assert!(
        err.to_string()
            .contains("cache prune accepts only one strategy at a time")
    );
    run_command(Commands::Cache {
        command: CacheCommands::Prune {
            file: None,
            cache_dir: Some(cache_dir),
            age: Some(999),
            all_unused: false,
            yes: true,
            format: None,
        },
    })
    .expect("prune age");
    run_command(Commands::Cache {
        command: CacheCommands::Prune {
            file: Some(compose.clone()),
            cache_dir: None,
            age: None,
            all_unused: true,
            yes: true,
            format: None,
        },
    })
    .expect("prune all unused");

    run_command(Commands::Cancel {
        file: Some(compose.clone()),
        job_id: Some("12345".into()),
        scancel_bin: scancel_ok.display().to_string(),
        purge_cache: false,
        yes: false,
        format: None,
    })
    .expect("cancel ok");
    let cancel_err = run_command(Commands::Cancel {
        file: Some(compose.clone()),
        job_id: Some("12345".into()),
        scancel_bin: scancel_fail.display().to_string(),
        purge_cache: false,
        yes: false,
        format: None,
    })
    .expect_err("cancel fail");
    assert!(
        cancel_err
            .to_string()
            .contains("scancel failed for job 12345")
    );

    let init_output = tmpdir.path().join("init-compose.yaml");
    run_command(Commands::New {
        template: Some("dev-python-app".into()),
        list_templates: false,
        describe_template: None,
        name: Some("custom-init".into()),
        cache_dir: Some("/tmp/custom-cache".into()),
        output: init_output.clone(),
        force: true,
        format: None,
    })
    .expect("init");
    assert!(init_output.exists());
}

#[test]
fn write_status_snapshot_omits_window_for_non_restart_or_legacy_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let plan = RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.path().join("cache"),
        runtime: crate::spec::RuntimeConfig::default(),
        slurm: SlurmConfig::default(),
        ordered_services: vec![runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("prepared.sqsh"),
            None,
        )],
    };
    let record = submission_record(tmpdir.path(), &plan, "12345");
    let status = StatusSnapshot {
        record,
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: None,
        },
        queue_diagnostics: None,
        array: None,
        log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
        batch_log: BatchLogStatus {
            path: tmpdir.path().join("slurm-12345.out"),
            present: true,
            updated_at: Some(1),
            updated_age_seconds: Some(1),
        },
        services: vec![
            ServiceLogStatus {
                service_name: "ignore".into(),
                failure_policy_mode: Some("ignore".into()),
                restart_count: Some(0),
                max_restarts: Some(0),
                window_seconds: Some(0),
                max_restarts_in_window: Some(0),
                restart_failures_in_window: Some(0),
                last_exit_code: Some(42),
                placement_mode: None,
                nodes: None,
                ntasks: None,
                ntasks_per_node: None,
                nodelist: None,
                status: Some("failed".into()),
                present: true,
                updated_at: Some(1),
                updated_age_seconds: Some(1),
                ..sample_service_status(tmpdir.path().join(".hpc-compose/12345/logs/ignore.log"))
            },
            ServiceLogStatus {
                service_name: "legacy".into(),
                failure_policy_mode: Some("restart_on_failure".into()),
                restart_count: Some(1),
                max_restarts: Some(3),
                window_seconds: None,
                max_restarts_in_window: None,
                restart_failures_in_window: None,
                last_exit_code: Some(17),
                started_at: None,
                finished_at: None,
                duration_seconds: None,
                assertions: None,
                placement_mode: None,
                nodes: None,
                ntasks: None,
                ntasks_per_node: None,
                nodelist: None,
                status: Some("failed".into()),
                present: true,
                updated_at: Some(1),
                updated_age_seconds: Some(1),
                path: tmpdir.path().join(".hpc-compose/12345/logs/legacy.log"),
                log_path: None,
                step_name: None,
                launch_index: None,
                launcher_pid: None,
                healthy: None,
                completed_successfully: None,
                readiness_configured: None,
            },
        ],
        attempt: None,
        is_resume: None,
        resume_dir: None,
    };
    let mut status_out = Vec::new();
    write_status_snapshot(&mut status_out, &status).expect("status");
    let status_text = String::from_utf8(status_out).expect("utf8");
    assert!(
        status_text
            .contains("  state service 'ignore': failure_policy=ignore restarts=0/0 last_exit=42")
    );
    assert!(status_text.contains(
        "  state service 'legacy': failure_policy=restart_on_failure restarts=1/3 last_exit=17"
    ));
    assert!(!status_text.contains("window=0/0@0s"));
    assert!(!status_text.contains("window=unknown/unknown@unknowns"));
}

#[test]
fn inspect_tree_preserves_indentation_for_root_descendants() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        r#"
name: demo
x-slurm:
  cache_dir: ./cache
services:
  root:
    image: redis:7
    command: /bin/true
  child:
    image: redis:7
    command: /bin/true
    depends_on:
      root:
        condition: service_started
  grandchild:
    image: redis:7
    command: /bin/true
    depends_on:
      child:
        condition: service_started
"#,
    );
    let plan = load_plan(&compose).expect("plan");
    let runtime_plan = build_runtime_plan(&plan);
    let mut out = Vec::new();
    write_plan_inspect_tree(&mut out, &plan, &runtime_plan).expect("tree");
    let text = String::from_utf8(out).expect("utf8");
    let lines: Vec<&str> = text.lines().collect();

    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("    └── ") && line.contains("child")),
        "{text}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("        └── ") && line.contains("grandchild")),
        "{text}"
    );
}

#[test]
fn score_bar_fills_proportionally_and_clamps() {
    assert_eq!(score_bar(0.0), "..........");
    assert_eq!(score_bar(1.0), "##########");
    assert_eq!(score_bar(0.5), "#####.....");
    // Out-of-range utilization is clamped to [0, 1].
    assert_eq!(score_bar(2.0), "##########");
    assert_eq!(score_bar(-1.0), "..........");
}

#[test]
fn clip_ascii_truncates_with_ellipsis_and_handles_narrow_widths() {
    assert_eq!(clip_ascii("hello", 10), "hello");
    assert_eq!(clip_ascii("abc", 3), "abc");
    assert_eq!(clip_ascii("hello world", 8), "hello...");
    // Widths too small for an ellipsis collapse to dots.
    assert_eq!(clip_ascii("hello", 2), "..");
}

#[test]
fn wrap_score_card_text_wraps_on_width_and_keeps_one_empty_line() {
    assert_eq!(
        wrap_score_card_text("the quick brown fox", 9),
        vec!["the quick".to_string(), "brown fox".to_string()]
    );
    assert_eq!(wrap_score_card_text("", 5), vec![String::new()]);
}

#[test]
fn score_confidence_label_covers_all_variants() {
    use hpc_compose::job::EfficiencyScoreConfidence;
    assert_eq!(
        score_confidence_label(EfficiencyScoreConfidence::High),
        "high"
    );
    assert_eq!(
        score_confidence_label(EfficiencyScoreConfidence::Medium),
        "medium"
    );
    assert_eq!(
        score_confidence_label(EfficiencyScoreConfidence::Low),
        "low"
    );
}

#[test]
fn format_bytes_scales_units_and_keeps_raw_byte_counts() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(1023), "1023 B");
    assert_eq!(format_bytes(1024), "1.0 KiB");
    assert_eq!(format_bytes(1536), "1.5 KiB");
    assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
    assert_eq!(format_bytes(1024u64.pow(4)), "1.0 TiB");
    assert_eq!(format_bytes(2 * 1024u64.pow(4)), "2.0 TiB");
}

#[test]
fn format_compact_elapsed_covers_each_duration_band() {
    assert_eq!(format_compact_elapsed(0), "0s");
    assert_eq!(format_compact_elapsed(59), "59s");
    assert_eq!(format_compact_elapsed(61), "1m1s");
    assert_eq!(format_compact_elapsed(3_600), "1h0m");
    assert_eq!(format_compact_elapsed(3_661), "1h1m");
    assert_eq!(format_compact_elapsed(90_000), "1d1h");
}

#[test]
fn confidence_label_covers_all_rightsize_variants() {
    use hpc_compose::job::RightsizeConfidence;
    assert_eq!(confidence_label(RightsizeConfidence::High), "high");
    assert_eq!(confidence_label(RightsizeConfidence::Medium), "medium");
    assert_eq!(confidence_label(RightsizeConfidence::Low), "low");
}

#[test]
fn runtime_presence_label_covers_all_combinations() {
    assert_eq!(runtime_presence_label(true, true), "runtime+legacy");
    assert_eq!(runtime_presence_label(true, false), "runtime");
    assert_eq!(runtime_presence_label(false, true), "legacy");
    assert_eq!(runtime_presence_label(false, false), "missing");
}

#[test]
fn csv_field_and_format_tres_map_quote_and_join() {
    assert_eq!(csv_field("plain"), "\"plain\"");
    assert_eq!(csv_field("a\"b"), "\"a\"\"b\"");
    assert_eq!(csv_field(""), "\"\"");

    let mut map = std::collections::BTreeMap::new();
    map.insert("gres/gpu".to_string(), "1".to_string());
    map.insert("cpu".to_string(), "4".to_string());
    assert_eq!(format_tres_map(&map), "cpu=4;gres/gpu=1");
    assert_eq!(format_tres_map(&std::collections::BTreeMap::new()), "");
}

#[test]
fn display_optional_f64_formats_to_six_decimals_or_dash() {
    assert_eq!(display_optional_f64(None), "-");
    assert_eq!(display_optional_f64(Some(1.5)), "1.500000");
    assert_eq!(display_optional_f64(Some(0.0)), "0.000000");
}

#[test]
fn dot_escape_escapes_graphviz_special_characters() {
    assert_eq!(dot_escape("plain"), "plain");
    assert_eq!(dot_escape("a\"b"), "a\\\"b");
    assert_eq!(dot_escape("a\\b"), "a\\\\b");
    assert_eq!(dot_escape("a\nb\tc\rd"), "a\\nb\\tc\\rd");
}

#[test]
fn http_host_port_handles_scheme_defaults_ipv6_and_userinfo() {
    assert_eq!(
        http_host_port("http://node02:9000/health"),
        ("node02".to_string(), 9000)
    );
    assert_eq!(http_host_port("https://x/"), ("x".to_string(), 443));
    assert_eq!(http_host_port("http://y/"), ("y".to_string(), 80));
    assert_eq!(
        http_host_port("http://user:pass@host:1234/p"),
        ("host".to_string(), 1234)
    );
    assert_eq!(
        http_host_port("http://[::1]:8080/"),
        ("::1".to_string(), 8080)
    );
    assert_eq!(http_host_port("garbage"), ("<host>".to_string(), 80));
}

#[test]
fn submit_next_commands_parameterizes_job_id_and_orders_pull_before_down() {
    let with_id = submit_next_commands(Some("12345"));
    assert_eq!(
        with_id,
        vec![
            "hpc-compose status --job-id 12345".to_string(),
            "hpc-compose logs --follow".to_string(),
            "hpc-compose stats --job-id 12345".to_string(),
            "hpc-compose pull --job-id 12345".to_string(),
            "hpc-compose down".to_string(),
        ]
    );
    // pull is suggested before the destructive down so results are collected first.
    let pull = with_id
        .iter()
        .position(|c| c.starts_with("hpc-compose pull"))
        .expect("pull present");
    let down = with_id
        .iter()
        .position(|c| c == "hpc-compose down")
        .expect("down present");
    assert!(pull < down, "pull must precede down");

    // Without a job id, no --job-id is appended to any suggestion.
    let without_id = submit_next_commands(None);
    assert!(without_id.iter().all(|c| !c.contains("--job-id")));
    assert!(without_id.contains(&"hpc-compose status".to_string()));
}

#[test]
fn print_next_steps_is_noop_when_empty() {
    // Empty list prints nothing and does not panic.
    print_next_steps(&[]);
}

#[test]
fn inspect_next_commands_omit_status_and_parameterize_job_id() {
    let cmds = inspect_next_commands(Some("777"));
    assert!(
        cmds.iter().all(|c| !c.starts_with("hpc-compose status")),
        "inspect hints must not re-suggest status: {cmds:?}"
    );
    assert!(cmds.contains(&"hpc-compose stats --job-id 777".to_string()));
    assert!(cmds.contains(&"hpc-compose pull --job-id 777".to_string()));
    assert!(cmds.contains(&"hpc-compose down".to_string()));
}
