use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use super::*;
use hpc_compose::context::{ResolvedBinaries, ResolvedValue, ValueSource};

fn write_compose(root: &Path) -> PathBuf {
    let compose = root.join("compose.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: docker://redis:7\nx-slurm:\n  cache_dir: {}\n",
                root.join("cache").display()
            ),
        )
        .expect("write compose");
    compose
}

fn write_local_compose(root: &Path) -> PathBuf {
    let local_image = root.join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = root.join("compose-local.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                root.join("cache-local").display()
            ),
        )
        .expect("write local compose");
    compose
}

fn write_local_compose_with_services(root: &Path) -> PathBuf {
    let local_image = root.join("local-rich.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = root.join("compose-local-rich.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {}\n    command: /bin/true\n    readiness:\n      type: log\n      pattern: ready\n      timeout_seconds: 5\n  worker:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                local_image.display(),
                root.join("cache-rich").display()
            ),
        )
        .expect("write rich local compose");
    compose
}

fn resolved_string(value: &str) -> ResolvedValue<String> {
    ResolvedValue {
        value: value.to_string(),
        source: ValueSource::Cli,
    }
}

fn context_for(compose: &Path, cwd: &Path) -> ResolvedContext {
    ResolvedContext {
        cwd: cwd.to_path_buf(),
        settings_path: None,
        settings_base_dir: None,
        selected_profile: None,
        compose_file: ResolvedValue {
            value: compose.to_path_buf(),
            source: ValueSource::Cli,
        },
        cache_dir: ResolvedValue {
            value: cwd.join(".cache/hpc-compose"),
            source: ValueSource::Builtin,
        },
        resource_profiles: BTreeMap::new(),
        binaries: ResolvedBinaries {
            enroot: resolved_string("/definitely/missing-enroot"),
            apptainer: resolved_string("/definitely/missing-apptainer"),
            singularity: resolved_string("/definitely/missing-singularity"),
            salloc: resolved_string("/definitely/missing-salloc"),
            sbatch: resolved_string("/definitely/missing-sbatch"),
            srun: resolved_string("/definitely/missing-srun"),
            scontrol: resolved_string("/definitely/missing-scontrol"),
            sinfo: resolved_string("/definitely/missing-sinfo"),
            squeue: resolved_string("/definitely/missing-squeue"),
            sacct: resolved_string("/definitely/missing-sacct"),
            sstat: resolved_string("/definitely/missing-sstat"),
            scancel: resolved_string("/definitely/missing-scancel"),
            sshare: resolved_string("/definitely/missing-sshare"),
            sprio: resolved_string("/definitely/missing-sprio"),
        },
        interpolation_vars: BTreeMap::new(),
        interpolation_var_sources: BTreeMap::new(),
        watch: Default::default(),
    }
}

#[test]
fn detect_dev_changes_reports_modified_targets_once() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = tmpdir.path().to_path_buf();
    fs::write(root.join("a.txt"), "one").expect("write");
    let snapshot = collect_dev_snapshot(&root).expect("snapshot");
    let mut targets = vec![DevWatchTarget {
        root: root.clone(),
        services: BTreeSet::from(["api".to_string()]),
        snapshot,
    }];

    // No change yet.
    assert!(detect_dev_changes(&mut targets).is_empty());

    // A differently-sized rewrite is detected and the snapshot advances.
    fs::write(root.join("a.txt"), "modified-content").expect("rewrite");
    let affected = detect_dev_changes(&mut targets);
    assert!(affected.contains("api"));

    // The advanced snapshot means a second pass is clean.
    assert!(detect_dev_changes(&mut targets).is_empty());
}

#[test]
fn dev_watch_inference_uses_directory_mounts_and_explicit_roots() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let source_dir = tmpdir.path().join("src");
    let relative_source_dir = tmpdir.path().join("relative-src");
    let explicit_dir = tmpdir.path().join("extra");
    let cache_dir = tmpdir.path().join("cache");
    let file_mount = tmpdir.path().join("settings.toml");
    fs::create_dir_all(&source_dir).expect("source dir");
    fs::create_dir_all(&relative_source_dir).expect("relative source dir");
    fs::create_dir_all(&explicit_dir).expect("explicit dir");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    fs::write(&file_mount, "x").expect("file mount");
    let compose = tmpdir.path().join("compose-dev.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nx-slurm:\n  cache_dir: {}\nservices:\n  api:\n    image: {}\n    command: /bin/true\n    volumes:\n      - {}:/workspace\n      - ./relative-src:/relative\n      - {}:/config.toml:ro\n      - {}:/cache\n  worker:\n    image: {}\n    command: /bin/true\n    volumes:\n      - {}:/workspace\n",
                cache_dir.display(),
                local_image.display(),
                source_dir.display(),
                file_mount.display(),
                cache_dir.display(),
                local_image.display(),
                source_dir.display(),
            ),
        )
        .expect("compose");
    let plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let targets =
        infer_dev_watch_targets(&plan, tmpdir.path(), std::slice::from_ref(&explicit_dir))
            .expect("watch targets");
    let source_dir = canonical_dev_path(&source_dir);
    let relative_source_dir = canonical_dev_path(&relative_source_dir);
    let explicit_dir = canonical_dev_path(&explicit_dir);
    let cache_dir = canonical_dev_path(&cache_dir);
    let file_mount = canonical_dev_path(&file_mount);
    let source = targets
        .iter()
        .find(|target| target.root == source_dir)
        .expect("source target");
    assert!(source.services.contains("api"));
    assert!(source.services.contains("worker"));
    let relative_source = targets
        .iter()
        .find(|target| target.root == relative_source_dir)
        .unwrap_or_else(|| panic!("relative source target missing from {targets:#?}"));
    assert!(relative_source.services.contains("api"));
    assert!(!targets.iter().any(|target| target.root == cache_dir));
    assert!(!targets.iter().any(|target| target.root == file_mount));
    let explicit = targets
        .iter()
        .find(|target| target.root == explicit_dir)
        .expect("explicit target");
    assert_eq!(
        explicit.services,
        ["api".to_string(), "worker".to_string()]
            .into_iter()
            .collect()
    );

    let before = collect_dev_snapshot(&source_dir).expect("snapshot before");
    fs::write(source_dir.join("main.py"), "print('hi')\n").expect("source change");
    let after = collect_dev_snapshot(&source_dir).expect("snapshot after");
    assert_ne!(before, after);
}

#[test]
fn smoke_evaluation_rejects_missing_readiness_and_completion() {
    let snapshot = hpc_compose::job::StatusSnapshot {
        record: SubmissionRecord {
            schema_version: 2,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: "123".into(),
            submitted_at: 1,
            compose_file: PathBuf::from("compose.yaml"),
            submit_dir: PathBuf::from("/tmp"),
            script_path: PathBuf::from("job.sbatch"),
            cache_dir: PathBuf::from("/tmp/cache"),
            batch_log: PathBuf::from("slurm-123.out"),
            service_logs: BTreeMap::new(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
        },
        scheduler: hpc_compose::job::SchedulerStatus {
            state: "COMPLETED".into(),
            source: hpc_compose::job::SchedulerSource::Sacct,
            terminal: true,
            failed: false,
            detail: None,
        },
        queue_diagnostics: None,
        array: None,
        log_dir: PathBuf::from("/tmp/logs"),
        batch_log: hpc_compose::job::BatchLogStatus {
            path: PathBuf::from("slurm-123.out"),
            present: true,
            updated_at: None,
            updated_age_seconds: None,
        },
        services: vec![hpc_compose::job::PsServiceRow {
            service_name: "api".into(),
            path: PathBuf::from("api.log"),
            present: true,
            updated_at: None,
            updated_age_seconds: None,
            log_path: None,
            step_name: None,
            launch_index: Some(0),
            launcher_pid: None,
            healthy: Some(false),
            completed_successfully: Some(false),
            readiness_configured: Some(true),
            status: Some("exited(1)".into()),
            failure_policy_mode: Some("ignore".into()),
            restart_count: Some(0),
            max_restarts: None,
            window_seconds: None,
            max_restarts_in_window: None,
            restart_failures_in_window: None,
            last_exit_code: Some(1),
            started_at: Some(10),
            finished_at: Some(11),
            duration_seconds: Some(1),
            assertions: None,
            placement_mode: None,
            nodes: None,
            ntasks: None,
            ntasks_per_node: None,
            nodelist: None,
        }],
        attempt: None,
        is_resume: None,
        resume_dir: None,
    };
    let evaluation = evaluate_smoke_snapshot(&snapshot);
    assert!(!evaluation.ok);
    let reason = evaluation.failure_reason.expect("failure reason");
    assert!(reason.contains("api"));
    assert!(reason.contains("readiness"));
    assert!(reason.contains("complete successfully"));
}

#[test]
fn tmux_tail_command_quotes_log_paths() {
    let command = tmux_tail_command(Path::new("/tmp/demo run/api's log.txt"), 25);
    assert_eq!(command, "tail -n 25 -F '/tmp/demo run/api'\\''s log.txt'");
}

#[test]
fn runtime_command_wrappers_cover_success_and_error_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(tmpdir.path());
    let context = context_for(&compose, tmpdir.path());
    let local_compose = write_local_compose(tmpdir.path());

    launch(
        context_for(&local_compose, tmpdir.path()),
        Some(tmpdir.path().join("job.sbatch")),
        PrepareFlags {
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        },
        false,
        false,
        None,
        false,
        false,
        false,
        true,
        None,
        WatchMode::Auto,
        HoldOnExit::Failure,
        false,
    )
    .expect("submit dry run");
    launch(
        context_for(&local_compose, tmpdir.path()),
        Some(tmpdir.path().join("job.json.sbatch")),
        PrepareFlags {
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        },
        false,
        false,
        None,
        false,
        false,
        false,
        true,
        Some(OutputFormat::Json),
        WatchMode::Auto,
        HoldOnExit::Failure,
        false,
    )
    .expect("submit dry run json");

    let status_err = status(
        context.clone(),
        Some("12345".into()),
        Some(OutputFormat::Json),
        false,
        false,
    )
    .expect_err("status should require tracked metadata");
    assert!(status_err.to_string().contains("tracked job '12345'"));

    stats(
        context.clone(),
        Some("12345".into()),
        false,
        Some(StatsOutputFormat::Json),
        false,
    )
    .expect("stats should degrade when scheduler commands are unavailable");

    let artifacts_err = artifacts(
        context.clone(),
        None,
        Some(OutputFormat::Json),
        false,
        Vec::new(),
        false,
    )
    .expect_err("artifacts should require a tracked submission");
    assert!(
        artifacts_err
            .to_string()
            .contains("no tracked submission metadata exists")
    );

    let logs_err = logs(context.clone(), None, None, false, 10, None, None)
        .expect_err("logs should require a tracked submission");
    assert!(
        logs_err
            .to_string()
            .contains("no tracked submission metadata exists")
    );

    jobs_list(false, Some(OutputFormat::Json)).expect("jobs list");
    clean(
        context,
        Some(7),
        false,
        true,
        true,
        Some(OutputFormat::Json),
    )
    .expect("clean");

    let sbatch_path = tmpdir.path().join("fake-sbatch.sh");
    fs::write(
        &sbatch_path,
        "#!/bin/sh\nprintf 'submit boom\\n' >&2\nexit 1\n",
    )
    .expect("fake sbatch");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&sbatch_path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&sbatch_path, perms).expect("chmod");
    }
    let mut sbatch_context = context_for(&compose, tmpdir.path());
    sbatch_context.binaries.sbatch.value = sbatch_path.to_string_lossy().to_string();
    let submit_err = launch(
        sbatch_context,
        Some(tmpdir.path().join("submit-fail.sbatch")),
        PrepareFlags {
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
        },
        false,
        false,
        None,
        false,
        false,
        false,
        false,
        None,
        WatchMode::Auto,
        HoldOnExit::Failure,
        false,
    )
    .expect_err("sbatch failure");
    assert!(
        submit_err
            .to_string()
            .contains("sbatch failed: submit boom")
    );
}

#[test]
fn local_helper_functions_cover_labels_ids_and_stub_state_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose_with_services(tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("job.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-test-123",
        SubmissionBackend::Local,
    )
    .expect("record");

    assert!(generate_local_job_id().starts_with("local-"));
    assert_eq!(
        local_failure_policy_mode_label(ServiceFailureMode::FailJob),
        "fail_job"
    );
    assert_eq!(
        local_failure_policy_mode_label(ServiceFailureMode::Ignore),
        "ignore"
    );
    assert_eq!(
        local_failure_policy_mode_label(ServiceFailureMode::RestartOnFailure),
        "restart_on_failure"
    );
    assert_eq!(
        local_placement_mode_label(ServicePlacementMode::PrimaryNode),
        "primary_node"
    );
    assert_eq!(
        local_placement_mode_label(ServicePlacementMode::Distributed),
        "distributed"
    );
    assert_eq!(
        local_placement_mode_label(ServicePlacementMode::Partitioned),
        "partitioned"
    );
    assert_eq!(local_service_step_name("api"), "hpc-compose:api");
    assert_eq!(
        local_service_step_name("api.worker-1"),
        "hpc-compose:api_x2e_worker_x2d_1"
    );

    write_local_runtime_state_stub(&record, &runtime_plan, 777).expect("state stub");
    let state_path = state_path_for_record(&record);
    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).expect("read state"))
            .expect("parse state");
    assert_eq!(state["backend"], serde_json::Value::from("local"));
    assert_eq!(state["supervisor_pid"], serde_json::Value::from(777));
    assert_eq!(state["services"].as_array().map(Vec::len), Some(2));
    assert_eq!(state["services"][0]["service_name"], "api");
    assert_eq!(state["services"][0]["readiness_configured"], true);
    assert_eq!(state["services"][1]["service_name"], "worker");

    assert_eq!(
        read_local_supervisor_pid(&record).expect("supervisor pid"),
        Some(777)
    );

    fs::write(&state_path, "{\"supervisor_pid\":9}").expect("overwrite state");
    write_local_runtime_state_stub(&record, &runtime_plan, 888).expect("existing state");
    let preserved: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).expect("read preserved"))
            .expect("parse preserved");
    assert_eq!(preserved["supervisor_pid"], serde_json::Value::from(9));
}

#[test]
fn process_helpers_cover_spawn_kill_and_pid_reader_edges() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose(tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("job.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-test-456",
        SubmissionBackend::Local,
    )
    .expect("record");

    assert_eq!(
        read_local_supervisor_pid(&record).expect("missing state pid"),
        None
    );

    let state_path = state_path_for_record(&record);
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent).expect("state dir");
    }
    fs::write(&state_path, "{not-json").expect("bad state");
    let parse_err = read_local_supervisor_pid(&record).expect_err("malformed state");
    assert!(parse_err.to_string().contains("failed to parse"));

    fs::write(&script_path, "#!/bin/bash\ntrap 'exit 0' TERM\nsleep 30\n").expect("script");
    let batch_log = tmpdir.path().join("batch.log");
    let pid = spawn_local_supervisor(tmpdir.path(), &script_path, &batch_log).expect("spawn local");
    assert!(batch_log.exists());

    kill_pid(pid).expect("kill child");
    thread::sleep(Duration::from_millis(200));

    let kill_err = kill_pid(u32::MAX).expect_err("unknown pid");
    assert!(kill_err.to_string().contains("failed to signal pid"));
}

#[test]
fn tracking_resolution_and_cache_purge_helpers_cover_edge_cases() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose(tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let context = context_for(&compose, tmpdir.path());
    let script_path = tmpdir.path().join("job.local.sh");
    let mut older = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-old",
        SubmissionBackend::Local,
    )
    .expect("older record");
    older.submitted_at = 100;
    let mut newer = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-new",
        SubmissionBackend::Local,
    )
    .expect("newer record");
    newer.submitted_at = 200;
    write_submission_record(&newer).expect("write newer");
    write_submission_record(&older).expect("write older");

    assert_eq!(
        resolve_tracked_record(&context, None)
            .expect("resolve latest")
            .expect("latest")
            .job_id,
        "local-new"
    );
    assert_eq!(
        resolve_tracked_record(&context, Some("local-old"))
            .expect("resolve explicit")
            .expect("explicit")
            .job_id,
        "local-old"
    );
    assert!(
        resolve_tracked_record(&context, Some("missing"))
            .expect("resolve missing")
            .is_none()
    );

    let file_artifact = tmpdir.path().join("cache/file.sqsh");
    let dir_artifact = tmpdir.path().join("cache/dir-artifact");
    fs::create_dir_all(&dir_artifact).expect("dir artifact");
    fs::create_dir_all(file_artifact.parent().expect("file parent")).expect("file parent");
    fs::write(&file_artifact, "artifact").expect("file artifact");
    fs::write(dir_artifact.join("payload"), "artifact").expect("dir payload");
    let missing = tmpdir.path().join("cache/missing.sqsh");
    let removed =
        purge_cached_artifacts(&[file_artifact.clone(), dir_artifact.clone(), missing.clone()])
            .expect("purge");
    assert_eq!(removed, vec![file_artifact.clone(), dir_artifact.clone()]);
    assert!(!file_artifact.exists());
    assert!(!dir_artifact.exists());

    assert!(
        cached_artifacts_for_teardown(None)
            .expect_err("missing record")
            .to_string()
            .contains("--purge-cache requires tracked submission metadata")
    );
    assert!(
        cached_artifacts_for_teardown(Some(&older))
            .expect_err("empty cached artifacts")
            .to_string()
            .contains("does not contain cached artifact snapshots")
    );
    newer.cached_artifacts = vec![missing.clone()];
    assert_eq!(
        cached_artifacts_for_teardown(Some(&newer)).expect("cached artifacts"),
        vec![missing]
    );
}

#[test]
fn local_submit_support_and_warning_helpers_cover_non_linux_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose(tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");

    warn_local_ignored_scheduler_settings(&runtime_plan);

    let distributed = tmpdir.path().join("distributed.yaml");
    let local_image = tmpdir.path().join("distributed.sqsh");
    fs::write(&local_image, "sqsh").expect("distributed image");
    fs::write(
            &distributed,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      nodes: 2\nx-slurm:\n  cache_dir: {}\n  nodes: 2\n",
                local_image.display(),
                tmpdir.path().join("cache-distributed").display()
            ),
        )
        .expect("distributed compose");
    let distributed_plan = output::load_runtime_plan(&distributed).expect("distributed plan");
    let distributed_err =
        ensure_local_plan_supported(&distributed_plan).expect_err("distributed unsupported");
    assert!(
        distributed_err
            .to_string()
            .contains("does not support distributed or partitioned placement")
    );

    let extra_args = tmpdir.path().join("extra-args.yaml");
    fs::write(
            &extra_args,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      extra_srun_args:\n        - --exclusive\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache-extra").display()
            ),
        )
        .expect("extra args compose");
    let extra_args_plan = output::load_runtime_plan(&extra_args).expect("extra args plan");
    let extra_args_err =
        ensure_local_plan_supported(&extra_args_plan).expect_err("extra args unsupported");
    assert!(extra_args_err.to_string().contains("extra_srun_args"));

    let mpi = tmpdir.path().join("mpi.yaml");
    fs::write(
            &mpi,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      mpi:\n        type: pmix\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache-mpi").display()
            ),
        )
        .expect("mpi compose");
    let mpi_plan = output::load_runtime_plan(&mpi).expect("mpi plan");
    let mpi_err = ensure_local_plan_supported(&mpi_plan).expect_err("mpi unsupported");
    assert!(mpi_err.to_string().contains("x-slurm.mpi"));

    let multi_node = tmpdir.path().join("multi-node.yaml");
    fs::write(
            &multi_node,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\n    x-slurm:\n      nodes: 1\nx-slurm:\n  cache_dir: {}\n  nodes: 2\n",
                local_image.display(),
                tmpdir.path().join("cache-nodes").display()
            ),
        )
        .expect("multi-node compose");
    let multi_node_plan = output::load_runtime_plan(&multi_node).expect("multi-node plan");
    let multi_node_err =
        ensure_local_plan_supported(&multi_node_plan).expect_err("multi-node unsupported");
    assert!(
        multi_node_err
            .to_string()
            .contains("only single-host specs")
    );

    if env::consts::OS != "linux" {
        let err = ensure_local_host_supported().expect_err("non-linux");
        assert!(err.to_string().contains("only supported on Linux hosts"));
    }
}

#[test]
fn local_watch_cancel_and_rollback_helpers_cover_terminal_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose(tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("watch.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-watch-123",
        SubmissionBackend::Local,
    )
    .expect("record");

    write_submission_record(&record).expect("persist record");
    let state_path = state_path_for_record(&record);
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent).expect("state dir");
    }
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": SubmissionBackend::Local,
            "job_status": "COMPLETED",
            "job_exit_code": 0,
            "supervisor_pid": serde_json::Value::Null,
            "services": [],
        }))
        .expect("state json"),
    )
    .expect("write state");

    print_local_launch_details(&record, &runtime_plan, &script_path);

    let watch = watch_with_fallback(
        &record,
        &SchedulerOptions {
            squeue_bin: "/definitely/missing-squeue".into(),
            sacct_bin: "/definitely/missing-sacct".into(),
        },
        None,
        5,
        WatchMode::Auto,
        HoldOnExit::Failure,
        watch_ui::WatchPrefs::default(),
    )
    .expect("watch");
    assert!(matches!(
        watch,
        hpc_compose::job::WatchOutcome::Completed(_)
    ));

    cancel(
        context_for(&compose, tmpdir.path()),
        Some(record.job_id.clone()),
        false,
        Some(OutputFormat::Json),
    )
    .expect("cancel local without pid");

    write_submission_record(&record).expect("rewrite record");
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent).expect("recreate state dir");
    }
    let mut sleeper = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn sleep");
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": SubmissionBackend::Local,
            "job_status": "RUNNING",
            "job_exit_code": serde_json::Value::Null,
            "supervisor_pid": sleeper.id(),
            "services": [],
        }))
        .expect("running state json"),
    )
    .expect("write running state");
    cancel(
        context_for(&compose, tmpdir.path()),
        Some(record.job_id.clone()),
        false,
        None,
    )
    .expect("cancel running local");
    sleeper.wait().expect("wait for cancelled sleep");

    let reservation_compose = tmpdir.path().join("compose-reservation.yaml");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(
            &reservation_compose,
            format!(
                "name: demo\nservices:\n  app:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n  error: local.err\n  submit_args:\n    - --reservation=debug\n",
                local_image.display(),
                tmpdir.path().join("cache-reservation").display()
            ),
        )
        .expect("reservation compose");
    let reservation_plan =
        output::load_runtime_plan(&reservation_compose).expect("reservation runtime plan");
    warn_local_ignored_scheduler_settings(&reservation_plan);

    let mut sleeper = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn sleep");
    rollback_local_tracking(&record, Some(sleeper.id()));
    sleeper.wait().expect("wait for sleep");
    assert!(
        load_submission_record(&compose, Some(&record.job_id)).is_err(),
        "rollback should remove tracked record"
    );
}

#[test]
fn runtime_wrappers_cover_success_paths_with_local_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_local_compose_with_services(tmpdir.path());
    let context = context_for(&compose, tmpdir.path());
    let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("local-wrapper.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-success-123",
        SubmissionBackend::Local,
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    for (service_name, log_path) in &record.service_logs {
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).expect("log dir");
        }
        fs::write(log_path, format!("{service_name} ready\n")).expect("service log");
    }

    let state_path = state_path_for_record(&record);
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent).expect("state dir");
    }
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": SubmissionBackend::Local,
            "job_status": "COMPLETED",
            "job_exit_code": 0,
            "supervisor_pid": serde_json::Value::Null,
            "services": [
                {
                    "service_name": "api",
                    "step_name": "hpc-compose:api",
                    "log_path": record.service_logs["api"],
                    "launch_index": 0,
                    "launcher_pid": serde_json::Value::Null,
                    "healthy": true,
                    "readiness_configured": true,
                    "failure_policy_mode": "fail_job",
                    "restart_count": 0,
                    "last_exit_code": 0
                },
                {
                    "service_name": "worker",
                    "step_name": "hpc-compose:worker",
                    "log_path": record.service_logs["worker"],
                    "launch_index": 1,
                    "launcher_pid": serde_json::Value::Null,
                    "healthy": false,
                    "readiness_configured": false,
                    "failure_policy_mode": "ignore",
                    "restart_count": 0,
                    "last_exit_code": 0
                }
            ]
        }))
        .expect("state json"),
    )
    .expect("write state");

    status(
        context.clone(),
        Some(record.job_id.clone()),
        Some(OutputFormat::Json),
        false,
        false,
    )
    .expect("status");
    stats(
        context.clone(),
        Some(record.job_id.clone()),
        false,
        Some(StatsOutputFormat::Json),
        false,
    )
    .expect("stats");
    ps(
        context.clone(),
        Some(record.job_id.clone()),
        Some(OutputFormat::Json),
    )
    .expect("ps");
    logs(
        context.clone(),
        Some(record.job_id.clone()),
        Some("api".into()),
        false,
        10,
        None,
        None,
    )
    .expect("logs");
    watch(
        context.clone(),
        Some(record.job_id.clone()),
        Some("api".into()),
        10,
        WatchMode::Line,
        HoldOnExit::Failure,
    )
    .expect("watch");
    cancel(
        context.clone(),
        Some(record.job_id.clone()),
        false,
        Some(OutputFormat::Json),
    )
    .expect("cancel");
    jobs_list(true, Some(OutputFormat::Json)).expect("jobs list");
    clean(
        context,
        Some(0),
        false,
        true,
        true,
        Some(OutputFormat::Json),
    )
    .expect("clean");
}
