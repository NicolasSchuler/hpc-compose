use super::*;
use hpc_compose::job::{
    CollectorCoverage, CollectorCoverageScope, CollectorStatus, CpuNodeSample, CpuSnapshot,
    CpuSummary, GpuNodeSummary, PsSnapshot, QueueDiagnostics, ReplayArtifactPaths, ReplayEvent,
    ReplayEventKind, ReplayFrame, ReplayReport, ReplayServiceFrame, RequestedWalltime,
    SamplerSnapshot, SchedulerOptions, SchedulerSource, SchedulerStatus, StatsSnapshot,
    SubmissionBackend, SubmissionKind, SubmissionRecord, WalltimeProgress, WatchOutcome,
    WatchdogClassification, WatchdogObservation, WatchdogResource, WatchdogSnapshot,
    WatchdogStatus, build_submission_record_with_backend, state_path_for_record,
    write_submission_record,
};
use hpc_compose::spec::WatchdogAction;

fn stats_snapshot_with_cpu(cpu: Option<CpuSnapshot>, available: bool) -> StatsSnapshot {
    StatsSnapshot {
        job_id: "12345".into(),
        record: None,
        metrics_dir: None,
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: None,
        },
        available,
        reason: None,
        source: "sampler".into(),
        notes: vec![],
        sampler: Some(SamplerSnapshot {
            interval_seconds: 5,
            collectors: vec![],
            gpu: None,
            slurm: None,
            cpu,
        }),
        steps: vec![],
        accounting: None,
        first_failure: None,
        watchdog: None,
        attempt: None,
        is_resume: None,
        resume_dir: None,
    }
}

#[test]
fn format_watch_metrics_line_appends_cpu_segment_when_present() {
    let cpu = CpuSnapshot {
        sampled_at: "2026-04-05T10:00:10Z".into(),
        nodes: vec![
            CpuNodeSample {
                node: Some("nodeA".into()),
                cpu_util_pct: Some(40.0),
                core_count: Some(64),
                loadavg_1m: Some(12.5),
            },
            CpuNodeSample {
                node: Some("nodeB".into()),
                cpu_util_pct: Some(45.4),
                core_count: Some(32),
                loadavg_1m: Some(8.0),
            },
        ],
        summary: CpuSummary {
            node_count: 2,
            mean_util_pct: Some(42.7),
            max_util_pct: Some(45.4),
            total_core_count: Some(96),
        },
    };
    // available=false isolates the cpu segment (no trailing "stats:" part).
    let snapshot = stats_snapshot_with_cpu(Some(cpu), false);
    assert_eq!(
        format_watch_metrics_line(&snapshot).as_deref(),
        Some("cpu: 43%"),
        "mean util is rounded and rendered as a compact segment"
    );
}

#[test]
fn format_watch_metrics_line_omits_cpu_segment_when_absent_or_util_less() {
    // No cpu data at all: the line carries no cpu segment (here: nothing).
    let no_cpu = stats_snapshot_with_cpu(None, false);
    assert_eq!(format_watch_metrics_line(&no_cpu), None);

    // CPU present but every node is a util-less first sample: still no segment.
    let util_less = CpuSnapshot {
        sampled_at: "2026-04-05T10:00:10Z".into(),
        nodes: vec![CpuNodeSample {
            node: Some("nodeA".into()),
            cpu_util_pct: None,
            core_count: Some(64),
            loadavg_1m: Some(12.5),
        }],
        summary: CpuSummary {
            node_count: 1,
            mean_util_pct: None,
            max_util_pct: None,
            total_core_count: Some(64),
        },
    };
    assert_eq!(
        format_watch_metrics_line(&stats_snapshot_with_cpu(Some(util_less), false)),
        None
    );
}

#[test]
fn format_watch_metrics_line_includes_watchdog_warning() {
    let mut snapshot = stats_snapshot_with_cpu(None, false);
    snapshot.watchdog = Some(WatchdogSnapshot {
        enabled: true,
        action: WatchdogAction::Warn,
        status: WatchdogStatus::Warning,
        message: "low GPU compute with resident VRAM".into(),
        grace_period_seconds: 1,
        observations: vec![WatchdogObservation {
            resource: WatchdogResource::Gpu,
            status: WatchdogStatus::Warning,
            classification: WatchdogClassification::ResidentIdle,
            window_seconds: 120,
            observed_seconds: 120,
            sample_count: 2,
            mean_compute_pct: Some(0.0),
            max_compute_pct: Some(0.0),
            memory_resident_pct: Some(75.0),
            memory_signal: Some("gpu_memory_used_total".into()),
            message: "low GPU compute with resident VRAM".into(),
        }],
        telemetry_coverage: Vec::new(),
        confidence_notes: Vec::new(),
    });

    assert_eq!(
        format_watch_metrics_line(&snapshot).as_deref(),
        Some("watchdog: low GPU compute with resident VRAM")
    );
}

#[test]
fn format_watch_metrics_line_leads_with_plain_coverage_warning() {
    let mut snapshot = stats_snapshot_with_cpu(None, true);
    snapshot.sampler.as_mut().expect("sampler").collectors = vec![CollectorStatus {
        name: "gpu".into(),
        enabled: true,
        available: true,
        note: Some("fanout failed".into()),
        last_sampled_at: Some("2026-04-05T10:00:10Z".into()),
        coverage: Some(CollectorCoverage {
            scope: CollectorCoverageScope::BatchNode,
            expected_nodes: 4,
            observed_nodes: 1,
            degraded: true,
            reason: Some("fanout failed".into()),
        }),
    }];

    let line = format_watch_metrics_line(&snapshot).expect("metrics line");
    assert!(line.starts_with("TELEMETRY DEGRADED: GPU covers batch node only (1/4)"));
    assert!(line.ends_with("stats: sampler"));
}

#[test]
fn format_gpu_metrics_includes_power_when_reported() {
    let node = GpuNodeSummary {
        node: Some("node01".into()),
        gpu_count: 4,
        avg_utilization_gpu: Some(72.0),
        memory_used_mib: Some(2100),
        memory_total_mib: Some(40960),
        power_draw_w: Some(185.4),
        power_limit_w: Some(1200.0),
    };
    assert_eq!(
        format_gpu_metrics(&node),
        "gpu: 4 util=72% mem=2100/40960 MiB power=185W"
    );

    // Power is omitted entirely when the sampler did not report it.
    let no_power = GpuNodeSummary {
        power_draw_w: None,
        ..node
    };
    assert_eq!(
        format_gpu_metrics(&no_power),
        "gpu: 4 util=72% mem=2100/40960 MiB"
    );
}

#[test]
fn aggregate_gpu_nodes_sums_fleet_and_marks_node_count() {
    let node_a = GpuNodeSummary {
        node: Some("node01".into()),
        gpu_count: 2,
        avg_utilization_gpu: Some(80.0),
        memory_used_mib: Some(1000),
        memory_total_mib: Some(40000),
        power_draw_w: Some(100.0),
        power_limit_w: Some(500.0),
    };
    let node_b = GpuNodeSummary {
        node: Some("node02".into()),
        gpu_count: 4,
        avg_utilization_gpu: Some(50.0),
        memory_used_mib: Some(3000),
        memory_total_mib: Some(80000),
        power_draw_w: Some(300.0),
        power_limit_w: Some(1000.0),
    };
    let nodes = [node_a, node_b];

    let aggregate = aggregate_gpu_nodes(&nodes);
    assert_eq!(aggregate.gpu_count, 6);
    // Unweighted mean across all six devices: (80*2 + 50*4) / 6 = 60.
    assert_eq!(aggregate.avg_utilization_gpu, Some(60.0));
    assert_eq!(aggregate.memory_used_mib, Some(4000));
    assert_eq!(aggregate.memory_total_mib, Some(120000));
    assert_eq!(aggregate.power_draw_w, Some(400.0));

    // The rendered multi-node line sums the fleet and marks the node count.
    assert_eq!(
        format!("{} x{} nodes", format_gpu_metrics(&aggregate), nodes.len()),
        "gpu: 6 util=60% mem=4000/120000 MiB power=400W x2 nodes"
    );
}

fn sample_snapshot() -> PsSnapshot {
    PsSnapshot {
        record: SubmissionRecord {
            schema_version: 1,
            backend: hpc_compose::job::SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: "12345".into(),
            submitted_at: 0,
            compose_file: PathBuf::from("/tmp/compose.yaml"),
            submit_dir: PathBuf::from("/tmp"),
            script_path: PathBuf::from("/tmp/job.sbatch"),
            cache_dir: PathBuf::from("/tmp/cache"),
            runtime_root: None,
            batch_log: PathBuf::from("/tmp/slurm-12345.out"),
            batch_log_managed: false,
            service_logs: Default::default(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: Some(RequestedWalltime {
                original: "00:10:00".into(),
                seconds: 600,
            }),
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
            provenance: None,
            tags: Vec::new(),
            notes: Vec::new(),
        },
        scheduler: SchedulerStatus {
            state: "RUNNING".into(),
            source: SchedulerSource::Squeue,
            terminal: false,
            failed: false,
            detail: None,
        },
        queue_diagnostics: Some(QueueDiagnostics {
            pending_reason: None,
            eligible_time: None,
            start_time: None,
        }),
        log_dir: PathBuf::from("/tmp/.hpc-compose/12345/logs"),
        services: vec![
            PsServiceRow {
                service_name: "api".into(),
                path: PathBuf::from("/tmp/api.log"),
                present: true,
                updated_at: None,
                updated_age_seconds: None,
                log_path: Some(PathBuf::from("/tmp/api.log")),
                step_name: Some("hpc-compose:api".into()),
                launch_index: Some(0),
                launcher_pid: Some(4242),
                healthy: Some(true),
                completed_successfully: Some(false),
                readiness_configured: Some(true),
                status: Some("ready".into()),
                failure_policy_mode: Some("restart_on_failure".into()),
                restart_count: Some(1),
                max_restarts: Some(3),
                window_seconds: Some(60),
                max_restarts_in_window: Some(3),
                restart_failures_in_window: Some(1),
                last_exit_code: None,
                started_at: None,
                finished_at: None,
                duration_seconds: None,
                assertions: None,
                placement_mode: Some("primary".into()),
                nodes: Some(1),
                ntasks: Some(1),
                ntasks_per_node: Some(1),
                nodelist: Some("node001".into()),
            },
            PsServiceRow {
                service_name: "worker".into(),
                path: PathBuf::from("/tmp/worker.log"),
                present: true,
                updated_at: None,
                updated_age_seconds: None,
                log_path: Some(PathBuf::from("/tmp/worker.log")),
                step_name: Some("hpc-compose:worker".into()),
                launch_index: Some(1),
                launcher_pid: Some(5252),
                healthy: Some(false),
                completed_successfully: Some(false),
                readiness_configured: Some(false),
                status: Some("running".into()),
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
            },
        ],
        attempt: None,
        is_resume: None,
        resume_dir: None,
    }
}

fn sample_watch_model() -> WatchModel {
    WatchModel {
        snapshot: sample_snapshot(),
        selected_index: 0,
        walltime_progress: None,
        log_lines: Vec::new(),
        follow_logs: true,
        log_scroll: 0,
        log_view_mode: LogViewMode::Selected,
        hold_state: None,
        metrics_line: None,
        show_help: false,
        filter: None,
        search_buffer: String::new(),
        input_mode: InputMode::Normal,
        log_query: None,
        log_wrap: false,
        sort_mode: ServiceSort::Spec,
        notice: None,
        show_detail: false,
        replay: None,
    }
}

fn sample_replay_report() -> ReplayReport {
    let snapshot = sample_snapshot();
    let events = vec![
        ReplayEvent {
            at_unix: 100,
            attempt: None,
            kind: ReplayEventKind::ServiceStart,
            service: Some("api".into()),
            exit_code: None,
            detail: Some("started".into()),
        },
        ReplayEvent {
            at_unix: 110,
            attempt: None,
            kind: ReplayEventKind::ServiceExit,
            service: Some("api".into()),
            exit_code: Some(7),
            detail: Some("node=n1".into()),
        },
    ];
    let frames = events
        .iter()
        .enumerate()
        .map(|(index, event)| ReplayFrame {
            cursor_unix: event.at_unix,
            event_index: index,
            event: event.clone(),
            services: vec![ReplayServiceFrame {
                service_name: "api".into(),
                status: if index == 0 {
                    "running".into()
                } else {
                    "failed".into()
                },
                started_at: Some(100),
                finished_at: (index == 1).then_some(110),
                last_exit_code: (index == 1).then_some(7),
                restart_count: Some(0),
            }],
            metrics_line: (index == 1).then_some("gpu: 1 util=90% mem=4/8 MiB".into()),
            fidelity_note: Some("best-effort replay from existing tracked artifacts".into()),
            snapshot: {
                let mut snapshot = snapshot.clone();
                snapshot.scheduler.state = if index == 0 {
                    "RUNNING".into()
                } else {
                    "FAILED".into()
                };
                snapshot.scheduler.failed = index == 1;
                snapshot.scheduler.terminal = index == 1;
                snapshot.scheduler.detail =
                    Some("best-effort replay from existing tracked artifacts".into());
                snapshot.services[0].status = Some(if index == 0 {
                    "running".into()
                } else {
                    "failed".into()
                });
                snapshot.services[0].last_exit_code = (index == 1).then_some(7);
                snapshot
            },
        })
        .collect::<Vec<_>>();
    ReplayReport {
        job_id: "12345".into(),
        record: snapshot.record.clone(),
        fidelity: "best-effort".into(),
        notes: vec!["best-effort".into()],
        artifacts: ReplayArtifactPaths::default(),
        events,
        frames,
        timeline_start_unix: Some(100),
        timeline_end_unix: Some(110),
    }
}

/// Deterministic event source that replays a scripted sequence of inputs,
/// then quits so the watch and replay loops always terminate.
struct ScriptedEvents {
    events: std::collections::VecDeque<WatchInput>,
}

impl ScriptedEvents {
    fn new(events: impl IntoIterator<Item = WatchInput>) -> Self {
        Self {
            events: events.into_iter().collect(),
        }
    }
}

impl WatchEventSource for ScriptedEvents {
    fn poll_event(&mut self, _timeout: Duration, _mode: InputMode) -> Result<Option<WatchInput>> {
        Ok(Some(
            self.events
                .pop_front()
                .unwrap_or(WatchInput::Normal(WatchKey::Quit)),
        ))
    }
}

fn normal(key: WatchKey) -> WatchInput {
    WatchInput::Normal(key)
}

#[test]
fn frame_renderer_tracks_rows_and_handles_resize() {
    let mut renderer = FrameRenderer::new();
    renderer.render("a\nb\nc", (3, 3)).expect("initial paint");
    assert_eq!(renderer.previous_lines, vec!["a", "b", "c"]);

    // Identical frame: nothing to rewrite, cached rows preserved.
    renderer.render("a\nb\nc", (3, 3)).expect("identical paint");
    assert_eq!(renderer.previous_lines.len(), 3);

    // Shorter frame at the same size exercises the trailing-clear branch.
    renderer.render("a\nb", (3, 3)).expect("shorter paint");
    assert_eq!(renderer.previous_lines, vec!["a", "b"]);

    // Taller frame grows the cached rows.
    renderer.render("a\nb\nc\nd", (3, 3)).expect("taller paint");
    assert_eq!(renderer.previous_lines.len(), 4);

    // A size change forces a full repaint and updates the tracked size.
    renderer.render("x\ny", (2, 2)).expect("resized paint");
    assert_eq!(renderer.last_size, Some((2, 2)));
    assert_eq!(renderer.previous_lines, vec!["x", "y"]);
}

fn search(key: SearchKey) -> WatchInput {
    WatchInput::Search(key)
}

#[test]
fn walltime_changed_gates_idle_redraws() {
    let progress = |elapsed| {
        Some(WalltimeProgress {
            original: "00:10:00".into(),
            elapsed_seconds: elapsed,
            total_seconds: 600,
            remaining_seconds: 600 - elapsed,
        })
    };

    // No walltime in either state: nothing to repaint for.
    assert!(!walltime_changed(&None, &None));
    // Walltime appears (job started running): repaint.
    assert!(walltime_changed(&None, &progress(300)));
    // Walltime disappears (job left RUNNING): repaint.
    assert!(walltime_changed(&progress(300), &None));
    // Same second, same progress: idle wake-up, skip the rebuild.
    assert!(!walltime_changed(&progress(300), &progress(300)));
    // Progress advanced by a second: repaint.
    assert!(walltime_changed(&progress(300), &progress(301)));
}

#[test]
fn replay_loop_navigation_selects_service_via_injected_events() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::Down), normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.selected_service.as_deref(), Some("worker"));
}

#[test]
fn replay_loop_initial_triage_sort_preserves_service_selection() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(
        &report,
        Some("api"),
        5,
        1.0,
        &mut events,
        WatchPrefs {
            sort: ServiceSort::Triage,
            ..WatchPrefs::default()
        },
    )
    .expect("replay loop runs");
    assert_eq!(result.sort_mode, ServiceSort::Triage);
    assert_eq!(result.selected_service.as_deref(), Some("api"));
}

#[test]
fn replay_loop_filter_narrows_to_matching_service() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([
        normal(WatchKey::Search),
        search(SearchKey::Char('w')),
        search(SearchKey::Char('o')),
        search(SearchKey::Submit),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.filter.as_deref(), Some("wo"));
    assert_eq!(result.selected_service.as_deref(), Some("worker"));
}

#[test]
fn replay_loop_search_cancel_restores_unfiltered_view() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([
        normal(WatchKey::Search),
        search(SearchKey::Char('z')),
        search(SearchKey::Cancel),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert!(result.filter.is_none());
    assert_eq!(result.selected_service.as_deref(), Some("api"));
}

#[test]
fn replay_loop_speed_and_pause_keys_update_playback() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([
        normal(WatchKey::SpeedUp),
        normal(WatchKey::TogglePause),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.playback.speed, 10.0);
    assert!(result.playback.paused);
}

#[test]
fn replay_loop_event_step_advances_cursor_and_pauses() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::NextEvent), normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.playback.frame_index, 1);
    assert_eq!(result.playback.cursor_unix, 110);
    assert!(result.playback.paused);
}

#[test]
fn replay_loop_toggle_all_logs_changes_view_mode() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::ToggleAllLogs), normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.log_view_mode, LogViewMode::All);
}

#[test]
fn replay_loop_empty_report_returns_without_reading_events() {
    let mut report = sample_replay_report();
    report.frames.clear();
    let mut events = ScriptedEvents::new([normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert!(result.selected_service.is_none());
}

#[test]
fn replay_loop_log_search_sets_query_without_touching_filter() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([
        normal(WatchKey::LogSearch),
        search(SearchKey::Char('e')),
        search(SearchKey::Char('r')),
        search(SearchKey::Char('r')),
        search(SearchKey::Submit),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.log_query.as_deref(), Some("err"));
    assert!(result.filter.is_none());
}

#[test]
fn replay_loop_toggle_wrap_flips_state() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::ToggleWrap), normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert!(result.log_wrap);
}

#[test]
fn replay_loop_cycle_sort_switches_and_preserves_selection() {
    let report = sample_replay_report();
    // Select `worker` (unhealthy), then switch to triage order. `worker`
    // moves to the front but stays selected.
    let mut events = ScriptedEvents::new([
        normal(WatchKey::Down),
        normal(WatchKey::CycleSort),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert_eq!(result.sort_mode, ServiceSort::Triage);
    assert_eq!(result.selected_service.as_deref(), Some("worker"));
}

#[test]
fn log_severity_classifies_levels_by_word() {
    assert_eq!(log_severity("[ERROR] boom"), Some(LogSeverity::Error));
    assert_eq!(log_severity("level=warn retrying"), Some(LogSeverity::Warn));
    assert_eq!(
        log_severity("thread 'main' panicked"),
        Some(LogSeverity::Error)
    );
    assert_eq!(log_severity("all systems nominal"), None);
    // Inflected forms are detected via the prefix boundary.
    assert_eq!(
        log_severity("the build errored earlier"),
        Some(LogSeverity::Error)
    );
    // ...but embedded matches like `terror` (contains `error`) are rejected.
    assert_eq!(log_severity("terror is not a level"), None);
}

#[test]
fn highlight_and_count_track_query_matches() {
    let lines = [
        "api ok".to_string(),
        "API ready".to_string(),
        "db idle".to_string(),
    ];
    assert_eq!(count_log_matches(&lines, "api"), 2);
    assert_eq!(count_log_matches(&lines, ""), 0);
    assert!(highlight_matches("nothing here", "zzz").is_none());
    // Highlighting preserves the visible text (only adds styling).
    let highlighted = highlight_matches("hello WORLD", "world").expect("match present");
    assert_eq!(strip_ansi_for_snapshot(&highlighted), "hello WORLD");
}

#[test]
fn env_refresh_interval_opt_parses_and_clamps() {
    assert_eq!(env_refresh_interval_opt(None, 100, 60_000), None);
    assert_eq!(env_refresh_interval_opt(Some("nope"), 100, 60_000), None);
    // Below the floor and above the ceiling are clamped.
    assert_eq!(
        env_refresh_interval_opt(Some("10"), 100, 60_000),
        Some(Duration::from_millis(100))
    );
    assert_eq!(
        env_refresh_interval_opt(Some("999999"), 100, 60_000),
        Some(Duration::from_millis(60_000))
    );
    assert_eq!(
        env_refresh_interval_opt(Some(" 2500 "), 100, 60_000),
        Some(Duration::from_millis(2500))
    );
}

#[test]
fn watch_prefs_resolve_reads_settings() {
    use hpc_compose::context::WatchSettings;
    let prefs = WatchPrefs::resolve(&WatchSettings {
        sort: Some("triage".into()),
        wrap: Some(true),
        refresh_ms: Some(250),
        metrics_refresh_ms: Some(2000),
        mouse: Some(true),
    });
    assert_eq!(prefs.sort, ServiceSort::Triage);
    assert!(prefs.wrap);
    assert_eq!(prefs.data_refresh, Duration::from_millis(250));
    assert_eq!(prefs.metrics_refresh, Duration::from_millis(2000));
    assert!(prefs.mouse);
    // Defaults when unset.
    let defaults = WatchPrefs::resolve(&WatchSettings::default());
    assert_eq!(defaults.sort, ServiceSort::Spec);
    assert!(!defaults.wrap);
    assert_eq!(defaults.data_refresh, DATA_REFRESH_INTERVAL);
}

#[test]
fn expand_log_lines_wraps_only_when_enabled() {
    let lines = vec!["abcdef".to_string(), "gh".to_string()];
    assert_eq!(expand_log_lines(&lines, 3, false), lines);
    assert_eq!(
        expand_log_lines(&lines, 3, true),
        vec!["abc".to_string(), "def".to_string(), "gh".to_string()]
    );
}

#[test]
fn replay_scrubber_renders_label_cursor_and_handles_zero_span() {
    let replay = ReplayWatchStatus {
        cursor_unix: 105,
        speed: 1.0,
        paused: false,
        fidelity: "best-effort".into(),
        start_unix: 100,
        end_unix: 110,
        event_unix: vec![100, 110],
    };
    let bar = render_replay_scrubber(&replay, 80);
    assert!(bar.contains("timeline 5s/10s"));
    let visible = strip_ansi_for_snapshot(&bar);
    // Cursor head is drawn (ASCII `#` or unicode ●).
    assert!(visible.contains('#') || visible.contains('\u{25cf}'));
    // A zero-span timeline must not panic.
    let zero = ReplayWatchStatus {
        cursor_unix: 100,
        start_unix: 100,
        end_unix: 100,
        event_unix: vec![100],
        ..replay
    };
    let _ = render_replay_scrubber(&zero, 80);
}

#[test]
fn render_service_detail_surfaces_table_omitted_fields() {
    let snapshot = sample_snapshot();
    let detail = render_service_detail(&snapshot.services[0], 80, 40).join("\n");
    let visible = strip_ansi_for_snapshot(&detail);
    assert!(visible.contains("[ api ]"));
    assert!(visible.contains("pid         4242"));
    assert!(visible.contains("placement   primary"));
    assert!(visible.contains("nodelist    node001"));
    assert!(visible.contains("restarts    1/3"));
    assert!(visible.contains("Esc/Enter back"));
}

#[test]
fn replay_loop_enter_toggles_detail_panel() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([normal(WatchKey::ShowDetail), normal(WatchKey::Quit)]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert!(result.show_detail);
}

#[test]
fn replay_loop_escape_closes_detail_panel() {
    let report = sample_replay_report();
    let mut events = ScriptedEvents::new([
        normal(WatchKey::ShowDetail),
        search(SearchKey::Cancel),
        normal(WatchKey::Quit),
    ]);
    let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
        .expect("replay loop runs");
    assert!(!result.show_detail);
}

#[test]
fn render_watch_frame_detail_panel_replaces_body() {
    let model = WatchModel {
        show_detail: true,
        ..sample_watch_model()
    };
    let frame = render_watch_frame(&model, 100, 24);
    let visible = strip_ansi_for_snapshot(&frame);
    // The detail panel is shown instead of the two-pane table/log body.
    assert!(visible.contains("nodelist    node001"));
    assert!(!visible.contains("svc              step"));
}

#[test]
fn base64_encode_matches_rfc4648_vectors() {
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"f"), "Zg==");
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert_eq!(base64_encode(b"foo"), "Zm9v");
    assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
    assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
}

#[test]
fn osc52_sequence_wraps_base64_payload() {
    assert_eq!(osc52_sequence("hi"), "\u{1b}]52;c;aGk=\u{7}");
}

#[test]
fn effective_services_triage_orders_problems_first() {
    let snapshot = sample_snapshot();
    let names = |services: &[&PsServiceRow]| {
        services
            .iter()
            .map(|s| s.service_name.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&effective_services(
            &snapshot.services,
            None,
            ServiceSort::Spec
        )),
        vec!["api".to_string(), "worker".to_string()]
    );
    // `worker` is unhealthy, so triage order surfaces it first.
    assert_eq!(
        names(&effective_services(
            &snapshot.services,
            None,
            ServiceSort::Triage
        )),
        vec!["worker".to_string(), "api".to_string()]
    );
}

#[test]
fn preserve_selected_index_keeps_service_across_triage_sort() {
    let snapshot = sample_snapshot();
    let selected_index = preserve_selected_index(
        &snapshot.services,
        None,
        ServiceSort::Triage,
        Some("api"),
        0,
    );
    let selected = selected_effective_service(
        &snapshot.services,
        None,
        ServiceSort::Triage,
        selected_index,
    )
    .expect("selected service");
    assert_eq!(selected.service_name, "api");
}

#[test]
fn restart_supported_gates_on_local_backend() {
    let mut record = sample_snapshot().record;
    record.backend = SubmissionBackend::Local;
    assert!(restart_supported(&record));
    record.backend = SubmissionBackend::Slurm;
    assert!(!restart_supported(&record));
}

#[test]
fn request_service_restart_writes_named_request() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let mut record = sample_snapshot().record;
    record.submit_dir = tmpdir.path().to_path_buf();
    let path = request_service_restart(&record, "api").expect("write request");
    assert!(path.exists());
    assert_eq!(
        std::fs::read_to_string(&path).expect("read request").trim(),
        "api"
    );
    assert!(
        std::fs::read_dir(path.parent().expect("request parent"))
            .expect("read request dir")
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp.")),
        "restart request writes should not leave visible temp files"
    );
    let display = path.to_string_lossy();
    assert!(display.contains("dev-control"));
    assert!(display.ends_with(".request"));
}

#[test]
fn watch_loop_restart_writes_request_for_local_job() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {img}\n    command: /bin/true\nx-slurm:\n  cache_dir: {cache}\n",
                img = local_image.display(),
                cache = tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
    let runtime_plan = crate::commands::load::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("job.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-watch-restart-123",
        SubmissionBackend::Local,
    )
    .expect("record");
    write_submission_record(&record).expect("write record");
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
            "services": []
        }))
        .expect("state json"),
    )
    .expect("write state");

    let options = SchedulerOptions {
        squeue_bin: "/definitely/missing-squeue".into(),
        sacct_bin: "/definitely/missing-sacct".into(),
    };
    // HoldOnExit::Always keeps the completed job open so `r` is processed.
    let mut events = ScriptedEvents::new([normal(WatchKey::Restart), normal(WatchKey::Quit)]);
    run_watch_ui_loop(
        &record,
        &options,
        None,
        5,
        HoldOnExit::Always,
        &mut events,
        WatchPrefs::default(),
    )
    .expect("watch loop runs");

    let restart_dir = runtime_job_root_for_record(&record)
        .join("dev-control")
        .join("restart");
    let requests: Vec<_> = fs::read_dir(&restart_dir)
        .expect("restart dir exists")
        .filter_map(|entry| entry.ok())
        .collect();
    assert_eq!(requests.len(), 1, "exactly one restart request written");
}

#[test]
fn watch_loop_navigates_services_via_injected_events() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {img}\n    command: /bin/true\n  worker:\n    image: {img}\n    command: /bin/true\nx-slurm:\n  cache_dir: {cache}\n",
                img = local_image.display(),
                cache = tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
    let runtime_plan = crate::commands::load::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("job.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-watch-nav-123",
        SubmissionBackend::Local,
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

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
            "services": []
        }))
        .expect("state json"),
    )
    .expect("write state");

    let options = SchedulerOptions {
        squeue_bin: "/definitely/missing-squeue".into(),
        sacct_bin: "/definitely/missing-sacct".into(),
    };

    // Resolve the service ordering the snapshot will present so the
    // assertion is independent of spec iteration order.
    let snapshot =
        build_ps_snapshot(&record.compose_file, Some(&record.job_id), &options).expect("snapshot");
    assert!(
        snapshot.services.len() >= 2,
        "fixture must expose at least two services"
    );
    let second_service = snapshot.services[1].service_name.clone();

    // HoldOnExit::Always keeps the completed job's UI open so the down-key
    // is processed before the quit.
    let mut events = ScriptedEvents::new([normal(WatchKey::Down), normal(WatchKey::Quit)]);
    let result = run_watch_ui_loop(
        &record,
        &options,
        None,
        5,
        HoldOnExit::Always,
        &mut events,
        WatchPrefs::default(),
    )
    .expect("watch loop runs");

    assert!(matches!(result.outcome, WatchOutcome::Completed(_)));
    assert_eq!(
        result.selected_service.as_deref(),
        Some(second_service.as_str())
    );
}

#[test]
fn watch_key_navigation_clamps_to_bounds() {
    assert_eq!(apply_watch_key(0, 2, WatchKey::Up), 0);
    assert_eq!(apply_watch_key(0, 2, WatchKey::Down), 1);
    assert_eq!(apply_watch_key(1, 2, WatchKey::Down), 1);
    assert_eq!(apply_watch_key(1, 2, WatchKey::First), 0);
    assert_eq!(apply_watch_key(0, 2, WatchKey::Last), 1);
    assert_eq!(apply_watch_key(0, 0, WatchKey::Down), 0);
}

#[test]
fn parse_keys_recognizes_navigation_sequences() {
    let mut raw = vec![
        b'j', b'k', b'g', b'G', b'\t', 0x1b, b'[', b'A', 0x1b, b'[', b'B', b'q', b'?', b'/',
    ];
    assert_eq!(
        parse_keys(&mut raw),
        vec![
            WatchKey::Down,
            WatchKey::Up,
            WatchKey::First,
            WatchKey::Last,
            WatchKey::Tab,
            WatchKey::Up,
            WatchKey::Down,
            WatchKey::Quit,
            WatchKey::Help,
            WatchKey::Search,
        ]
    );
    assert!(raw.is_empty());
}

#[test]
fn parse_keys_preserves_partial_escape_sequences() {
    let mut raw = vec![0x1b, b'['];
    assert!(parse_keys(&mut raw).is_empty());
    assert_eq!(raw, vec![0x1b, b'[']);

    raw.push(b'A');
    assert_eq!(parse_keys(&mut raw), vec![WatchKey::Up]);
    assert!(raw.is_empty());
}

#[test]
fn render_watch_frame_includes_table_and_log_pane() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["booting".into(), "ready".into()],
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        18,
    );
    assert!(frame.contains("hpc-compose watch"));
    assert!(frame.contains("job 12345"));
    assert!(frame.contains("logs"));
    assert!(frame.contains(">"));
    assert!(frame.contains("api"));
    assert!(frame.contains("ready"));
    assert!(frame.contains("worker"));
    assert!(frame.contains("q quit"));
    assert!(frame.lines().count() <= 18);
}

#[test]
fn render_watch_frame_shows_replay_status_and_controls() {
    let report = sample_replay_report();
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: report.frames[1].snapshot.clone(),
            metrics_line: report.frames[1].metrics_line.clone(),
            replay: Some(ReplayWatchStatus {
                cursor_unix: 110,
                speed: 10.0,
                paused: true,
                fidelity: "best-effort".into(),
                start_unix: 100,
                end_unix: 110,
                event_unix: vec![100, 110],
            }),
            ..sample_watch_model()
        },
        110,
        22,
    );
    let stripped = strip_ansi_for_snapshot(&frame);
    assert!(stripped.contains("hpc-compose replay"));
    assert!(stripped.contains("t=110 | speed=10x | PAUSED | best-effort"));
    assert!(stripped.contains("gpu: 1 util=90% mem=4/8 MiB"));
    assert!(stripped.contains("Space play/pause"));
    assert!(stripped.contains("[/] event"));
}

#[test]
fn render_compact_watch_frame_shows_replay_header() {
    let report = sample_replay_report();
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: report.frames[0].snapshot.clone(),
            replay: Some(ReplayWatchStatus {
                cursor_unix: 100,
                speed: 1.0,
                paused: false,
                fidelity: "best-effort".into(),
                start_unix: 100,
                end_unix: 110,
                event_unix: vec![100, 110],
            }),
            ..sample_watch_model()
        },
        60,
        10,
    );
    let stripped = strip_ansi_for_snapshot(&frame);
    assert!(stripped.contains("hpc-compose replay"));
    assert!(stripped.contains("speed=1x"));
    assert!(stripped.contains("Space play/pause"));
}

#[test]
fn replay_key_navigation_updates_playback_state() {
    let report = sample_replay_report();
    let state = ReplayPlaybackState::new(&report, 1.0);
    let paused = apply_replay_key(state, &report, WatchKey::TogglePause);
    assert!(paused.paused);
    let next = apply_replay_key(paused, &report, WatchKey::NextEvent);
    assert_eq!(next.frame_index, 1);
    assert_eq!(next.cursor_unix, 110);
    let faster = apply_replay_key(next, &report, WatchKey::SpeedUp);
    assert_eq!(faster.speed, 10.0);
    let slower = apply_replay_key(faster, &report, WatchKey::SpeedDown);
    assert_eq!(slower.speed, 1.0);
    let first = apply_replay_key(next, &report, WatchKey::ReplayStart);
    assert_eq!(first.frame_index, 0);
    let final_state = apply_replay_key(first, &report, WatchKey::End);
    assert_eq!(final_state.frame_index, 1);
}

#[test]
fn render_watch_frame_normal_snapshot_stays_stable() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["booting".into(), "ready".into()],
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        18,
    );
    let lines = canonical_frame_lines(&frame);

    assert_anchored_line(
        &lines,
        "hpc-compose watch",
        "hpc-compose watch | RUNNING (squeue) | job 12345",
    );
    assert_anchored_line(
        &lines,
        "services:",
        "services: 2 | selected: api | logs: selected FOLLOW",
    );
    // Body rows are addressed relative to the table header so an inserted
    // status/notice line above it cannot silently shift these assertions.
    let table_header = anchor_line(&lines, "svc              step");
    assert!(lines[table_header + 1].contains("api")); // first service row
    assert!(lines[table_header + 2].contains("booting")); // first streamed log line
    assert!(lines.last().unwrap_or(&String::new()).contains("q quit"));
}

#[test]
fn env_and_terminal_helpers_cover_force_and_fallback_paths() {
    assert!(force_watch_ui_from_value(Some(OsStr::new("1"))));
    assert!(!force_watch_ui_from_value(Some(OsStr::new("0"))));
    assert!(!force_watch_ui_from_value(None));

    assert!(watch_ui_available(true, false, false, false));
    assert!(watch_ui_available(false, false, true, true));
    assert!(!watch_ui_available(false, false, true, false));
    assert!(!watch_ui_available(true, true, true, true));

    assert_eq!(fallback_terminal_size(Some("101"), Some("33")), (101, 33));
    assert_eq!(
        fallback_terminal_size(Some("bad"), Some("also-bad")),
        (DEFAULT_WIDTH, DEFAULT_HEIGHT)
    );
    assert_eq!(parse_terminal_env_size(Some("72"), DEFAULT_WIDTH), 72);
    assert_eq!(
        parse_terminal_env_size(Some("not-a-number"), DEFAULT_WIDTH),
        DEFAULT_WIDTH
    );
}

#[test]
fn ctrl_c_maps_to_quit() {
    assert_eq!(
        map_key_event(
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            InputMode::Normal
        ),
        Some(WatchInput::Normal(WatchKey::Quit))
    );
    assert_eq!(
        map_key_event(
            KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
            InputMode::Normal
        ),
        Some(WatchInput::Search(SearchKey::Clear))
    );
}

#[test]
fn map_key_event_is_mode_aware_for_text_entry() {
    // `q` quits in normal mode but is plain text while typing a query.
    assert_eq!(
        map_key_event(
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
            InputMode::Normal
        ),
        Some(WatchInput::Normal(WatchKey::Quit))
    );
    for mode in [InputMode::Search, InputMode::LogSearch] {
        assert_eq!(
            map_key_event(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE), mode),
            Some(WatchInput::Search(SearchKey::Char('q')))
        );
        assert_eq!(
            map_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), mode),
            Some(WatchInput::Search(SearchKey::Submit))
        );
    }
}

#[test]
fn map_mouse_event_scrolls_log_pane() {
    assert_eq!(
        map_mouse_event(MouseEventKind::ScrollUp),
        Some(WatchInput::Normal(WatchKey::PageUp))
    );
    assert_eq!(
        map_mouse_event(MouseEventKind::ScrollDown),
        Some(WatchInput::Normal(WatchKey::PageDown))
    );
    assert_eq!(map_mouse_event(MouseEventKind::Moved), None);
}

#[test]
fn selection_and_formatting_helpers_cover_remaining_paths() {
    let snapshot = sample_snapshot();
    assert_eq!(initial_selected_index(&snapshot, None).expect("default"), 0);
    assert_eq!(
        initial_selected_index(&snapshot, Some("worker")).expect("selected worker"),
        1
    );
    let err = initial_selected_index(&snapshot, Some("missing")).expect_err("missing service");
    assert!(err.to_string().contains("does not exist"));

    let mut empty = snapshot.clone();
    empty.services.clear();
    assert_eq!(clamp_selected_index(&empty, 5), 0);
    assert_eq!(clamp_selected_index(&snapshot, 7), 1);

    assert_eq!(log_capacity(2), 4);
    assert_eq!(log_capacity(12), 6);
    assert_eq!(yes_no_short(true), "yes");
    assert_eq!(yes_no_short(false), "no");
    assert_eq!(truncate_cell("abcdef", 3), "abc");
    assert_eq!(fit_line("abcdef", 4), "abcd");
    assert_eq!(pad_line("abc", 5), "abc  ");
    assert_eq!(
        capped_lines(vec!["a".into(), "b".into(), "c".into()], 2),
        vec!["b", "c"]
    );
}

#[test]
fn ansi_aware_formatting_uses_visible_width() {
    let truncated = fit_line("\x1b[31mabcdef\x1b[39m", 4);
    assert_eq!(visible_width(&truncated), 4);
    assert!(truncated.starts_with("\x1b[31m"));
    assert!(truncated.ends_with(ANSI_RESET_ALL));
    assert!(truncated.contains("abcd"));
    assert!(!truncated.contains("abcde"));

    let padded = pad_line("\x1b[32mabc\x1b[39m", 5);
    assert_eq!(visible_width(&padded), 5);
    assert!(padded.ends_with("  "));
}

fn strip_ansi_for_snapshot(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    while index < value.len() {
        if let Some(len) = ansi_escape_len(bytes, index) {
            index += len;
            continue;
        }
        let ch = value[index..]
            .chars()
            .next()
            .expect("strip_ansi_for_snapshot walked a valid UTF-8 boundary");
        out.push(ch);
        index += ch.len_utf8();
    }
    out
}

fn canonical_frame_lines(frame: &str) -> Vec<String> {
    strip_ansi_for_snapshot(frame)
        .replace('\u{2502}', "|")
        .lines()
        .map(|line| line.trim_end().to_string())
        .collect()
}

/// Returns the index of the one rendered line containing `needle`, panicking if
/// zero or more than one line matches. Anchoring on stable content keeps
/// assertions valid when unrelated header/status lines shift absolute indices.
fn anchor_line(lines: &[String], needle: &str) -> usize {
    let matches: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains(needle))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one line containing {needle:?}, found indices {matches:?} in {lines:?}"
    );
    matches[0]
}

/// Content-anchored replacement for absolute-index assertions: finds the unique
/// line containing `needle` and asserts its full (trimmed) content equals
/// `expected`. Returns the anchor index so callers can address neighbouring
/// rows by relative offset.
fn assert_anchored_line(lines: &[String], needle: &str, expected: &str) -> usize {
    let index = anchor_line(lines, needle);
    assert_eq!(
        lines[index].as_str(),
        expected,
        "unexpected content on line anchored by {needle:?}"
    );
    index
}

#[test]
fn read_new_lines_and_selected_log_buffer_cover_growth_and_reset_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log_path = tmpdir.path().join("service.log");
    fs::write(&log_path, "one\ntwo\npart").expect("seed log");

    let mut offset = 0;
    let mut pending = String::new();
    let lines = read_new_lines(&log_path, &mut offset, &mut pending).expect("initial read");
    assert_eq!(lines, vec!["one", "two"]);
    assert_eq!(pending, "part");

    fs::write(&log_path, "reset\n").expect("truncate log");
    let lines = read_new_lines(&log_path, &mut offset, &mut pending).expect("truncated read");
    assert_eq!(lines, vec!["reset"]);
    assert!(pending.is_empty());

    let missing = tmpdir.path().join("missing.log");
    let lines = read_new_lines(&missing, &mut offset, &mut pending).expect("missing log");
    assert!(lines.is_empty());
    assert_eq!(offset, 0);
    assert!(pending.is_empty());

    fs::write(&log_path, "alpha\nbeta\ngamma\n").expect("rewrite log");
    let row = PsServiceRow {
        service_name: "api".into(),
        path: log_path.clone(),
        present: true,
        updated_at: None,
        updated_age_seconds: None,
        log_path: Some(log_path.clone()),
        step_name: Some("hpc-compose:api".into()),
        launch_index: Some(0),
        launcher_pid: Some(4242),
        healthy: Some(true),
        completed_successfully: Some(false),
        readiness_configured: Some(true),
        status: Some("ready".into()),
        failure_policy_mode: None,
        restart_count: Some(0),
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
    };

    let mut buffer = SelectedLogBuffer::seed(Some(&row), 2, 2);
    assert_eq!(buffer.lines, vec!["beta", "gamma"]);

    fs::write(&log_path, "alpha\nbeta\ngamma\ndelta\n").expect("append log");
    buffer.refresh().expect("refresh");
    assert_eq!(buffer.lines, vec!["gamma", "delta"]);

    let other_path = tmpdir.path().join("worker.log");
    fs::write(&other_path, "worker-started\n").expect("other log");
    let other = PsServiceRow {
        service_name: "worker".into(),
        path: other_path.clone(),
        present: true,
        updated_at: None,
        updated_age_seconds: None,
        log_path: Some(other_path),
        step_name: Some("hpc-compose:worker".into()),
        launch_index: Some(1),
        launcher_pid: Some(5252),
        healthy: Some(false),
        completed_successfully: Some(false),
        readiness_configured: Some(false),
        status: Some("running".into()),
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
    };
    buffer.reseed_if_needed(Some(&other), 5, 4);
    assert_eq!(buffer.service_name, "worker");
    assert_eq!(buffer.lines, vec!["worker-started"]);

    buffer.reseed_if_needed(None, 5, 4);
    assert_eq!(buffer.service_name, "<none>");
    assert!(buffer.lines.is_empty());
}

#[test]
fn render_watch_frame_prefers_detail_then_pending_reason() {
    let mut detail_snapshot = sample_snapshot();
    detail_snapshot.scheduler.detail = Some("visible in queue".into());
    let detail_frame = render_watch_frame(
        &WatchModel {
            snapshot: detail_snapshot,
            selected_index: 1,
            walltime_progress: None,
            log_lines: vec!["tail".into()],
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        90,
        14,
    );
    assert!(detail_frame.contains("note: visible in queue"));

    let mut pending_snapshot = sample_snapshot();
    pending_snapshot.scheduler.state = "PENDING".into();
    pending_snapshot.queue_diagnostics = Some(QueueDiagnostics {
        pending_reason: Some("Resources".into()),
        eligible_time: None,
        start_time: None,
    });
    let pending_frame = render_watch_frame(
        &WatchModel {
            snapshot: pending_snapshot,
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        90,
        14,
    );
    assert!(pending_frame.contains("pending reason"));
    assert!(pending_frame.contains("Resources"));
}

#[test]
fn render_watch_frame_includes_walltime_bar_when_available() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: Some(WalltimeProgress {
                original: "00:10:00".into(),
                elapsed_seconds: 300,
                total_seconds: 600,
                remaining_seconds: 300,
            }),
            log_lines: Vec::new(),
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        14,
    );
    assert!(frame.contains("walltime: ["));
    assert!(frame.contains("50% 00:05:00 / 00:10:00 remaining 00:05:00"));
}

#[test]
fn restore_terminal_best_effort_is_idempotent() {
    // Drop, the panic hook, and the SIGTERM/SIGHUP handler can each reach the
    // restore; the AtomicBool guard makes only one win, but restoring twice must
    // still be harmless. Off a tty (CI) the crossterm calls may warn, never panic.
    restore_terminal_best_effort();
    restore_terminal_best_effort();
}

#[test]
fn terminal_guard_and_run_watch_ui_cover_interactive_paths() {
    let guard = TerminalGuard::enter(false).expect("enter terminal guard");
    assert!(guard.panic_restore_armed());
    drop(guard);
    assert!(!TERMINAL_RESTORE_ARMED.load(Ordering::SeqCst));

    let model = WatchModel {
        snapshot: sample_snapshot(),
        selected_index: 0,
        walltime_progress: None,
        log_lines: vec!["line".into()],
        show_help: false,
        filter: None,
        search_buffer: String::new(),
        input_mode: InputMode::Normal,
        ..sample_watch_model()
    };
    let mut renderer = FrameRenderer::new();
    renderer
        .render(&render_watch_frame(&model, 90, 14), (90, 14))
        .expect("render frame");
    // A second identical render exercises the no-change diff path.
    renderer
        .render(&render_watch_frame(&model, 90, 14), (90, 14))
        .expect("render frame again");
    // A different size forces the resize/full-repaint path.
    renderer
        .render(&render_watch_frame(&model, 70, 10), (70, 10))
        .expect("render frame resized");

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
    let runtime_plan = crate::commands::load::load_runtime_plan(&compose).expect("runtime plan");
    let script_path = tmpdir.path().join("job.local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &runtime_plan,
        "local-watch-ui-123",
        SubmissionBackend::Local,
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

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
            "services": []
        }))
        .expect("state json"),
    )
    .expect("write state");

    let outcome = run_watch_ui(
        &record,
        &SchedulerOptions {
            squeue_bin: "/definitely/missing-squeue".into(),
            sacct_bin: "/definitely/missing-sacct".into(),
        },
        None,
        5,
        HoldOnExit::Never,
        WatchPrefs::default(),
    )
    .expect("run watch ui");
    assert!(matches!(outcome, WatchOutcome::Completed(_)));
}

#[test]
fn should_hold_on_exit_matches_policy_and_terminal_outcome() {
    fn status(state: &str, failed: bool) -> SchedulerStatus {
        SchedulerStatus {
            state: state.into(),
            source: SchedulerSource::Sacct,
            terminal: true,
            failed,
            detail: None,
        }
    }

    let completed = WatchOutcome::Completed(status("COMPLETED", false));
    let failed = WatchOutcome::Failed(status("FAILED", true));
    let unknown = WatchOutcome::Unknown(status("unknown", false));
    let interrupted = WatchOutcome::Interrupted(status("RUNNING", false));

    for outcome in [&completed, &failed, &unknown, &interrupted] {
        assert!(!should_hold_on_exit(HoldOnExit::Never, outcome));
    }

    assert!(!should_hold_on_exit(HoldOnExit::Failure, &completed));
    assert!(should_hold_on_exit(HoldOnExit::Failure, &failed));
    assert!(!should_hold_on_exit(HoldOnExit::Failure, &unknown));
    assert!(!should_hold_on_exit(HoldOnExit::Failure, &interrupted));

    assert!(should_hold_on_exit(HoldOnExit::Always, &completed));
    assert!(should_hold_on_exit(HoldOnExit::Always, &failed));
    assert!(!should_hold_on_exit(HoldOnExit::Always, &unknown));
    assert!(!should_hold_on_exit(HoldOnExit::Always, &interrupted));
}

#[test]
fn filtered_services_narrows_by_name() {
    let snapshot = sample_snapshot();
    let all = filtered_services(&snapshot.services, None);
    assert_eq!(all.len(), 2);
    let narrowed = filtered_services(&snapshot.services, Some("api"));
    assert_eq!(narrowed.len(), 1);
    assert_eq!(narrowed[0].service_name, "api");
    let none = filtered_services(&snapshot.services, Some("missing"));
    assert_eq!(none.len(), 0);
}

#[test]
fn render_watch_frame_shows_help_overlay() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            show_help: true,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        28,
    );
    assert!(frame.contains("Keybindings:"));
    assert!(frame.contains("j / Down"));
    assert!(frame.contains("f           find in logs"));
    assert!(frame.contains("w           toggle log line wrap"));
    assert!(frame.contains("o           cycle service sort"));
    assert!(frame.contains("q           quit"));
    assert!(frame.contains("q quit"));
    assert!(frame.lines().count() <= 28);
}

#[test]
fn render_watch_frame_help_snapshot_stays_stable() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            show_help: true,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        28,
    );
    let lines = canonical_frame_lines(&frame);

    assert!(lines.iter().any(|line| line == "Keybindings:"));
    assert!(
        lines
            .iter()
            .any(|line| line == "  /           filter services by name")
    );
    assert!(lines.iter().any(|line| line == "  q           quit"));
    assert!(lines.last().unwrap_or(&String::new()).contains("q quit"));
}

#[test]
fn render_watch_frame_shows_filter_indicator() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            show_help: false,
            filter: Some("api".into()),
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        14,
    );
    assert!(frame.contains("filter: api"));
}

#[test]
fn render_watch_frame_filtered_snapshot_stays_stable() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            show_help: false,
            filter: Some("api".into()),
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        100,
        14,
    );
    let lines = canonical_frame_lines(&frame);

    assert_anchored_line(
        &lines,
        "hpc-compose watch",
        "hpc-compose watch | RUNNING (squeue) | job 12345 | filter: api",
    );
    assert_anchored_line(
        &lines,
        "services:",
        "services: 1 | selected: api | logs: selected FOLLOW",
    );
    assert!(lines.iter().any(|line| line.contains("> api")));
    assert!(!lines.iter().any(|line| line.contains("worker")));
}

#[test]
fn render_watch_frame_bounds_footer_search_and_help() {
    let search_frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["tail".into()],
            show_help: false,
            filter: None,
            search_buffer: "api".into(),
            input_mode: InputMode::Search,
            ..sample_watch_model()
        },
        90,
        12,
    );
    assert!(search_frame.contains("filter: api"));
    assert!(
        search_frame
            .lines()
            .last()
            .unwrap_or("")
            .contains("Enter apply")
    );
    assert!(search_frame.lines().count() <= 12);

    let help_frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["tail".into()],
            show_help: true,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        90,
        12,
    );
    assert!(help_frame.contains("Keybindings:"));
    assert!(help_frame.lines().last().unwrap_or("").contains("q quit"));
    assert!(help_frame.lines().count() <= 12);
}

#[test]
fn render_watch_frame_respects_narrow_terminal_dimensions() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: Some(WalltimeProgress {
                original: "00:10:00".into(),
                elapsed_seconds: 300,
                total_seconds: 600,
                remaining_seconds: 300,
            }),
            log_lines: vec![
                "a deliberately long log line that must not wrap the terminal".into(),
                "ready".into(),
            ],
            show_help: true,
            filter: Some("api".into()),
            search_buffer: "api".into(),
            input_mode: InputMode::Search,
            ..sample_watch_model()
        },
        48,
        9,
    );

    let lines = frame.lines().collect::<Vec<_>>();
    assert!(lines.len() <= 9);
    assert!(lines.iter().all(|line| visible_width(line) <= 48));
    assert!(frame.contains("hpc-compose watch"));
    assert!(frame.contains("RUNNING"));
}

#[test]
fn render_watch_frame_compact_snapshot_stays_stable() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["tail".into()],
            show_help: true,
            filter: Some("api".into()),
            search_buffer: "api".into(),
            input_mode: InputMode::Search,
            ..sample_watch_model()
        },
        48,
        9,
    );
    let lines = canonical_frame_lines(&frame);

    assert_anchored_line(&lines, "hpc-compose watch", "hpc-compose watch | job 12345");
    assert_anchored_line(&lines, "filter: api", "filter: api");
    assert_anchored_line(&lines, "filter input:", "filter input: api");
    assert_anchored_line(
        &lines,
        "? help | /",
        "? help | / filter | f find | w wrap | o sort | q",
    );
    assert_anchored_line(&lines, "> api", "> api OK ready=yes");
    // Exact height is load-bearing here: the compact layout must pack the whole
    // UI (header, filter block, one service row, log title, footer) into exactly
    // the 9 rows requested, with no blank padding or overflow.
    assert_eq!(lines.len(), 9);
}

#[test]
fn render_watch_frame_handles_tiny_terminal_without_overflow() {
    let frame = render_watch_frame(
        &WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["tail".into()],
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        },
        12,
        3,
    );

    let lines = frame.lines().collect::<Vec<_>>();
    assert!(lines.len() <= 3);
    assert!(lines.iter().all(|line| visible_width(line) <= 12));
    assert!(frame.contains("hpc-compose"));
}

#[test]
fn tail_lines_reads_large_log_suffix_and_decodes_lossily() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("watch.log");
    let mut bytes = Vec::new();
    for index in 0..5_000 {
        bytes.extend_from_slice(format!("line-{index}\n").as_bytes());
    }
    bytes.extend_from_slice(b"bad-\xff\nlast\n");
    fs::write(&log, bytes).expect("large log");

    let tailed = tail_lines(&log, 2).expect("tail large log");

    assert_eq!(tailed, vec!["bad-\u{fffd}".to_string(), "last".to_string()]);
}

#[test]
fn search_keys_parse_correctly() {
    let mut buf = vec![b'a', b'b', 0x7f, b'\n'];
    let keys = parse_search_keys(&mut buf);
    assert_eq!(
        keys,
        vec![
            SearchKey::Char('a'),
            SearchKey::Char('b'),
            SearchKey::Backspace,
            SearchKey::Submit,
        ]
    );

    let mut cancel_buf = vec![0x1b];
    let keys = parse_search_keys(&mut cancel_buf);
    assert_eq!(keys, vec![SearchKey::Cancel]);
}

fn snapshot_with_job(job_id: &str) -> Box<PsSnapshot> {
    let mut snapshot = sample_snapshot();
    snapshot.record.job_id = job_id.into();
    Box::new(snapshot)
}

#[test]
fn drain_worker_messages_keeps_freshest_of_each_kind() {
    // Queue three data snapshots and two metrics lines; the drain must collapse
    // them to only the newest of each so the UI never applies stale state.
    let messages = vec![
        WatchWorkerMsg::Data(Ok(snapshot_with_job("first"))),
        WatchWorkerMsg::Metrics(Some("stale metrics".into())),
        WatchWorkerMsg::Data(Ok(snapshot_with_job("second"))),
        WatchWorkerMsg::Metrics(Some("fresh metrics".into())),
        WatchWorkerMsg::Data(Ok(snapshot_with_job("third"))),
    ];

    let drained = drain_worker_messages(messages);
    let data = drained.data.expect("data present").expect("snapshot ok");
    assert_eq!(
        data.record.job_id, "third",
        "the newest snapshot must win over queued stale ones"
    );
    assert_eq!(drained.metrics, Some(Some("fresh metrics".into())));
}

#[test]
fn drain_worker_messages_empty_yields_nothing() {
    let drained = drain_worker_messages(Vec::new());
    assert!(drained.data.is_none());
    assert!(drained.metrics.is_none());
}

#[test]
fn drain_worker_messages_propagates_freshest_data_error() {
    // An error queued after an ok snapshot is still the freshest data message and
    // must be the one surfaced (so the loop propagates it just like the old
    // inline `build_ps_snapshot(...)?`).
    let messages = vec![
        WatchWorkerMsg::Data(Ok(snapshot_with_job("ok"))),
        WatchWorkerMsg::Data(Err(anyhow::anyhow!("probe failed"))),
    ];
    let drained = drain_worker_messages(messages);
    let data = drained.data.expect("data present");
    assert!(data.is_err(), "freshest (error) data message must win");
}

#[test]
fn apply_worker_metrics_updates_watch_model_on_change() {
    let mut model = sample_watch_model();
    assert!(model.metrics_line.is_none());

    // A new value updates the model and reports a repaint is warranted.
    assert!(apply_worker_metrics(
        &mut model.metrics_line,
        Some("gpu: 1 util=50%".into())
    ));
    assert_eq!(model.metrics_line.as_deref(), Some("gpu: 1 util=50%"));

    // Re-applying the same value is a no-op and reports no change.
    assert!(!apply_worker_metrics(
        &mut model.metrics_line,
        Some("gpu: 1 util=50%".into())
    ));
    assert_eq!(model.metrics_line.as_deref(), Some("gpu: 1 util=50%"));

    // Clearing to `None` is a change.
    assert!(apply_worker_metrics(&mut model.metrics_line, None));
    assert!(model.metrics_line.is_none());
}
