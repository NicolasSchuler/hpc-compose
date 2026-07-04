use super::*;

pub(crate) fn status(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    array: bool,
) -> Result<()> {
    let lookup_job_id = if array {
        job_id
            .as_deref()
            .and_then(|value| value.split_once('_').map(|(parent, _)| parent.to_string()))
            .or_else(|| job_id.clone())
    } else {
        job_id.clone()
    };
    let record = match lookup_job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, lookup_job_id.as_deref())?,
        None => None,
    };
    if lookup_job_id.is_some() && record.is_none() {
        bail!("{}", tracked_job_hint(lookup_job_id.as_deref()));
    }
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value,
        sacct_bin: context.binaries.sacct.value,
    };
    let mut snapshot = build_status_snapshot_with_array(
        compose_file,
        lookup_job_id.as_deref(),
        &scheduler_options,
        array,
    )?;
    if array && job_id.as_deref() != lookup_job_id.as_deref() {
        snapshot.array = Some(build_array_status_snapshot(
            &snapshot.record,
            job_id.as_deref(),
            &scheduler_options,
        )?);
    }
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_status_snapshot(&snapshot).context("failed to write status output")?;
            let job_id = snapshot.record.job_id.as_str();
            let export_dir_configured = snapshot
                .record
                .artifact_export_dir
                .as_deref()
                .is_some_and(|export_dir| !export_dir.trim().is_empty());
            output::print_next_steps(&output::inspect_next_commands(
                (!job_id.is_empty()).then_some(job_id),
                export_dir_configured,
            ));
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::StatusOutput::new(snapshot))
                    .context("failed to serialize status output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn stats(
    context: ResolvedContext,
    job_id: Option<String>,
    json: bool,
    format: Option<StatsOutputFormat>,
    accounting: bool,
) -> Result<()> {
    let record = match job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, job_id.as_deref())?,
        None => None,
    };
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let snapshot = build_stats_snapshot(
        compose_file,
        job_id.as_deref(),
        &StatsOptions {
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            sstat_bin: context.binaries.sstat.value,
            accounting,
        },
    )?;
    match output::resolve_stats_output_format(format, json) {
        StatsOutputFormat::Text => {
            output::print_stats_snapshot(&snapshot).context("failed to write stats output")?;
        }
        StatsOutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::StatsOutput::new(snapshot))
                    .context("failed to serialize stats output")?
            );
        }
        StatsOutputFormat::Csv => output::write_stats_snapshot_csv(&mut io::stdout(), &snapshot)
            .context("failed to write csv stats output")?,
        StatsOutputFormat::Jsonl => {
            output::write_stats_snapshot_jsonl(&mut io::stdout(), &snapshot)
                .context("failed to write jsonl stats output")?;
        }
    }
    Ok(())
}

pub(crate) fn metrics_probe(
    duration_seconds: u64,
    format: OutputFormat,
    compare_nvidia_smi: bool,
) -> Result<()> {
    if format != OutputFormat::Json {
        bail!("metrics-probe currently supports only --format json");
    }
    let options = MetricsProbeOptions {
        duration_seconds,
        compare_nvidia_smi,
    };
    validate_metrics_probe_options(options)?;
    let report = build_metrics_probe_report(options)?;
    println!("{}", serialize_metrics_probe_report(&report)?);
    Ok(())
}

pub(crate) fn score(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    pue: f64,
    gpu_tdp_w: f64,
    cpu_watts_per_core: f64,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &record.compose_file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )
        .with_context(|| {
            format!(
                "failed to load runtime plan for tracked job {} from {}",
                record.job_id,
                record.compose_file.display()
            )
        })?;
    let report = build_efficiency_score_report(
        &runtime_plan,
        &record,
        &EfficiencyScoreOptions {
            scheduler: SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            sstat_bin: context.binaries.sstat.value,
            pue,
            gpu_tdp_w,
            cpu_watts_per_core,
        },
    )?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_efficiency_score_report(&report)
                .context("failed to write score output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::ScoreOutput::new(report))
                    .context("failed to serialize score output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn artifacts(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    bundles: Vec<String>,
    tarball: bool,
) -> Result<()> {
    let report = export_artifacts(
        &context.compose_file.value,
        job_id.as_deref(),
        &ArtifactExportOptions {
            selected_bundles: bundles,
            tarball,
        },
    )?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_artifact_export_report(&report)
                .context("failed to write artifacts output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::ArtifactsOutput::new(report))
                    .context("failed to serialize artifacts output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn diff(
    context: ResolvedContext,
    job_id_1: Option<String>,
    job_id_2: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (Some(job_id_1), Some(job_id_2)) = (job_id_1, job_id_2) else {
        bail!(
            "pairwise diff requires two tracked job ids; pass both (e.g. 'hpc-compose diff 12345 12346'), or use --across <sweep>/--jobs a,b,c for an N-way matrix"
        );
    };
    let left = resolve_tracked_record(&context, Some(&job_id_1))?
        .with_context(|| format!("tracked job '{job_id_1}' was not found"))?;
    let right = resolve_tracked_record(&context, Some(&job_id_2))?
        .with_context(|| format!("tracked job '{job_id_2}' was not found"))?;
    let report = build_job_diff_report(
        &left,
        &right,
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value,
            sacct_bin: context.binaries.sacct.value,
        },
    );
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_job_diff_report(&report).context("failed to write diff output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::DiffOutput::new(report))
                    .context("failed to serialize diff output")?
            );
        }
    }
    Ok(())
}

/// Builds an N-way comparison matrix over either every submitted trial of a
/// sweep (`--across`) or an explicit list of tracked job ids (`--jobs`).
pub(crate) fn diff_matrix(
    context: ResolvedContext,
    across: Option<String>,
    jobs: Vec<String>,
    format: Option<DiffMatrixFormat>,
) -> Result<()> {
    let options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let mut records = Vec::new();
    let mut notes = Vec::new();
    match across {
        Some(sweep) => {
            let manifest = load_sweep_manifest(&context.compose_file.value, Some(&sweep))?;
            for trial in &manifest.trials {
                match trial.job_id.as_deref() {
                    Some(job_id) => {
                        let record = resolve_tracked_record(&context, Some(job_id))?
                            .with_context(|| format!("tracked job '{job_id}' was not found"))?;
                        records.push(record);
                    }
                    None => notes.push(format!(
                        "trial '{}' has not been submitted; skipping",
                        trial.trial_id
                    )),
                }
            }
        }
        None => {
            for job_id in &jobs {
                let record = resolve_tracked_record(&context, Some(job_id))?
                    .with_context(|| format!("tracked job '{job_id}' was not found"))?;
                records.push(record);
            }
        }
    }
    if records.len() < 2 {
        bail!(
            "an N-way diff needs at least two resolvable runs; found {}",
            records.len()
        );
    }

    let mut report = build_job_matrix_report(&records, &options);
    // Surface any unsubmitted-trial notes alongside the builder's own notes.
    report.notes.splice(0..0, notes);

    match output::resolve_diff_matrix_format(format) {
        DiffMatrixFormat::Text => {
            output::print_job_matrix_report(&report).context("failed to write diff matrix")?;
        }
        DiffMatrixFormat::Csv => println!("{}", output::job_matrix_csv(&report)),
        DiffMatrixFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::DiffMatrixOutput::new(report))
                    .context("failed to serialize diff matrix")?
            );
        }
    }
    Ok(())
}

pub(crate) fn logs(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    follow: bool,
    lines: usize,
    grep: Option<String>,
    since: Option<String>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let since_seconds = since.as_deref().map(parse_log_since_duration).transpose()?;
    print_logs(
        &record,
        &hpc_compose::job::LogPrintOptions {
            service,
            lines,
            follow,
            grep,
            since_seconds,
        },
    )
}

pub(crate) fn ps(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = match job_id.as_deref() {
        Some(_) => resolve_tracked_record(&context, job_id.as_deref())?,
        None => None,
    };
    if job_id.is_some() && record.is_none() {
        bail!("{}", tracked_job_hint(job_id.as_deref()));
    }
    let compose_file = record
        .as_ref()
        .map(|record| record.compose_file.as_path())
        .unwrap_or(context.compose_file.value.as_path());
    let snapshot = build_ps_snapshot(
        compose_file,
        job_id.as_deref(),
        &SchedulerOptions {
            squeue_bin: context.binaries.squeue.value,
            sacct_bin: context.binaries.sacct.value,
        },
    )?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            output::print_ps_snapshot(&snapshot).context("failed to write ps output")?;
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::PsOutput::new(snapshot))
                    .context("failed to serialize ps output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn watch(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    lines: usize,
    watch_mode: WatchMode,
    hold_on_exit: HoldOnExit,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value,
                sacct_bin: context.binaries.sacct.value,
            },
            service.as_deref(),
            lines,
            watch_mode,
            hold_on_exit,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

pub(crate) fn replay(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    speed: f64,
    lines: usize,
    watch_mode: WatchMode,
    format: Option<OutputFormat>,
) -> Result<()> {
    if !speed.is_finite() || speed <= 0.0 {
        bail!("replay --speed must be a positive finite number");
    }
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let report = build_replay_report(&record, service.as_deref())?;
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::ReplayOutput::new(report))
                    .context("failed to serialize replay output")?
            );
            Ok(())
        }
        OutputFormat::Text => match watch_mode {
            WatchMode::Line => print_replay_summary(&report),
            WatchMode::Tui => watch_ui::run_replay_ui(
                &report,
                service.as_deref(),
                lines,
                speed,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )
            .context("replay UI requested with --watch-mode tui but could not be started"),
            WatchMode::Auto => {
                if watch_ui::can_use_watch_ui() {
                    match watch_ui::run_replay_ui(
                        &report,
                        service.as_deref(),
                        lines,
                        speed,
                        watch_ui::WatchPrefs::resolve(&context.watch),
                    ) {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            let _ = writeln!(
                                io::stderr(),
                                "warning: replay UI unavailable ({err}); printing static replay summary"
                            );
                            let _ = io::stderr().flush();
                            print_replay_summary(&report)
                        }
                    }
                } else {
                    print_replay_summary(&report)
                }
            }
        },
    }
}

fn print_replay_summary(report: &hpc_compose::job::ReplayReport) -> Result<()> {
    let mut stdout = io::stdout();
    writeln!(
        stdout,
        "hpc-compose replay | job {} | {}",
        report.job_id, report.fidelity
    )
    .context("failed to write replay output")?;
    if let (Some(start), Some(end)) = (report.timeline_start_unix, report.timeline_end_unix) {
        writeln!(stdout, "timeline: {start}..{end}")?;
    }
    writeln!(stdout, "events: {}", report.events.len())?;
    if !report.notes.is_empty() {
        writeln!(stdout, "notes:")?;
        for note in &report.notes {
            writeln!(stdout, "  - {note}")?;
        }
    }
    writeln!(stdout, "artifacts:")?;
    for path in &report.artifacts.runtime_roots {
        writeln!(stdout, "  runtime: {}", path.display())?;
    }
    for path in &report.artifacts.service_exit_dirs {
        writeln!(stdout, "  service-exits: {}", path.display())?;
    }
    for path in &report.artifacts.metrics_dirs {
        writeln!(stdout, "  metrics: {}", path.display())?;
    }
    writeln!(stdout, "timeline:")?;
    for frame in &report.frames {
        let event = &frame.event;
        let service = event
            .service
            .as_deref()
            .map(|service| format!(" service={service}"))
            .unwrap_or_default();
        let exit = event
            .exit_code
            .map(|code| format!(" exit={code}"))
            .unwrap_or_default();
        let detail = event
            .detail
            .as_deref()
            .map(|detail| format!(" ({detail})"))
            .unwrap_or_default();
        let metrics = frame
            .metrics_line
            .as_deref()
            .map(|line| format!(" | metrics: {line}"))
            .unwrap_or_default();
        writeln!(
            stdout,
            "  {} {}{}{}{}{}",
            event.at_unix,
            replay_event_kind_label(event.kind),
            service,
            exit,
            detail,
            metrics
        )?;
    }
    stdout.flush().context("failed to flush replay output")
}

fn replay_event_kind_label(kind: hpc_compose::job::ReplayEventKind) -> &'static str {
    match kind {
        hpc_compose::job::ReplayEventKind::AttemptStart => "attempt_start",
        hpc_compose::job::ReplayEventKind::ServiceStart => "service_start",
        hpc_compose::job::ReplayEventKind::MetricsSample => "metrics_sample",
        hpc_compose::job::ReplayEventKind::ServiceExit => "service_exit",
        hpc_compose::job::ReplayEventKind::FinalSnapshot => "final_snapshot",
    }
}

pub(crate) fn tracked_job_hint(job_id: Option<&str>) -> String {
    match job_id {
        Some(job_id) => format!(
            "tracked job '{job_id}' was not found from this repository; run `hpc-compose jobs list` to inspect known tracked jobs"
        ),
        None => "no tracked job was found for the active compose file; run `hpc-compose jobs list` to inspect known tracked jobs".to_string(),
    }
}
