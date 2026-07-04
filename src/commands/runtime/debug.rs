use super::*;

#[derive(Debug, Serialize)]
struct DebugLogTail {
    service_name: Option<String>,
    path: PathBuf,
    present: bool,
    lines: Vec<String>,
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct DebugSummary {
    scheduler_state: Option<String>,
    failed_service: Option<String>,
    exit_code: Option<i64>,
    log_path: Option<PathBuf>,
    next_command: String,
}

#[derive(Debug, Serialize)]
struct DebugReport {
    tracked: bool,
    compose_file: PathBuf,
    job_id: Option<String>,
    summary: DebugSummary,
    status: Option<hpc_compose::job::StatusSnapshot>,
    ps: Option<hpc_compose::job::PsSnapshot>,
    batch_log: Option<DebugLogTail>,
    service_logs: Vec<DebugLogTail>,
    notes: Vec<String>,
    recommendation: String,
    preflight: Option<serde_json::Value>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn debug(
    context: ResolvedContext,
    job_id: Option<String>,
    service: Option<String>,
    lines: usize,
    run_preflight_again: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    let mut notes = Vec::new();
    let mut preflight_json = None;
    let mut preflight_failed = false;

    let record = match resolve_tracked_record(&context, job_id.as_deref()) {
        Ok(record) => record,
        Err(err) => {
            notes.push(format!("{err:#}"));
            None
        }
    };
    let preflight_context = if run_preflight_again {
        match record.as_ref() {
            Some(record) if record.compose_file != context.compose_file.value => Some(
                resolve_context_for_tracked_compose(&context, record.compose_file.clone())?,
            ),
            Some(_) => Some(context.clone()),
            None => None,
        }
    } else {
        None
    };

    if run_preflight_again {
        let preflight_context = match preflight_context.as_ref() {
            Some(preflight_context) => preflight_context,
            None => &context,
        };
        let runtime_plan =
            load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
                &preflight_context.compose_file.value,
                &preflight_context.interpolation_vars,
                Some(&preflight_context.cache_dir.value),
                &preflight_context.resource_profiles,
            )?;
        let cluster_profile = load_discovered_cluster_profile(preflight_context)?;
        let report = run_preflight(
            &runtime_plan,
            &PreflightOptions {
                enroot_bin: preflight_context.binaries.enroot.value.clone(),
                apptainer_bin: preflight_context.binaries.apptainer.value.clone(),
                singularity_bin: preflight_context.binaries.singularity.value.clone(),
                sbatch_bin: preflight_context.binaries.sbatch.value.clone(),
                srun_bin: preflight_context.binaries.srun.value.clone(),
                scontrol_bin: preflight_context.binaries.scontrol.value.clone(),
                require_submit_tools: true,
                skip_prepare: false,
                cluster_profile,
            },
        );
        preflight_failed = report.has_errors();
        if output_format == OutputFormat::Text && (!quiet || report.has_errors()) {
            output::print_report(&report, true);
        }
        preflight_json = Some(
            serde_json::to_value(report.grouped())
                .context("failed to serialize preflight report")?,
        );
    }

    let Some(record) = record else {
        let report = DebugReport {
            tracked: false,
            compose_file: context.compose_file.value.clone(),
            job_id,
            summary: DebugSummary {
                scheduler_state: None,
                failed_service: None,
                exit_code: None,
                log_path: None,
                next_command: format!("hpc-compose up -f {}", context.compose_file.value.display()),
            },
            status: None,
            ps: None,
            batch_log: None,
            service_logs: Vec::new(),
            notes,
            recommendation: format!(
                "No tracked run was found. Run `hpc-compose plan -f {}` before `hpc-compose up -f {}`.",
                context.compose_file.value.display(),
                context.compose_file.value.display()
            ),
            preflight: preflight_json,
        };
        emit_debug_report(&report, output_format)?;
        if preflight_failed {
            bail!("preflight failed");
        }
        return Ok(());
    };
    let debug_compose_file = record.compose_file.clone();

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let status_snapshot = build_status_snapshot(
        &debug_compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )?;
    let ps_snapshot = build_ps_snapshot(
        &debug_compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )?;
    let batch_log = DebugLogTail {
        service_name: None,
        path: status_snapshot.batch_log.path.clone(),
        present: status_snapshot.batch_log.present,
        lines: tail_file_lines(&status_snapshot.batch_log.path, lines)?,
        note: (!status_snapshot.batch_log.present).then_some(
            "batch log is missing; the job may not have started or the scheduler wrote elsewhere"
                .to_string(),
        ),
    };
    let selected_services = if let Some(service_name) = service.as_deref() {
        let matching = status_snapshot
            .services
            .iter()
            .filter(|entry| entry.service_name == service_name)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            bail!(
                "service '{}' does not exist in tracked job {}",
                service_name,
                record.job_id
            );
        }
        matching
    } else {
        status_snapshot.services.iter().collect::<Vec<_>>()
    };
    let mut service_logs = Vec::with_capacity(selected_services.len());
    for service in selected_services {
        service_logs.push(DebugLogTail {
            service_name: Some(service.service_name.clone()),
            path: service.path.clone(),
            present: service.present,
            lines: tail_file_lines(&service.path, lines)?,
            note: (!service.present).then_some(
                "service log is missing; check the batch log for launch-time failures".to_string(),
            ),
        });
    }
    let recommendation = debug_recommendation(&debug_compose_file, &status_snapshot);
    let summary = build_debug_summary(
        &debug_compose_file,
        &record.job_id,
        &status_snapshot,
        &recommendation,
    );
    let report = DebugReport {
        tracked: true,
        compose_file: debug_compose_file,
        job_id: Some(record.job_id.clone()),
        summary,
        status: Some(status_snapshot),
        ps: Some(ps_snapshot),
        batch_log: Some(batch_log),
        service_logs,
        notes,
        recommendation,
        preflight: preflight_json,
    };
    emit_debug_report(&report, output_format)?;
    if preflight_failed {
        bail!("preflight failed");
    }
    Ok(())
}

fn resolve_context_for_tracked_compose(
    context: &ResolvedContext,
    compose_file: PathBuf,
) -> Result<ResolvedContext> {
    resolve(&ResolveRequest {
        cwd: context.cwd.clone(),
        profile: context.selected_profile.clone(),
        settings_file: context.settings_path.clone(),
        compose_file_override: Some(compose_file),
        binary_overrides: resolved_binary_overrides(context),
        huggingface_cli_bin: Some(context.huggingface_cli_bin.clone()),
    })
}

fn resolved_binary_overrides(context: &ResolvedContext) -> BinaryOverrides {
    BinaryOverrides {
        enroot: Some(context.binaries.enroot.value.clone()),
        apptainer: Some(context.binaries.apptainer.value.clone()),
        singularity: Some(context.binaries.singularity.value.clone()),
        salloc: Some(context.binaries.salloc.value.clone()),
        sbatch: Some(context.binaries.sbatch.value.clone()),
        srun: Some(context.binaries.srun.value.clone()),
        scontrol: Some(context.binaries.scontrol.value.clone()),
        sinfo: Some(context.binaries.sinfo.value.clone()),
        squeue: Some(context.binaries.squeue.value.clone()),
        sacct: Some(context.binaries.sacct.value.clone()),
        sstat: Some(context.binaries.sstat.value.clone()),
        scancel: Some(context.binaries.scancel.value.clone()),
        sshare: Some(context.binaries.sshare.value.clone()),
        sprio: Some(context.binaries.sprio.value.clone()),
        ssh: Some(context.binaries.ssh.value.clone()),
        rsync: Some(context.binaries.rsync.value.clone()),
    }
}

fn build_debug_summary(
    compose_file: &Path,
    job_id: &str,
    status: &hpc_compose::job::StatusSnapshot,
    recommendation: &str,
) -> DebugSummary {
    let failed = status.services.iter().find(|service| {
        service.status.as_deref() == Some("failed")
            || service.last_exit_code.is_some_and(|code| code != 0)
    });
    DebugSummary {
        scheduler_state: Some(status.scheduler.state.clone()),
        failed_service: failed.map(|service| service.service_name.clone()),
        exit_code: failed.and_then(|service| service.last_exit_code.map(i64::from)),
        log_path: failed.map(|service| service.path.clone()).or_else(|| {
            status
                .batch_log
                .present
                .then(|| status.batch_log.path.clone())
        }),
        next_command: if status.scheduler.failed {
            format!(
                "hpc-compose debug -f {} --job-id {} --preflight",
                compose_file.display(),
                job_id
            )
        } else {
            recommendation.to_string()
        },
    }
}

fn emit_debug_report(report: &DebugReport, output_format: OutputFormat) -> Result<()> {
    match output_format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(report).context("failed to serialize debug output")?
            );
        }
        OutputFormat::Text => {
            println!(
                "{}",
                hpc_compose::term::styled_label(
                    "compose file",
                    &report.compose_file.display().to_string()
                )
            );
            if !report.tracked {
                println!("tracked job: none");
                print_debug_summary(&report.summary);
                for note in &report.notes {
                    println!("note: {note}");
                }
                println!("recommendation: {}", report.recommendation);
                return Ok(());
            }
            print_debug_summary(&report.summary);
            if let Some(status) = report.status.as_ref() {
                output::print_status_snapshot(status)
                    .context("failed to write debug status output")?;
            }
            if let Some(ps) = report.ps.as_ref() {
                println!(
                    "{}",
                    hpc_compose::term::styled_section_header("Per-service state:")
                );
                output::print_ps_snapshot(ps).context("failed to write debug ps output")?;
            }
            if let Some(batch_log) = report.batch_log.as_ref() {
                print_debug_log_tail("Batch log", batch_log);
            }
            if !report.service_logs.is_empty() {
                println!(
                    "{}",
                    hpc_compose::term::styled_section_header("Service log tails:")
                );
                for log in &report.service_logs {
                    let label = log.service_name.as_deref().unwrap_or("service");
                    print_debug_log_tail(label, log);
                }
            }
            for note in &report.notes {
                println!("note: {note}");
            }
            println!("recommendation: {}", report.recommendation);
        }
    }
    Ok(())
}

fn print_debug_summary(summary: &DebugSummary) {
    println!(
        "{}",
        hpc_compose::term::styled_section_header("Debug summary:")
    );
    if let Some(state) = summary.scheduler_state.as_deref() {
        println!("  scheduler state: {state}");
    }
    if let Some(service) = summary.failed_service.as_deref() {
        println!("  failed service: {service}");
    }
    if let Some(exit_code) = summary.exit_code {
        println!("  exit code: {exit_code}");
    }
    if let Some(path) = summary.log_path.as_ref() {
        println!("  relevant log: {}", path.display());
    }
    println!("  next command: {}", summary.next_command);
}

fn print_debug_log_tail(label: &str, log: &DebugLogTail) {
    println!(
        "{} {} (present: {})",
        hpc_compose::term::styled_section_header(label),
        log.path.display(),
        if log.present { "yes" } else { "no" }
    );
    if let Some(note) = &log.note {
        println!("  note: {note}");
    }
    if log.lines.is_empty() {
        println!("  <no log lines>");
    } else {
        for line in &log.lines {
            println!("  {line}");
        }
    }
}

fn tail_file_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut collected = raw.lines().map(ToString::to_string).collect::<Vec<_>>();
    if collected.len() > lines {
        collected.drain(0..(collected.len() - lines));
    }
    Ok(collected)
}

fn debug_recommendation(
    compose_file: &Path,
    snapshot: &hpc_compose::job::StatusSnapshot,
) -> String {
    if snapshot.scheduler.failed {
        format!(
            "Run `hpc-compose debug -f {} --preflight` if preflight has not been rerun, then inspect the batch log above.",
            compose_file.display()
        )
    } else if snapshot.scheduler.terminal {
        format!(
            "The tracked job is terminal. Use `hpc-compose artifacts -f {}` if artifacts are configured.",
            compose_file.display()
        )
    } else {
        format!(
            "The tracked job is still active. Use `hpc-compose watch -f {}` or `hpc-compose logs -f {} --follow`.",
            compose_file.display(),
            compose_file.display()
        )
    }
}
