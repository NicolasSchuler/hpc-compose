use super::scheduler::{
    is_transitional_local_only, reconcile_scheduler_status, unix_timestamp_now,
};
use super::*;
use crate::term;

/// Final outcome returned by `watch_submission`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchOutcome {
    /// The job reached a successful terminal scheduler state.
    Completed(SchedulerStatus),
    /// The job reached a failed terminal scheduler state.
    Failed(SchedulerStatus),
    /// The tracker stopped with only local information available.
    Unknown(SchedulerStatus),
    /// The user detached from the watch UI before a terminal state.
    Interrupted(SchedulerStatus),
}

#[derive(Debug, Clone)]
pub(super) struct LogCursor {
    pub(super) service_name: String,
    pub(super) path: PathBuf,
    pub(super) offset: u64,
    pub(super) pending: String,
}

/// Prints tracked service logs, optionally following them until interrupted.
pub fn print_logs(
    record: &SubmissionRecord,
    service: Option<&str>,
    lines: usize,
    follow: bool,
) -> Result<()> {
    let selected = selected_service_logs(record, service)?;
    let mut stdout = io::stdout();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    if !follow {
        stdout.flush().context("failed to flush log output")?;
        return Ok(());
    }

    let mut cursors = build_cursors(&selected);
    loop {
        let emitted = drain_log_cursors(&mut cursors, &mut stdout)?;
        stdout.flush().context("failed to flush log output")?;
        if !emitted {
            thread::sleep(POLL_INTERVAL);
        }
    }
}

/// Streams tracked logs and scheduler state changes until the job finishes.
pub fn watch_submission(
    record: &SubmissionRecord,
    service: Option<&str>,
    options: &SchedulerOptions,
    lines: usize,
) -> Result<WatchOutcome> {
    if record.backend == SubmissionBackend::Local {
        return watch_local_submission(record, service, options, lines);
    }

    let selected = selected_service_logs(record, service)?;
    let mut stdout = io::stdout();
    writeln!(stdout, "watching job {}...", record.job_id).ok();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    let mut cursors = build_cursors(&selected);
    let mut last_state: Option<(String, SchedulerSource)> = None;
    let mut last_visible_at: Option<u64> = None;
    let mut last_progress_minute: Option<u64> = None;

    loop {
        let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
        let (raw_status, queue_diagnostics) =
            probe_scheduler_status_with_queue_diagnostics(&record.job_id, options);
        let now = unix_timestamp_now();
        if raw_status.source != SchedulerSource::LocalOnly {
            last_visible_at = Some(now);
        }
        let status =
            reconcile_scheduler_status(raw_status, record.submitted_at, last_visible_at, now);
        let state_key = (status.state.clone(), status.source);
        let state_changed = last_state.as_ref() != Some(&state_key);
        if state_changed {
            writeln!(
                stdout,
                "scheduler state: {} ({})",
                status.state,
                scheduler_source_label(status.source)
            )
            .ok();
            if let Some(detail) = &status.detail {
                writeln!(stdout, "note: {detail}").ok();
            }
            stdout.flush().ok();
            last_state = Some(state_key);
        }
        if let Some(progress) = walltime_progress(record, &status, queue_diagnostics.as_ref(), now)
        {
            let minute = progress.elapsed_seconds / 60;
            if state_changed || last_progress_minute != Some(minute) {
                writeln!(
                    stdout,
                    "walltime: {}% {}",
                    walltime_progress_percent(&progress),
                    format_walltime_summary(&progress)
                )
                .ok();
                stdout.flush().ok();
                last_progress_minute = Some(minute);
            }
        } else if state_changed {
            last_progress_minute = None;
        }

        match status.source {
            SchedulerSource::LocalOnly if is_transitional_local_only(&status) => {
                thread::sleep(POLL_INTERVAL);
            }
            SchedulerSource::LocalOnly => return Ok(WatchOutcome::Unknown(status)),
            _ if status.terminal && status.failed => {
                let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
                stdout.flush().ok();
                return Ok(WatchOutcome::Failed(status));
            }
            _ if status.terminal => {
                let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
                stdout.flush().ok();
                return Ok(WatchOutcome::Completed(status));
            }
            _ => thread::sleep(POLL_INTERVAL),
        }
    }
}

fn watch_local_submission(
    record: &SubmissionRecord,
    service: Option<&str>,
    options: &SchedulerOptions,
    lines: usize,
) -> Result<WatchOutcome> {
    let selected = selected_service_logs(record, service)?;
    let mut stdout = io::stdout();
    writeln!(stdout, "watching job {}...", record.job_id).ok();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    let mut cursors = build_cursors(&selected);
    let mut last_state: Option<String> = None;
    let mut last_progress_minute: Option<u64> = None;

    loop {
        let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
        let snapshot = build_status_snapshot(&record.compose_file, Some(&record.job_id), options)?;
        let state_changed = last_state.as_ref() != Some(&snapshot.scheduler.state);
        if state_changed {
            writeln!(
                stdout,
                "scheduler state: {} ({})",
                snapshot.scheduler.state,
                scheduler_source_label(snapshot.scheduler.source)
            )
            .ok();
            if let Some(detail) = &snapshot.scheduler.detail {
                writeln!(stdout, "note: {detail}").ok();
            }
            stdout.flush().ok();
            last_state = Some(snapshot.scheduler.state.clone());
        }
        if let Some(progress) = walltime_progress(
            &snapshot.record,
            &snapshot.scheduler,
            snapshot.queue_diagnostics.as_ref(),
            unix_timestamp_now(),
        ) {
            let minute = progress.elapsed_seconds / 60;
            if state_changed || last_progress_minute != Some(minute) {
                writeln!(
                    stdout,
                    "walltime: {}% {}",
                    walltime_progress_percent(&progress),
                    format_walltime_summary(&progress)
                )
                .ok();
                stdout.flush().ok();
                last_progress_minute = Some(minute);
            }
        } else if state_changed {
            last_progress_minute = None;
        }

        if snapshot.scheduler.terminal {
            let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
            stdout.flush().ok();
            return Ok(if snapshot.scheduler.failed {
                WatchOutcome::Failed(snapshot.scheduler)
            } else {
                WatchOutcome::Completed(snapshot.scheduler)
            });
        }

        thread::sleep(POLL_INTERVAL);
    }
}

pub(crate) fn selected_service_logs(
    record: &SubmissionRecord,
    service: Option<&str>,
) -> Result<Vec<(String, PathBuf)>> {
    if let Some(service) = service {
        let path = record.service_logs.get(service).cloned().context(format!(
            "service '{}' does not exist in tracked job {}",
            service, record.job_id
        ))?;
        return Ok(vec![(service.to_string(), path)]);
    }
    let mut selected = Vec::with_capacity(record.service_logs.len());
    for (name, path) in &record.service_logs {
        selected.push((name.clone(), path.clone()));
    }
    Ok(selected)
}

fn emit_initial_tail(
    selected: &[(String, PathBuf)],
    lines: usize,
    writer: &mut impl Write,
) -> Result<()> {
    let tailed = selected
        .iter()
        .map(|(service, path)| Ok((service.clone(), tail_lines(path, lines)?)))
        .collect::<Result<Vec<_>>>()?;
    let max_len = tailed
        .iter()
        .map(|(_, lines)| lines.len())
        .max()
        .unwrap_or(0);
    for index in 0..max_len {
        for (service, lines) in &tailed {
            if let Some(line) = lines.get(index) {
                writeln!(
                    writer,
                    "{} {line}",
                    term::styled_service_log_prefix(service)
                )
                .context("failed to write log output")?;
            }
        }
    }
    Ok(())
}

fn build_cursors(selected: &[(String, PathBuf)]) -> Vec<LogCursor> {
    let mut cursors = Vec::with_capacity(selected.len());
    for (service_name, path) in selected {
        let offset = match fs::metadata(path) {
            Ok(meta) => meta.len(),
            Err(_) => 0,
        };
        cursors.push(LogCursor {
            service_name: service_name.clone(),
            offset,
            path: path.clone(),
            pending: String::new(),
        });
    }
    cursors
}

fn drain_log_cursors(cursors: &mut [LogCursor], writer: &mut impl Write) -> Result<bool> {
    let mut emitted = false;
    for cursor in cursors {
        for line in read_new_lines(cursor)? {
            writeln!(
                writer,
                "{} {line}",
                term::styled_service_log_prefix(&cursor.service_name)
            )
            .context("failed to write log output")?;
            emitted = true;
        }
    }
    Ok(emitted)
}

pub(crate) fn read_new_lines(cursor: &mut LogCursor) -> Result<Vec<String>> {
    let Ok(mut file) = File::open(&cursor.path) else {
        return Ok(Vec::new());
    };
    let len = file
        .metadata()
        .context(format!(
            "failed to read metadata for {}",
            cursor.path.display()
        ))?
        .len();
    if cursor.offset > len {
        cursor.offset = 0;
        cursor.pending.clear();
    }
    if cursor.offset == len {
        return Ok(Vec::new());
    }

    file.seek(SeekFrom::Start(cursor.offset))
        .context(format!("failed to seek {}", cursor.path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .context(format!("failed to read {}", cursor.path.display()))?;
    cursor.offset = len;

    let mut combined = std::mem::take(&mut cursor.pending);
    combined.push_str(&String::from_utf8_lossy(&bytes));
    let mut lines = Vec::new();

    if combined.is_empty() {
        return Ok(lines);
    }

    let ends_with_newline = combined.ends_with('\n');
    for segment in combined.split_inclusive('\n') {
        lines.push(
            segment
                .trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string(),
        );
    }

    if !ends_with_newline {
        cursor.pending = lines.pop().unwrap_or_default();
    }

    Ok(lines)
}

pub(crate) fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut collected = raw.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    if collected.len() > lines {
        collected.drain(0..(collected.len() - lines));
    }
    Ok(collected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_helpers_cover_selection_cursors_and_local_watch() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
        let runtime_plan = crate::output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("job.local.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-logs-123",
            SubmissionBackend::Local,
        )
        .expect("record");
        write_submission_record(&record).expect("write record");

        let api_log = record.service_logs.get("api").expect("api log");
        if let Some(parent) = api_log.parent() {
            fs::create_dir_all(parent).expect("log dir");
        }
        fs::write(api_log, "boot\nready\npartial").expect("api log");

        let selected = selected_service_logs(&record, Some("api")).expect("selected");
        assert_eq!(selected.len(), 1);
        assert!(
            selected_service_logs(&record, Some("missing"))
                .expect_err("missing service")
                .to_string()
                .contains("does not exist")
        );

        let mut rendered = Vec::new();
        emit_initial_tail(&selected, 2, &mut rendered).expect("emit initial");
        let rendered = String::from_utf8(rendered).expect("utf8");
        assert!(rendered.contains("api") && rendered.contains("ready"));

        let mut cursors = build_cursors(&selected);
        fs::write(api_log, "boot\nready\npartial\nnext\n").expect("append api log");
        let mut followed = Vec::new();
        assert!(drain_log_cursors(&mut cursors, &mut followed).expect("drain"));
        let followed = String::from_utf8(followed).expect("utf8");
        assert!(followed.contains("api") && followed.contains("next"));

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

        let outcome = watch_submission(
            &record,
            Some("api"),
            &SchedulerOptions {
                squeue_bin: "/definitely/missing-squeue".into(),
                sacct_bin: "/definitely/missing-sacct".into(),
            },
            1,
        )
        .expect("watch local");
        assert!(matches!(outcome, WatchOutcome::Completed(_)));
    }
}
