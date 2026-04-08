use super::*;

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
    options: &SchedulerOptions,
    lines: usize,
) -> Result<WatchOutcome> {
    let selected = selected_service_logs(record, None)?;
    let mut stdout = io::stdout();
    writeln!(stdout, "watching job {}...", record.job_id).ok();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    let mut cursors = build_cursors(&selected);
    let mut last_state: Option<(String, SchedulerSource)> = None;
    let mut last_visible_at: Option<u64> = None;

    loop {
        let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
        let raw_status = probe_scheduler_status(&record.job_id, options);
        let now = unix_timestamp_now();
        if raw_status.source != SchedulerSource::LocalOnly {
            last_visible_at = Some(now);
        }
        let status =
            reconcile_scheduler_status(raw_status, record.submitted_at, last_visible_at, now);
        let state_key = (status.state.clone(), status.source);
        if last_state.as_ref() != Some(&state_key) {
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
                writeln!(writer, "[{service}] {line}").context("failed to write log output")?;
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
            writeln!(writer, "[{}] {}", cursor.service_name, line)
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
