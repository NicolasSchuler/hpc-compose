use super::scheduler::{
    is_transitional_local_only, reconcile_scheduler_status, unix_timestamp_now,
};
use super::*;
use crate::term;
use crate::time_util::system_time_to_unix;
use regex::Regex;

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

/// Options for printing tracked service logs.
#[derive(Debug, Clone, Default)]
pub struct LogPrintOptions {
    /// Optional service to select from the tracked service log map.
    pub service: Option<String>,
    /// Number of trailing lines to print before follow mode begins.
    pub lines: usize,
    /// Continue printing appended log output until interrupted.
    pub follow: bool,
    /// Optional Rust regex pattern applied to raw log lines.
    pub grep: Option<String>,
    /// Optional coarse lower bound based on the log file modification time.
    pub since_seconds: Option<u64>,
}

/// Polls scheduler state until a submitted Slurm job is ready for the normal watch view.
pub fn wait_for_job_start(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    pending_warn_after_seconds: Option<u64>,
) -> Result<SchedulerStatus> {
    let mut stdout = io::stdout();
    writeln!(stdout, "waiting for job {} to start...", record.job_id)
        .context("failed to write queue wait output")?;
    stdout
        .flush()
        .context("failed to flush queue wait output")?;

    let mut last_state: Option<(String, SchedulerSource)> = None;
    let mut last_visible_at: Option<u64> = None;
    let mut pending_warning_emitted = false;

    loop {
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
                "queue state: {} ({})",
                status.state,
                scheduler_source_label(status.source)
            )
            .context("failed to write queue wait state")?;
            if let Some(detail) = &status.detail {
                writeln!(stdout, "note: {detail}").context("failed to write queue wait detail")?;
            }
            write_queue_diagnostics(&mut stdout, queue_diagnostics.as_ref())?;
            stdout
                .flush()
                .context("failed to flush queue wait output")?;
            last_state = Some(state_key);
        }

        if JobState::parse(&status.state) == JobState::Pending
            && !pending_warning_emitted
            && pending_warn_after_seconds.is_some_and(|seconds| {
                seconds > 0 && now.saturating_sub(record.submitted_at) >= seconds
            })
        {
            warn_pending_queue_wait(
                record,
                pending_warn_after_seconds,
                queue_diagnostics.as_ref(),
            );
            pending_warning_emitted = true;
        }

        if JobState::parse(&status.state) == JobState::Running || status.terminal {
            return Ok(status);
        }
        match status.source {
            SchedulerSource::LocalOnly if is_transitional_local_only(&status) => {
                thread::sleep(POLL_INTERVAL);
            }
            SchedulerSource::LocalOnly => return Ok(status),
            _ => thread::sleep(POLL_INTERVAL),
        }
    }
}

/// Prints tracked service logs, optionally following them until interrupted.
pub fn print_logs(record: &SubmissionRecord, options: &LogPrintOptions) -> Result<()> {
    let grep = match options.grep.as_deref() {
        Some(pattern) => Some(
            Regex::new(pattern).with_context(|| format!("invalid --grep pattern '{pattern}'"))?,
        ),
        None => None,
    };
    let since_cutoff = options
        .since_seconds
        .map(|seconds| unix_timestamp_now().saturating_sub(seconds));
    let selected = selected_service_logs(record, options.service.as_deref())?;
    let mut stdout = io::stdout();
    emit_initial_tail_filtered(
        &selected,
        options.lines,
        grep.as_ref(),
        since_cutoff,
        &mut stdout,
    )?;
    if !options.follow {
        stdout.flush().context("failed to flush log output")?;
        return Ok(());
    }

    let mut cursors = build_cursors(&selected);
    loop {
        let emitted = drain_log_cursors_filtered(&mut cursors, grep.as_ref(), &mut stdout)?;
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

fn write_queue_diagnostics(
    writer: &mut impl Write,
    diagnostics: Option<&QueueDiagnostics>,
) -> Result<()> {
    let Some(diagnostics) = diagnostics else {
        return Ok(());
    };
    if let Some(reason) = &diagnostics.pending_reason {
        writeln!(writer, "  pending reason: {reason}")
            .context("failed to write queue pending reason")?;
    }
    if let Some(eligible_time) = &diagnostics.eligible_time {
        writeln!(writer, "  eligible time: {eligible_time}")
            .context("failed to write queue eligible time")?;
    }
    if let Some(start_time) = &diagnostics.start_time {
        writeln!(writer, "  start time: {start_time}")
            .context("failed to write queue start time")?;
    }
    Ok(())
}

fn warn_pending_queue_wait(
    record: &SubmissionRecord,
    pending_warn_after_seconds: Option<u64>,
    diagnostics: Option<&QueueDiagnostics>,
) {
    let Some(seconds) = pending_warn_after_seconds else {
        return;
    };
    if seconds == 0 {
        return;
    }
    let mut detail = format!(
        "job {} still PENDING after {}",
        record.job_id,
        format_walltime_duration(seconds)
    );
    if let Some(diagnostics) = diagnostics {
        let mut parts = Vec::new();
        if let Some(reason) = &diagnostics.pending_reason {
            parts.push(format!("pending reason: {reason}"));
        }
        if let Some(eligible_time) = &diagnostics.eligible_time {
            parts.push(format!("eligible time: {eligible_time}"));
        }
        if let Some(start_time) = &diagnostics.start_time {
            parts.push(format!("start time: {start_time}"));
        }
        if !parts.is_empty() {
            detail.push_str("; ");
            detail.push_str(&parts.join("; "));
        }
    }
    crate::diagnostics::warn(detail);
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
        let path = record.service_logs.get(service).cloned().with_context(|| {
            let available: Vec<&str> = record.service_logs.keys().map(String::as_str).collect();
            let mut message = format!(
                "service '{service}' does not exist in tracked job {}",
                record.job_id
            );
            if let Some(suggestion) = crate::suggest::nearest_default(service, &available) {
                message.push_str(&format!("; did you mean '{suggestion}'?"));
            }
            if !available.is_empty() {
                message.push_str(&format!(" (available: {})", available.join(", ")));
            }
            message
        })?;
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
    emit_initial_tail_filtered(selected, lines, None, None, writer)
}

fn emit_initial_tail_filtered(
    selected: &[(String, PathBuf)],
    lines: usize,
    grep: Option<&Regex>,
    since_cutoff: Option<u64>,
    writer: &mut impl Write,
) -> Result<()> {
    let tailed = selected
        .iter()
        .map(|(service, path)| {
            let lines = if log_file_is_recent_enough(path, since_cutoff) {
                tail_lines(path, lines)?
                    .into_iter()
                    .filter(|line| grep.is_none_or(|grep| grep.is_match(line)))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            Ok((service.clone(), lines))
        })
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

fn log_file_is_recent_enough(path: &Path, since_cutoff: Option<u64>) -> bool {
    let Some(since_cutoff) = since_cutoff else {
        return true;
    };
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(system_time_to_unix)
        .is_some_and(|updated_at| updated_at >= since_cutoff)
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
    drain_log_cursors_filtered(cursors, None, writer)
}

fn drain_log_cursors_filtered(
    cursors: &mut [LogCursor],
    grep: Option<&Regex>,
    writer: &mut impl Write,
) -> Result<bool> {
    let mut emitted = false;
    for cursor in cursors {
        for line in read_new_lines(cursor)? {
            if grep.is_some_and(|grep| !grep.is_match(&line)) {
                continue;
            }
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
    if lines == 0 {
        return Ok(Vec::new());
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {}", path.display()));
        }
    };
    let file_len = file
        .metadata()
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .len();
    if file_len == 0 {
        return Ok(Vec::new());
    }

    const TAIL_CHUNK_SIZE: u64 = 16 * 1024;
    let mut position = file_len;
    let mut newline_count = 0usize;
    let mut chunks = Vec::new();
    while position > 0 && newline_count <= lines {
        let read_len = position.min(TAIL_CHUNK_SIZE) as usize;
        position -= read_len as u64;
        let mut chunk = vec![0_u8; read_len];
        file.seek(SeekFrom::Start(position))
            .with_context(|| format!("failed to seek {}", path.display()))?;
        file.read_exact(&mut chunk)
            .with_context(|| format!("failed to read {}", path.display()))?;
        newline_count += chunk.iter().filter(|byte| **byte == b'\n').count();
        chunks.push(chunk);
    }

    let total_len = chunks.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks.iter().rev() {
        bytes.extend_from_slice(chunk);
    }
    let raw = String::from_utf8_lossy(&bytes);
    let mut collected = raw.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    if collected.len() > lines {
        collected.drain(0..(collected.len() - lines));
    }
    Ok(collected)
}

/// Parses a compact duration string accepted by `logs --since`.
pub fn parse_log_since_duration(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("--since must not be empty");
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        if seconds == 0 {
            bail!("--since must be greater than zero");
        }
        return Ok(seconds);
    }

    let mut total = 0_u64;
    let mut digits = String::new();
    let mut consumed_unit = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        let value = digits
            .parse::<u64>()
            .with_context(|| format!("unsupported --since duration '{trimmed}'"))?;
        if value == 0 {
            bail!("--since duration segments must be greater than zero");
        }
        let multiplier = match ch {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            _ => {
                bail!("unsupported --since duration unit '{ch}' in '{trimmed}'; use s, m, h, or d")
            }
        };
        total = total.saturating_add(value.saturating_mul(multiplier));
        digits.clear();
        consumed_unit = true;
    }
    if !digits.is_empty() || !consumed_unit || total == 0 {
        bail!("unsupported --since duration '{trimmed}'; use values like 30s, 15m, or 1h30m");
    }
    Ok(total)
}

/// Parses the compact duration accepted by `up --queue-warn-after`.
pub fn parse_queue_warn_after_duration(raw: &str) -> Result<Option<u64>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("--queue-warn-after must not be empty");
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Ok((seconds > 0).then_some(seconds));
    }

    let mut total = 0_u64;
    let mut digits = String::new();
    let mut consumed_unit = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        if digits.is_empty() {
            bail!("unsupported --queue-warn-after duration '{trimmed}'");
        }
        let value = digits
            .parse::<u64>()
            .with_context(|| format!("unsupported --queue-warn-after duration '{trimmed}'"))?;
        let multiplier = match ch {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            _ => {
                bail!(
                    "unsupported --queue-warn-after duration unit '{ch}' in '{trimmed}'; use s, m, h, or d"
                )
            }
        };
        total = total.saturating_add(value.saturating_mul(multiplier));
        digits.clear();
        consumed_unit = true;
    }
    if !digits.is_empty() || !consumed_unit {
        bail!(
            "unsupported --queue-warn-after duration '{trimmed}'; use values like 30s, 15m, 1h30m, or 0"
        );
    }
    Ok((total > 0).then_some(total))
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
        let runtime_plan =
            crate::commands::load::load_runtime_plan(&compose).expect("runtime plan");
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

    #[test]
    fn tail_lines_handles_missing_zero_and_short_logs() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("missing.log");
        assert!(tail_lines(&missing, 10).expect("missing log").is_empty());

        let log = tmpdir.path().join("app.log");
        fs::write(&log, "one\ntwo\n").expect("log");
        assert!(tail_lines(&log, 0).expect("zero lines").is_empty());
        assert_eq!(
            tail_lines(&log, 10).expect("short log"),
            vec!["one".to_string(), "two".to_string()]
        );
        assert_eq!(
            tail_lines(&log, 1).expect("last line"),
            vec!["two".to_string()]
        );
    }

    #[test]
    fn tail_lines_reads_only_needed_suffix_and_decodes_lossily() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("large.log");
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
    fn parse_log_since_duration_accepts_compact_units() {
        assert_eq!(parse_log_since_duration("30s").expect("30s"), 30);
        assert_eq!(parse_log_since_duration("15m").expect("15m"), 900);
        assert_eq!(parse_log_since_duration("2h").expect("2h"), 7_200);
        assert_eq!(parse_log_since_duration("1d").expect("1d"), 86_400);
        assert_eq!(parse_log_since_duration("1h30m").expect("compound"), 5_400);
        assert_eq!(parse_log_since_duration("42").expect("seconds"), 42);
        assert!(parse_log_since_duration("").is_err());
        assert!(parse_log_since_duration("0").is_err());
        assert!(parse_log_since_duration("7q").is_err());
        assert!(parse_log_since_duration("7m30").is_err());
    }

    #[test]
    fn parse_queue_warn_after_duration_accepts_compact_units_and_zero() {
        assert_eq!(
            parse_queue_warn_after_duration("30s").expect("30s"),
            Some(30)
        );
        assert_eq!(
            parse_queue_warn_after_duration("15m").expect("15m"),
            Some(900)
        );
        assert_eq!(
            parse_queue_warn_after_duration("1h30m").expect("compound"),
            Some(5_400)
        );
        assert_eq!(parse_queue_warn_after_duration("0").expect("0"), None);
        assert_eq!(parse_queue_warn_after_duration("0s").expect("0s"), None);
        assert!(parse_queue_warn_after_duration("").is_err());
        assert!(parse_queue_warn_after_duration("7q").is_err());
        assert!(parse_queue_warn_after_duration("7m30").is_err());
    }

    #[test]
    fn read_new_lines_buffers_partials_crlf_and_truncation() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log = tmpdir.path().join("api.log");
        fs::write(&log, "one\r\ntwo").expect("initial log");
        let mut cursor = LogCursor {
            service_name: "api".into(),
            path: log.clone(),
            offset: 0,
            pending: String::new(),
        };

        assert_eq!(
            read_new_lines(&mut cursor).expect("first read"),
            vec!["one".to_string()]
        );
        assert_eq!(cursor.pending, "two");

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .expect("open append")
            .write_all(b"\r\nthree\n")
            .expect("append");
        assert_eq!(
            read_new_lines(&mut cursor).expect("second read"),
            vec!["two".to_string(), "three".to_string()]
        );
        assert!(cursor.pending.is_empty());

        fs::write(&log, "reset\n").expect("truncate log");
        assert_eq!(
            read_new_lines(&mut cursor).expect("truncated read"),
            vec!["reset".to_string()]
        );

        let mut missing = LogCursor {
            service_name: "api".into(),
            path: tmpdir.path().join("gone.log"),
            offset: 42,
            pending: "partial".into(),
        };
        assert!(
            read_new_lines(&mut missing)
                .expect("missing read")
                .is_empty()
        );
        assert_eq!(missing.pending, "partial");
    }

    #[test]
    fn emit_initial_tail_interleaves_multiple_services_with_unequal_lengths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let api = tmpdir.path().join("api.log");
        let worker = tmpdir.path().join("worker.log");
        fs::write(&api, "api-one\napi-two\n").expect("api log");
        fs::write(&worker, "worker-only\n").expect("worker log");
        let selected = vec![("api".to_string(), api), ("worker".to_string(), worker)];

        let mut rendered = Vec::new();
        emit_initial_tail(&selected, 2, &mut rendered).expect("emit");
        let rendered = String::from_utf8(rendered).expect("utf8");

        assert!(rendered.contains("api-one"));
        assert!(rendered.contains("api-two"));
        assert!(rendered.contains("worker-only"));
        assert!(rendered.find("api-one") < rendered.find("api-two"));
    }
}
