use super::*;

/// Prints the attempt/requeue history for a tracked job from LOCAL state only.
///
/// Resolves the tracked record (latest for the active compose file, or an
/// explicit `--job-id`), reads the per-attempt `state.json` files (or the single
/// latest `state.json` fallback), and prints a text summary or one JSON object.
/// Contacts no scheduler and reads nothing from the cluster filesystem.
pub(crate) fn checkpoints(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let history = hpc_compose::job::collect_checkpoint_history(&record);

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&history)
                    .context("failed to serialize checkpoints output")?
            );
            Ok(())
        }
        OutputFormat::Text => print_checkpoint_history(&history),
    }
}

fn print_checkpoint_history(history: &hpc_compose::job::CheckpointHistory) -> Result<()> {
    let mut stdout = io::stdout();
    writeln!(
        stdout,
        "hpc-compose checkpoints | job {} | {}",
        history.job_id,
        history.compose_file.display()
    )
    .context("failed to write checkpoints output")?;
    writeln!(
        stdout,
        "resume configured: {}",
        if history.resume_configured {
            "yes"
        } else {
            "no"
        }
    )?;
    writeln!(
        stdout,
        "attempts: {} | requeues: {}",
        history.attempts, history.requeues
    )?;
    if let Some(current) = history.current_attempt {
        writeln!(stdout, "current attempt: {current}")?;
    }
    if let Some(is_resume) = history.is_resume {
        writeln!(stdout, "is resume: {is_resume}")?;
    }
    if let Some(resume_dir) = &history.resume_dir {
        writeln!(stdout, "resume dir: {}", resume_dir.display())?;
    }

    if history.entries.is_empty() {
        writeln!(stdout, "no attempts found")?;
    } else {
        writeln!(
            stdout,
            "attempt  status      started     finished    duration  exit"
        )?;
        for entry in &history.entries {
            writeln!(
                stdout,
                "{:<7}  {:<10}  {:<10}  {:<10}  {:<8}  {}",
                entry.attempt,
                entry.job_status.as_deref().unwrap_or("-"),
                optional_u64(entry.started_at),
                optional_u64(entry.finished_at),
                optional_u64(entry.duration_seconds),
                optional_i32(entry.job_exit_code),
            )?;
        }
    }

    if !history.degraded.is_empty() {
        writeln!(stdout, "notes:")?;
        for note in &history.degraded {
            writeln!(stdout, "  - {note}")?;
        }
    }

    stdout.flush().context("failed to flush checkpoints output")
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn optional_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
