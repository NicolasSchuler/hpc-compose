//! `hpc-compose experiment show` — read-only "one JSON object per run"
//! aggregator over already-persisted tracked state — plus `experiment tag` /
//! `experiment note`, which annotate the tracked record.
//!
//! `show` is static-safe: it contacts a scheduler only as much as `status`
//! (squeue plus a terminal-only sacct probe via [`build_status_snapshot`]) and
//! `score` (the same plus the terminal-only sstat efficiency probe via
//! [`build_efficiency_score_report`]) already do. It never submits, cancels,
//! exports, writes a file, or opens a connection. SSH/ControlMaster guidance and
//! per-service tunnel hints are PRINTED strings only.
//!
//! `tag` and `note` contact no scheduler either; they rewrite only the tracked
//! record file (and its latest-pointer duplicate when that pointer already
//! names the job) via [`update_submission_record`].

use hpc_compose::job::{
    ArtifactManifest, CheckpointHistory, EfficiencyScoreReport, JobDiffChange, JobNote,
    JobProvenance, SpecDiffReport, StatusSnapshot, append_job_note, apply_tag_changes,
    artifact_manifest_path_for_record, collect_checkpoint_history, hash_file,
    metrics_dir_for_record, update_submission_record, write_tree_tarball,
};

use super::*;

/// One JSON object aggregating the read-only state of a single tracked run.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentShowOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    name: String,
    state: String,
    services: Vec<ExperimentService>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<JobProvenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<ArtifactManifest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    efficiency: Option<EfficiencyScoreReport>,
    /// User-assigned labels on the tracked record (see `experiment tag`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    /// Append-only timestamped observations (see `experiment note`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    notes: Vec<JobNote>,
    next_commands: Vec<String>,
}

/// `experiment tag` result (`--format json`): the record's full tag set after
/// the change.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentTagOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    tags: Vec<String>,
}

/// `experiment note` result (`--format json`): the record's full note list
/// after the append.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentNoteOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    notes: Vec<JobNote>,
}

/// `experiment bundle` result (`--format json`): the emitted archive path, its
/// layout, the per-file sha256 ledger, and the `missing[]` ingredient ledger.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentBundleOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    /// The archive path (tarball) or bundle directory that was written.
    output: String,
    /// `"tarball"` or `"directory"`.
    layout: String,
    /// Every staged file with its sha256 (relative to the bundle root).
    files: Vec<ExperimentBundleFile>,
    /// Ingredients that could not be included, each with a human reason.
    missing: Vec<ExperimentBundleMissing>,
    /// `Some(true)` when the current spec drifted from the recorded snapshot,
    /// `Some(false)` when it matched, `None` when drift could not be checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    drift_detected: Option<bool>,
    /// Whether `--strict` was requested (the command fails when any ingredient
    /// is missing under strict mode).
    strict: bool,
}

/// One staged bundle file with its content hash.
#[derive(Debug, Serialize, schemars::JsonSchema)]
struct ExperimentBundleFile {
    /// Path relative to the bundle root (e.g. `spec/compose.yaml`).
    path: String,
    /// Lowercase hex SHA-256 of the file's bytes.
    sha256: String,
}

/// One ingredient that was requested but could not be included in the bundle.
#[derive(Debug, Serialize, schemars::JsonSchema)]
struct ExperimentBundleMissing {
    /// The bundle-relative path or logical item that is absent.
    item: String,
    /// A human-readable explanation of why it is absent.
    reason: String,
}

/// Per-service slice of the aggregate: tracked placement plus a printable tunnel
/// hint when the service exposes a TCP/HTTP readiness port.
#[derive(Debug, Serialize, PartialEq, Eq, schemars::JsonSchema)]
struct ExperimentService {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    nodelist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tunnel_hint: Option<String>,
}

pub(crate) fn experiment_show(
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

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    // Same scheduler contact as `status`: squeue + terminal-only sacct.
    let snapshot = build_status_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )
    .context("failed to inspect tracked scheduler state")?;

    // Best-effort efficiency: identical probe to `score`, but a non-terminal job
    // (or a local run) degrades to `None` rather than failing the aggregate.
    let efficiency = build_efficiency_score_report(
        &runtime_plan,
        &record,
        &EfficiencyScoreOptions {
            scheduler: scheduler_options,
            sstat_bin: context.binaries.sstat.value.clone(),
            pue,
            gpu_tdp_w,
            cpu_watts_per_core,
        },
    )
    .ok();

    // Pure, side-effect-free manifest read (never export_artifacts).
    let results = read_artifact_manifest(&record);

    let login_host = context.login_host.clone();
    let output = build_experiment_show_output(
        &record,
        &runtime_plan,
        &snapshot,
        results,
        efficiency,
        login_host.as_deref(),
    );

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment show output")?
            );
        }
        OutputFormat::Text => print_experiment_show_output(&output),
    }
    Ok(())
}

/// Pure read of the persisted artifact manifest for a record. Returns `None`
/// when the manifest is absent or unreadable; never copies, exports, or writes.
fn read_artifact_manifest(record: &SubmissionRecord) -> Option<ArtifactManifest> {
    let manifest_path = artifact_manifest_path_for_record(record);
    let contents = fs::read_to_string(&manifest_path).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Pure assembly of the aggregate from already-fetched reports. Keeps the
/// field-mapping and tunnel-hint logic unit-testable without a scheduler.
fn build_experiment_show_output(
    record: &SubmissionRecord,
    plan: &RuntimePlan,
    snapshot: &StatusSnapshot,
    results: Option<ArtifactManifest>,
    efficiency: Option<EfficiencyScoreReport>,
    login_host: Option<&str>,
) -> ExperimentShowOutput {
    let job_id = record.job_id.as_str();
    // Readiness-derived endpoints (TCP/HTTP only), keyed by service name. The
    // port + placeholder handling mirrors `reach`/`build_submit_endpoints`.
    let endpoints = output::build_submit_endpoints(plan);

    let services = plan
        .ordered_services
        .iter()
        .map(|service| {
            let row = snapshot
                .services
                .iter()
                .find(|row| row.service_name == service.name);
            let nodelist = row.and_then(|row| row.nodelist.clone());
            let status = row.and_then(|row| row.status.clone());
            let tunnel_hint = endpoints
                .iter()
                .find(|endpoint| endpoint.service == service.name)
                .map(|endpoint| {
                    let compute = nodelist
                        .as_deref()
                        .and_then(|nodes| nodes.split(',').next())
                        .unwrap_or("<compute-node>");
                    let login = login_host.unwrap_or("<login-node>");
                    ssh_forward_command(endpoint.port, endpoint.port, compute, login)
                });
            ExperimentService {
                name: service.name.clone(),
                nodelist,
                status,
                tunnel_hint,
            }
        })
        .collect();

    ExperimentShowOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: job_id.to_string(),
        name: plan.name.clone(),
        state: snapshot.scheduler.state.clone(),
        services,
        provenance: record.provenance.clone(),
        results,
        efficiency,
        tags: record.tags.clone(),
        notes: record.notes.clone(),
        next_commands: experiment_next_commands(job_id, output::artifact_export_configured(plan)),
    }
}

/// `experiment tag`: add and/or remove labels on one tracked record. The
/// record file (and, when it names this job, the latest-pointer duplicate) is
/// the only thing rewritten; no scheduler is contacted.
pub(crate) fn experiment_tag(
    context: ResolvedContext,
    tags: Vec<String>,
    remove: Vec<String>,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    if tags.is_empty() && remove.is_empty() {
        bail!("pass at least one tag to add, or --remove <TAG> to remove one");
    }
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let updated = update_submission_record(&record.compose_file, &record.job_id, |record| {
        apply_tag_changes(&mut record.tags, &tags, &remove)
    })?;
    let output = ExperimentTagOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: updated.job_id.clone(),
        tags: updated.tags.clone(),
    };
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment tag output")?
            );
        }
        OutputFormat::Text => {
            let tags = if output.tags.is_empty() {
                "(none)".to_string()
            } else {
                output.tags.join(", ")
            };
            println!("tags for job {}: {tags}", output.job_id);
        }
    }
    Ok(())
}

/// `experiment note`: append one timestamped observation to one tracked
/// record. Same write scope as `experiment tag`; no scheduler is contacted.
pub(crate) fn experiment_note(
    context: ResolvedContext,
    text: String,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let updated = update_submission_record(&record.compose_file, &record.job_id, |record| {
        append_job_note(record, &text)
    })?;
    let output = ExperimentNoteOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: updated.job_id.clone(),
        notes: updated.notes.clone(),
    };
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment note output")?
            );
        }
        OutputFormat::Text => {
            println!(
                "note added to job {} ({} note{})",
                output.job_id,
                output.notes.len(),
                if output.notes.len() == 1 { "" } else { "s" }
            );
        }
    }
    Ok(())
}

/// Suggested follow-up reads. References only shipped commands and carries the
/// ControlMaster/ControlPath/ControlPersist multiplexing note so a 2FA/OTP user
/// authenticates once. Never opens a connection.
fn experiment_next_commands(job_id: &str, export_dir_configured: bool) -> Vec<String> {
    let mut commands = vec![
        format!("hpc-compose status --job-id {job_id}"),
        format!("hpc-compose score --job-id {job_id}"),
    ];
    // `artifacts` exports the collected payload into the configured export_dir
    // (the cluster results dir downstream jobs read); `pull` only prints a laptop
    // rsync. Suggest the export step only when an export_dir is configured.
    if export_dir_configured {
        commands.push(format!("hpc-compose artifacts --job-id {job_id}"));
    }
    commands.push(format!(
        "hpc-compose pull --job-id {job_id} --into ./results"
    ));
    commands.push("hpc-compose down".to_string());
    commands.push(format!(
        "ssh {opts} <login-node>  # {note}",
        opts = control_master_opts_str(),
        note = OTP_MULTIPLEX_NOTE,
    ));
    commands
}

fn print_experiment_show_output(output: &ExperimentShowOutput) {
    println!("{}", term::styled_section_header("Experiment"));
    println!("  run:   {} (job {})", output.name, output.job_id);
    println!("  state: {}", output.state);
    if !output.tags.is_empty() {
        println!("  tags:  {}", output.tags.join(", "));
    }
    if !output.services.is_empty() {
        println!();
        println!("Services:");
        for service in &output.services {
            let node = service.nodelist.as_deref().unwrap_or("-");
            let status = service.status.as_deref().unwrap_or("-");
            println!("  {} [{status}] {node}", service.name);
            if let Some(hint) = &service.tunnel_hint {
                println!("    tunnel: {hint}");
            }
        }
    }
    if let Some(provenance) = &output.provenance {
        println!();
        println!("Provenance:");
        println!("  tool: {}", provenance.tool_version);
        if let Some(git) = &provenance.git {
            let dirty = if git.dirty { " (dirty)" } else { "" };
            println!("  git:  {}{dirty}", git.sha);
        }
    }
    if let Some(efficiency) = &output.efficiency {
        println!();
        println!(
            "Efficiency: {}/100 ({})",
            efficiency.score, efficiency.grade
        );
    }
    if let Some(results) = &output.results {
        let files = results.copied_relative_paths.len()
            + results
                .bundles
                .values()
                .map(|bundle| bundle.copied_relative_paths.len())
                .sum::<usize>();
        println!();
        println!("Artifacts: {files} collected file(s)");
    }
    if !output.notes.is_empty() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        println!();
        println!("Notes:");
        for note in &output.notes {
            println!(
                "  [{}] {}",
                output::format_age_seconds(now.saturating_sub(note.created_at)),
                note.text
            );
        }
    }
    println!();
    println!("Next:");
    for command in &output.next_commands {
        println!("  {command}");
    }
    println!();
    println!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));
}

/// Disambiguates concurrent staging directories from the same process.
static BUNDLE_STAGING_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `experiment bundle`: emit a citeable reproducibility archive for one tracked
/// run — compose spec, resolved config snapshot, rendered sbatch, provenance,
/// sweep manifest (seeds), metrics, checkpoint history, and a generated methods
/// appendix. Read-only over tracked state plus the same best-effort scheduler
/// probe `stats` performs; it writes only the bundle.
///
/// Every missing/degraded ingredient is a WARN to stderr plus an entry in the
/// MANIFEST `missing[]` ledger. The command still succeeds unless `--strict`.
pub(crate) fn experiment_bundle(
    context: ResolvedContext,
    job_id: Option<String>,
    output: Option<PathBuf>,
    dir: bool,
    strict: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;

    // Sweep trials minted their snapshot/plan with the trial's variable overlay;
    // re-apply it so the plan and the current effective config reproduce.
    let mut interpolation_vars = context.interpolation_vars.clone();
    if let Some(sweep) = &record.sweep {
        interpolation_vars.extend(interpolation_vars_for_sweep_metadata(sweep));
    }

    // Best-effort plan load: powers the run name and the sbatch reconstruction
    // fallback. A load failure is not fatal to the bundle.
    let plan = load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        &record.compose_file,
        &interpolation_vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )
    .ok();

    let stem = format!("experiment-bundle-{}", record.job_id);
    let requested = output.unwrap_or_else(|| {
        if dir {
            context.cwd.join(&stem)
        } else {
            context.cwd.join(format!("{stem}.tar.gz"))
        }
    });
    let output_abs = if requested.is_absolute() {
        requested
    } else {
        context.cwd.join(&requested)
    };

    // For a directory bundle the staged tree IS the output; for a tarball we
    // stage into a unique sibling directory, tar it, then remove the staging.
    let (staged_dir, staging_root) = if dir {
        (output_abs.clone(), None)
    } else {
        let parent = output_abs
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| context.cwd.clone());
        let root = parent.join(format!(
            ".{stem}.staging-{}-{}",
            std::process::id(),
            BUNDLE_STAGING_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let staged = root.join(&stem);
        (staged, Some(root))
    };
    fs::create_dir_all(&staged_dir)
        .with_context(|| format!("failed to create bundle directory {}", staged_dir.display()))?;

    let mut missing: Vec<ExperimentBundleMissing> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    let mut drift_detected: Option<bool> = None;
    let mut script_reconstructed = false;
    let mut sweep_seed: Option<String> = None;
    let mut metrics_summary: Option<String> = None;

    // spec/compose.yaml — the tracked compose file as submitted.
    match fs::read(&record.compose_file) {
        Ok(bytes) => stage_write(&staged_dir, "spec/compose.yaml", &bytes)?,
        Err(err) => note_missing(
            &mut missing,
            "spec/compose.yaml",
            format!(
                "compose file {} could not be read: {err}",
                record.compose_file.display()
            ),
        ),
    }

    // spec/config.snapshot.yaml + spec/spec-drift.diff (drift is best-effort).
    match record.config_snapshot_yaml.as_deref() {
        None => note_missing(
            &mut missing,
            "spec/config.snapshot.yaml",
            "no config snapshot was recorded (run-style or legacy submission)",
        ),
        Some(snapshot_yaml) => {
            stage_write(
                &staged_dir,
                "spec/config.snapshot.yaml",
                snapshot_yaml.as_bytes(),
            )?;
            match current_effective_config_yaml(&context, &record, &interpolation_vars) {
                Some(current_yaml) => {
                    match build_spec_diff_report(
                        &record.job_id,
                        record.submitted_at,
                        &record.compose_file,
                        snapshot_yaml,
                        &current_yaml,
                    ) {
                        Ok(report) if report.has_changes() => {
                            stage_write(
                                &staged_dir,
                                "spec/spec-drift.diff",
                                render_spec_drift_diff(&report).as_bytes(),
                            )?;
                            drift_detected = Some(true);
                        }
                        Ok(_) => drift_detected = Some(false),
                        Err(err) => note_missing(
                            &mut missing,
                            "spec/spec-drift.diff",
                            format!("drift check failed: {err}"),
                        ),
                    }
                }
                None => note_missing(
                    &mut missing,
                    "spec/spec-drift.diff",
                    "drift-check unavailable: the current spec could not be loaded",
                ),
            }
        }
    }

    // scripts/job.sbatch — from disk, else re-rendered from the plan.
    if record.script_path.is_file() {
        match fs::read(&record.script_path) {
            Ok(bytes) => stage_write(&staged_dir, "scripts/job.sbatch", &bytes)?,
            Err(err) => reconstruct_sbatch(
                &staged_dir,
                plan.as_ref(),
                &mut missing,
                &mut notes,
                &mut script_reconstructed,
                &format!(
                    "script {} could not be read: {err}",
                    record.script_path.display()
                ),
            )?,
        }
    } else {
        reconstruct_sbatch(
            &staged_dir,
            plan.as_ref(),
            &mut missing,
            &mut notes,
            &mut script_reconstructed,
            &format!(
                "the recorded script {} is not on disk",
                record.script_path.display()
            ),
        )?;
    }

    // provenance/record.json — the full record as stored.
    match serde_json::to_vec_pretty(&record) {
        Ok(bytes) => stage_write(&staged_dir, "provenance/record.json", &bytes)?,
        Err(err) => note_missing(
            &mut missing,
            "provenance/record.json",
            format!("record could not be serialized: {err}"),
        ),
    }

    // provenance/artifacts-manifest.json — the manifest JSON only (never payloads).
    let manifest_path = artifact_manifest_path_for_record(&record);
    if manifest_path.is_file() {
        match fs::read(&manifest_path) {
            Ok(bytes) => stage_write(&staged_dir, "provenance/artifacts-manifest.json", &bytes)?,
            Err(err) => note_missing(
                &mut missing,
                "provenance/artifacts-manifest.json",
                format!("artifact manifest could not be read: {err}"),
            ),
        }
    } else {
        note_missing(
            &mut missing,
            "provenance/artifacts-manifest.json",
            "no artifact manifest was recorded (run `hpc-compose artifacts` after the job)",
        );
    }

    // sweep/manifest.json — only for sweep-trial records (seeds live here).
    if let Some(sweep) = &record.sweep {
        match load_sweep_manifest(&record.compose_file, Some(&sweep.sweep_id)) {
            Ok(manifest) => {
                sweep_seed = manifest
                    .trials
                    .iter()
                    .find(|trial| trial.trial_id == sweep.trial_id)
                    .and_then(|trial| trial.seed.clone())
                    .or_else(|| manifest.seed.clone());
                match serde_json::to_vec_pretty(&manifest) {
                    Ok(bytes) => stage_write(&staged_dir, "sweep/manifest.json", &bytes)?,
                    Err(err) => note_missing(
                        &mut missing,
                        "sweep/manifest.json",
                        format!("sweep manifest could not be serialized: {err}"),
                    ),
                }
            }
            Err(err) => note_missing(
                &mut missing,
                "sweep/manifest.json",
                format!(
                    "sweep manifest for '{}' could not be loaded: {err}",
                    sweep.sweep_id
                ),
            ),
        }
    }

    // metrics/stats.csv + metrics/raw/*.jsonl — from the tracked metrics dir.
    let metrics_dir = metrics_dir_for_record(&record);
    if metrics_dir.is_dir() {
        match build_stats_snapshot(
            &record.compose_file,
            Some(&record.job_id),
            &StatsOptions {
                scheduler: SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
                sstat_bin: context.binaries.sstat.value.clone(),
                accounting: false,
            },
        ) {
            Ok(snapshot) => {
                let mut buffer = Vec::new();
                match output::write_stats_snapshot_csv(&mut buffer, &snapshot) {
                    Ok(()) => {
                        stage_write(&staged_dir, "metrics/stats.csv", &buffer)?;
                        metrics_summary = Some(stats_summary_line(&snapshot));
                    }
                    Err(err) => note_missing(
                        &mut missing,
                        "metrics/stats.csv",
                        format!("stats CSV could not be rendered: {err}"),
                    ),
                }
            }
            Err(err) => note_missing(
                &mut missing,
                "metrics/stats.csv",
                format!("stats snapshot could not be built: {err}"),
            ),
        }
        let copied_raw = copy_metrics_raw(&metrics_dir, &staged_dir)?;
        if copied_raw == 0 {
            note_missing(
                &mut missing,
                "metrics/raw",
                "no raw metrics JSONL files were found in the metrics directory",
            );
        }
    } else {
        note_missing(
            &mut missing,
            "metrics",
            "no metrics were collected for this job (no metrics directory)",
        );
    }

    // checkpoints/history.json — attempt/requeue history from local state.
    let history = collect_checkpoint_history(&record);
    match serde_json::to_vec_pretty(&history) {
        Ok(bytes) => stage_write(&staged_dir, "checkpoints/history.json", &bytes)?,
        Err(err) => note_missing(
            &mut missing,
            "checkpoints/history.json",
            format!("checkpoint history could not be serialized: {err}"),
        ),
    }

    // README.md — the generated methods appendix.
    let generated_at = format_unix_utc(unix_now_seconds());
    let resources = record
        .config_snapshot_yaml
        .as_deref()
        .map(resource_lines_from_snapshot)
        .unwrap_or_default();
    let artifact_names = read_artifact_manifest(&record)
        .map(artifact_inventory_names)
        .unwrap_or_default();
    let readme = BundleReadme {
        record: &record,
        name: plan.as_ref().map(|plan| plan.name.as_str()),
        generated_at: &generated_at,
        drift: drift_detected,
        reconstructed: script_reconstructed,
        snapshot_present: record.config_snapshot_yaml.is_some(),
        sweep_seed: sweep_seed.as_deref(),
        metrics_summary: metrics_summary.as_deref(),
        history: &history,
        artifact_names: &artifact_names,
        resources: &resources,
        missing: &missing,
    }
    .render();
    stage_write(&staged_dir, "README.md", readme.as_bytes())?;

    // MANIFEST.json — written last so it lists every other staged file.
    let files = collect_bundle_files(&staged_dir)?;
    let manifest = serde_json::json!({
        "schema_version": crate::output::OUTPUT_SCHEMA_VERSION,
        "tool_version": env!("CARGO_PKG_VERSION"),
        "job_id": record.job_id,
        "generated_at": generated_at,
        "layout": if dir { "directory" } else { "tarball" },
        "drift_detected": drift_detected,
        "files": files.iter().map(|file| serde_json::json!({
            "path": file.path,
            "sha256": file.sha256,
        })).collect::<Vec<_>>(),
        "missing": missing.iter().map(|entry| serde_json::json!({
            "item": entry.item,
            "reason": entry.reason,
        })).collect::<Vec<_>>(),
        "notes": notes,
    });
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize bundle MANIFEST.json")?;
    stage_write(&staged_dir, "MANIFEST.json", &manifest_bytes)?;
    // Re-scan so MANIFEST.json appears in the reported file ledger too.
    let files = collect_bundle_files(&staged_dir)?;

    // Finalize: tar the staged tree (deterministic sorted entries) and clean up.
    let layout = if dir {
        "directory"
    } else {
        write_tree_tarball(&output_abs, &staged_dir, Path::new(&stem))
            .with_context(|| format!("failed to write bundle tarball {}", output_abs.display()))?;
        if let Some(root) = &staging_root {
            let _ = fs::remove_dir_all(root);
        }
        "tarball"
    };

    let bundle_output = ExperimentBundleOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: record.job_id.clone(),
        output: output_abs.display().to_string(),
        layout: layout.to_string(),
        files,
        missing,
        drift_detected,
        strict,
    };

    // --strict fails after the ingredients have been reported (as warnings).
    if strict && !bundle_output.missing.is_empty() {
        let items = bundle_output
            .missing
            .iter()
            .map(|entry| entry.item.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "bundle for job {} is missing {} required ingredient(s) under --strict: {}",
            bundle_output.job_id,
            bundle_output.missing.len(),
            items
        );
    }

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&bundle_output)
                    .context("failed to serialize experiment bundle output")?
            );
        }
        OutputFormat::Text => print_bundle_output(&bundle_output),
    }
    Ok(())
}

/// Records a missing/degraded ingredient: warns to stderr and appends to the
/// `missing[]` ledger.
fn note_missing(missing: &mut Vec<ExperimentBundleMissing>, item: &str, reason: impl Into<String>) {
    let reason = reason.into();
    eprintln!("warning: bundle ingredient missing: {item} ({reason})");
    missing.push(ExperimentBundleMissing {
        item: item.to_string(),
        reason,
    });
}

/// Writes `bytes` to `<staged_dir>/<relative>`, creating parent directories.
fn stage_write(staged_dir: &Path, relative: &str, bytes: &[u8]) -> Result<()> {
    let path = staged_dir.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    crate::secure_io::write_atomic(&path, bytes, false)
        .with_context(|| format!("failed to write {}", path.display()))
}

/// Re-renders the sbatch from the plan when the on-disk script is unavailable,
/// marking it "reconstructed"; records it missing when re-render is impossible.
fn reconstruct_sbatch(
    staged_dir: &Path,
    plan: Option<&RuntimePlan>,
    missing: &mut Vec<ExperimentBundleMissing>,
    notes: &mut Vec<String>,
    reconstructed: &mut bool,
    disk_reason: &str,
) -> Result<()> {
    match plan {
        Some(plan) => match render_script_with_options(plan, &RenderOptions::default()) {
            Ok(script) => {
                stage_write(staged_dir, "scripts/job.sbatch", script.as_bytes())?;
                *reconstructed = true;
                notes.push(
                    "scripts/job.sbatch was reconstructed by re-rendering the plan; it may differ from the exact submitted script".to_string(),
                );
                eprintln!(
                    "warning: {disk_reason}; scripts/job.sbatch was reconstructed by re-rendering the plan"
                );
                Ok(())
            }
            Err(err) => {
                note_missing(
                    missing,
                    "scripts/job.sbatch",
                    format!("{disk_reason} and re-render failed: {err}"),
                );
                Ok(())
            }
        },
        None => {
            note_missing(
                missing,
                "scripts/job.sbatch",
                format!("{disk_reason} and the compose file could not be loaded to re-render it"),
            );
            Ok(())
        }
    }
}

/// Recomputes the current effective-config YAML for the record's compose file,
/// redacted exactly as the snapshot was. Returns `None` on any load failure so
/// drift degrades to "unavailable" rather than failing the bundle.
fn current_effective_config_yaml(
    context: &ResolvedContext,
    record: &SubmissionRecord,
    interpolation_vars: &BTreeMap<String, String>,
) -> Option<String> {
    let config =
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &record.compose_file,
            interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )
        .ok()?;
    output::effective_config_yaml(&config, &context.secret_values()).ok()
}

/// Copies every `*.jsonl` file in `metrics_dir` into `metrics/raw/`, returning
/// the count copied. Deterministic (sorted by file name).
fn copy_metrics_raw(metrics_dir: &Path, staged_dir: &Path) -> Result<usize> {
    let Ok(entries) = fs::read_dir(metrics_dir) else {
        return Ok(0);
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    files.sort();
    let mut copied = 0;
    for path in files {
        let Some(name) = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
        else {
            continue;
        };
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        stage_write(staged_dir, &format!("metrics/raw/{name}"), &bytes)?;
        copied += 1;
    }
    Ok(copied)
}

/// Recursively collects every file under `root` (relative, POSIX-separated,
/// sorted) with its sha256, for the MANIFEST ledger.
fn collect_bundle_files(root: &Path) -> Result<Vec<ExperimentBundleFile>> {
    let mut files = Vec::new();
    collect_bundle_files_into(root, root, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_bundle_files_into(
    root: &Path,
    dir: &Path,
    files: &mut Vec<ExperimentBundleFile>,
) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read {}", dir.display()))?
        .collect::<io::Result<Vec<_>>>()
        .with_context(|| format!("failed to read {}", dir.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if file_type.is_dir() {
            collect_bundle_files_into(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            files.push(ExperimentBundleFile {
                path: relative,
                sha256: hash_file(&path)?,
            });
        }
    }
    Ok(())
}

/// One-line human summary of a stats snapshot for the README.
fn stats_summary_line(snapshot: &hpc_compose::job::StatsSnapshot) -> String {
    if !snapshot.available {
        return match &snapshot.reason {
            Some(reason) => format!("metrics recorded but not summarizable ({reason})"),
            None => "metrics recorded but not summarizable".to_string(),
        };
    }
    format!(
        "{} step row(s) from {}",
        snapshot.steps.len(),
        snapshot.source
    )
}

/// Flat list of artifact names recorded in a manifest (top-level + bundles).
fn artifact_inventory_names(manifest: ArtifactManifest) -> Vec<String> {
    let mut names = manifest.copied_relative_paths;
    for bundle in manifest.bundles.into_values() {
        names.extend(bundle.copied_relative_paths);
    }
    names.sort();
    names.dedup();
    names
}

/// Renders a deterministic, ANSI-free textual drift diff for `spec-drift.diff`.
/// Left = the recorded snapshot; right = the current effective config.
fn render_spec_drift_diff(report: &SpecDiffReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("# spec drift for job {}\n", report.job_id));
    out.push_str("# left  = effective config snapshot recorded at submit time\n");
    out.push_str("# right = current effective config for the compose file\n");
    out.push_str(&format!(
        "# compose file: {}\n\n",
        report.compose_file.display()
    ));
    render_drift_section(&mut out, "Resources", &report.resource_changes);
    render_drift_section(&mut out, "Config", &report.config_changes);
    if !report.notes.is_empty() {
        out.push_str("Notes:\n");
        for note in &report.notes {
            out.push_str(&format!("- {note}\n"));
        }
    }
    out
}

fn render_drift_section(out: &mut String, title: &str, changes: &[JobDiffChange]) {
    out.push_str(&format!("## {title}\n"));
    if changes.is_empty() {
        out.push_str("  (no changes)\n\n");
        return;
    }
    for change in changes {
        match (&change.left, &change.right) {
            (Some(left), Some(right)) => {
                out.push_str(&format!("~ {}: {left} -> {right}\n", change.path));
            }
            (None, Some(right)) => out.push_str(&format!("+ {}: {right}\n", change.path)),
            (Some(left), None) => out.push_str(&format!("- {}: {left}\n", change.path)),
            (None, None) => out.push_str(&format!("? {}\n", change.path)),
        }
    }
    out.push('\n');
}

/// Leniently pulls display-worthy resource fields from the snapshot's top-level
/// `x-slurm` block. Omits anything absent; never fails.
fn resource_lines_from_snapshot(snapshot_yaml: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Ok(value) = serde_norway::from_str::<serde_norway::Value>(snapshot_yaml) else {
        return out;
    };
    let slurm = value.get("x-slurm");
    for (label, key) in [
        ("Partition", "partition"),
        ("Account", "account"),
        ("QoS", "qos"),
        ("Nodes", "nodes"),
        ("Walltime", "time"),
        ("GPUs", "gpus"),
        ("GPUs per node", "gpus_per_node"),
        ("GRES", "gres"),
        ("Memory", "mem"),
    ] {
        if let Some(rendered) = slurm
            .and_then(|map| map.get(key))
            .and_then(scalar_to_string)
        {
            out.push((label.to_string(), rendered));
        }
    }
    out
}

fn scalar_to_string(value: &serde_norway::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_u64() {
        return Some(number.to_string());
    }
    if let Some(flag) = value.as_bool() {
        return Some(flag.to_string());
    }
    value.as_f64().map(|number| number.to_string())
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// Formats a Unix timestamp (seconds) as an ISO-8601 UTC instant. Pure and
/// dependency-free (civil-from-days), so bundle content stays reproducible.
fn format_unix_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let seconds_of_day = secs % 86_400;
    let (hour, minute, second) = (
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
    );
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn print_bundle_output(output: &ExperimentBundleOutput) {
    println!("{}", term::styled_section_header("Experiment bundle"));
    println!(
        "  wrote {} bundle for job {} to {}",
        output.layout, output.job_id, output.output
    );
    println!("  files: {}", output.files.len());
    match output.drift_detected {
        Some(true) => println!("  spec drift: DETECTED (see spec/spec-drift.diff)"),
        Some(false) => println!("  spec drift: none"),
        None => println!("  spec drift: not checked"),
    }
    if output.missing.is_empty() {
        println!("  missing: none");
    } else {
        println!("  missing: {}", output.missing.len());
        for entry in &output.missing {
            println!("    - {} ({})", entry.item, entry.reason);
        }
    }
}

/// All the pieces the generated methods appendix (`README.md`) renders.
struct BundleReadme<'a> {
    record: &'a SubmissionRecord,
    name: Option<&'a str>,
    generated_at: &'a str,
    drift: Option<bool>,
    reconstructed: bool,
    snapshot_present: bool,
    sweep_seed: Option<&'a str>,
    metrics_summary: Option<&'a str>,
    history: &'a CheckpointHistory,
    artifact_names: &'a [String],
    resources: &'a [(String, String)],
    missing: &'a [ExperimentBundleMissing],
}

impl BundleReadme<'_> {
    fn render(&self) -> String {
        let record = self.record;
        let name = self.name.unwrap_or("(unknown run name)");
        let mut out = String::new();

        out.push_str(&format!("# Reproducibility bundle — {name}\n\n"));
        out.push_str(&format!(
            "Generated by hpc-compose {} at {} (UTC).\n\n",
            env!("CARGO_PKG_VERSION"),
            self.generated_at
        ));
        out.push_str(
            "This is a self-contained, citeable snapshot of one tracked run. It bundles the\ncompose spec, the resolved config snapshot, the rendered batch script, provenance,\nmetrics, checkpoint history, and (for sweep trials) the sweep manifest with seeds.\n\n",
        );

        out.push_str("## Run identity\n\n");
        out.push_str(&format!("- Name: {name}\n"));
        out.push_str(&format!("- Job id: {}\n", record.job_id));
        out.push_str(&format!("- Backend: {}\n", backend_label(record.backend)));
        out.push_str(&format!("- Kind: {}\n", kind_label(record.kind)));
        out.push_str(&format!(
            "- Submitted (UTC): {}\n",
            format_unix_utc(record.submitted_at)
        ));
        out.push_str(&format!(
            "- Compose file: {}\n\n",
            record.compose_file.display()
        ));

        out.push_str("## Provenance\n\n");
        match &record.provenance {
            Some(provenance) => {
                out.push_str(&format!("- Tool version: {}\n", provenance.tool_version));
                match &provenance.git {
                    Some(git) => {
                        let dirty = if git.dirty { "dirty" } else { "clean" };
                        out.push_str(&format!("- Git commit: {} ({dirty})\n", git.sha));
                        if let Some(branch) = &git.branch {
                            out.push_str(&format!("- Git branch: {branch}\n"));
                        }
                    }
                    None => out.push_str(
                        "- Git state: not recorded (submitted outside a git working tree)\n",
                    ),
                }
                out.push_str("\n### Image references\n\n");
                if provenance.image_refs.is_empty() {
                    out.push_str("No image references were recorded.\n\n");
                } else {
                    out.push_str("| Service | Reference |\n| --- | --- |\n");
                    for (service, reference) in &provenance.image_refs {
                        out.push_str(&format!("| {service} | `{reference}` |\n"));
                    }
                    out.push('\n');
                }
                out.push_str(
                    "> These are image **references as recorded at submit time**, not content\n> digests. They are not resolved against any registry and may since have moved.\n\n",
                );
            }
            None => out.push_str("No provenance was recorded for this run.\n\n"),
        }

        out.push_str("## Resources\n\n");
        if !self.snapshot_present {
            out.push_str(
                "No config snapshot was recorded, so resolved resources are unavailable.\n\n",
            );
        } else if self.resources.is_empty() {
            out.push_str(
                "No resource fields were found in the config snapshot (see `spec/config.snapshot.yaml`).\n\n",
            );
        } else {
            out.push_str("| Field | Value |\n| --- | --- |\n");
            for (label, value) in self.resources {
                out.push_str(&format!("| {label} | {value} |\n"));
            }
            out.push_str(
                "\nThe full resolved configuration is in `spec/config.snapshot.yaml`.\n\n",
            );
        }

        out.push_str("## Sweep\n\n");
        match &record.sweep {
            Some(sweep) => {
                out.push_str(&format!("- Sweep id: {}\n", sweep.sweep_id));
                out.push_str(&format!(
                    "- Trial: {} (index {})\n",
                    sweep.trial_id, sweep.trial_index
                ));
                match self.sweep_seed {
                    Some(seed) => out.push_str(&format!("- Seed: {seed}\n")),
                    None => out.push_str("- Seed: (none recorded for this trial)\n"),
                }
                if sweep.variables.is_empty() {
                    out.push_str("- Variables: (none)\n");
                } else {
                    out.push_str("\n| Variable | Value |\n| --- | --- |\n");
                    for (variable, value) in &sweep.variables {
                        out.push_str(&format!("| {variable} | {value} |\n"));
                    }
                }
                out.push_str("\nThe full sweep manifest is in `sweep/manifest.json`.\n\n");
            }
            None => out.push_str("This run was not part of a sweep.\n\n"),
        }

        out.push_str("## Attempt history\n\n");
        out.push_str(&format!(
            "- Attempts: {} (requeues: {})\n",
            self.history.attempts, self.history.requeues
        ));
        if !self.history.entries.is_empty() {
            out.push_str(
                "\n| Attempt | Status | Exit | Duration (s) |\n| --- | --- | --- | --- |\n",
            );
            for entry in &self.history.entries {
                out.push_str(&format!(
                    "| {} | {} | {} | {} |\n",
                    entry.attempt,
                    entry.job_status.as_deref().unwrap_or("-"),
                    entry
                        .job_exit_code
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    entry
                        .duration_seconds
                        .map(|seconds| seconds.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ));
            }
        }
        out.push_str("\nFull detail is in `checkpoints/history.json`.\n\n");

        out.push_str("## Metrics\n\n");
        match self.metrics_summary {
            Some(summary) => out.push_str(&format!(
                "{summary}. See `metrics/stats.csv` and the raw samples in `metrics/raw/`.\n\n"
            )),
            None => out.push_str("No metrics were collected for this run.\n\n"),
        }

        out.push_str("## Tags\n\n");
        if record.tags.is_empty() {
            out.push_str("(none)\n\n");
        } else {
            out.push_str(&format!("{}\n\n", record.tags.join(", ")));
        }

        out.push_str("## Notes\n\n");
        if record.notes.is_empty() {
            out.push_str("(none)\n\n");
        } else {
            for note in &record.notes {
                out.push_str(&format!(
                    "- [{}] {}\n",
                    format_unix_utc(note.created_at),
                    note.text
                ));
            }
            out.push('\n');
        }

        out.push_str("## Artifacts\n\n");
        if self.artifact_names.is_empty() {
            out.push_str(
                "No artifact manifest was recorded. Payloads are never bundled; only the manifest is.\n\n",
            );
        } else {
            out.push_str("Names recorded in the artifact manifest (payloads are not bundled):\n\n");
            for artifact in self.artifact_names {
                out.push_str(&format!("- `{artifact}`\n"));
            }
            out.push('\n');
        }

        out.push_str("## Reproducing this run\n\n");
        out.push_str("```sh\n");
        out.push_str(&format!(
            "hpc-compose up -f {}\n",
            record.compose_file.display()
        ));
        out.push_str(&format!(
            "hpc-compose diff --against-spec {}   # check drift vs this run's snapshot\n",
            record.job_id
        ));
        out.push_str("```\n\n");
        out.push_str(
            "The config snapshot in `spec/config.snapshot.yaml` is the ground-truth effective\nconfiguration this run used; treat it, not the current compose file, as authoritative.\n\n",
        );
        match self.drift {
            Some(true) => out.push_str(
                "> The current compose spec **differs** from this run's snapshot; see `spec/spec-drift.diff`.\n\n",
            ),
            Some(false) => {
                out.push_str("> The current compose spec matches this run's snapshot.\n\n");
            }
            None if self.snapshot_present => {
                out.push_str("> Spec drift could not be checked (the current spec failed to load).\n\n");
            }
            None => out.push_str(
                "> No config snapshot was recorded, so spec drift cannot be checked.\n\n",
            ),
        }
        if self.reconstructed {
            out.push_str(
                "> Note: `scripts/job.sbatch` was reconstructed by re-rendering the plan and may\n> differ from the exact script that was submitted.\n\n",
            );
        }

        if !self.missing.is_empty() {
            out.push_str("## Missing ingredients\n\n");
            out.push_str("The following ingredients were not available (see `MANIFEST.json`):\n\n");
            for entry in self.missing {
                out.push_str(&format!("- `{}` — {}\n", entry.item, entry.reason));
            }
            out.push('\n');
        }

        out.push_str("## Bundle contents\n\n");
        out.push_str("| Path | Contents |\n| --- | --- |\n");
        out.push_str("| `README.md` | This methods appendix |\n");
        out.push_str("| `MANIFEST.json` | Per-file sha256 + missing ledger |\n");
        out.push_str("| `spec/compose.yaml` | The tracked compose spec |\n");
        out.push_str(
            "| `spec/config.snapshot.yaml` | Resolved config snapshot (secret-redacted) |\n",
        );
        out.push_str("| `spec/spec-drift.diff` | Present only when the current spec drifted |\n");
        out.push_str("| `scripts/job.sbatch` | Rendered batch script |\n");
        out.push_str("| `provenance/record.json` | The full tracked submission record |\n");
        out.push_str("| `provenance/artifacts-manifest.json` | Artifact manifest (names only) |\n");
        out.push_str("| `sweep/manifest.json` | Sweep manifest with seeds (sweep trials only) |\n");
        out.push_str("| `metrics/stats.csv` | Metrics summary as CSV |\n");
        out.push_str("| `metrics/raw/*.jsonl` | Raw metric samples |\n");
        out.push_str("| `checkpoints/history.json` | Attempt/requeue history |\n");

        out
    }
}

fn backend_label(backend: SubmissionBackend) -> &'static str {
    match backend {
        SubmissionBackend::Slurm => "slurm",
        SubmissionBackend::Local => "local",
    }
}

fn kind_label(kind: SubmissionKind) -> &'static str {
    match kind {
        SubmissionKind::Main => "main",
        SubmissionKind::Run => "run",
        SubmissionKind::Canary => "canary",
        SubmissionKind::SweepTrial => "sweep_trial",
        SubmissionKind::Notebook => "notebook",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use hpc_compose::job::{ArtifactManifest, GitProvenance, JobProvenance};
    use hpc_compose::spec::ComposeSpec;

    use super::*;

    fn plan_with_services(yaml: &str) -> RuntimePlan {
        // Pure plan derivation only: build the normalized plan and map it into a
        // RuntimePlan without importing or preparing any images.
        let dir = tempfile::tempdir().expect("tmpdir");
        let compose = dir.path().join("compose.yaml");
        fs::write(&compose, yaml).expect("write compose");
        let spec = ComposeSpec::load(&compose).expect("spec");
        let plan = hpc_compose::planner::build_plan(&compose, spec).expect("plan");
        hpc_compose::prepare::build_runtime_plan(&plan)
    }

    fn record_for(job_id: &str) -> SubmissionRecord {
        // Minimal tracked record via serde (all optional/additive fields take
        // their defaults) so the test does not enumerate every record field.
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "backend": "slurm",
            "kind": "main",
            "job_id": job_id,
            "submitted_at": 1,
            "compose_file": "/tmp/compose.yaml",
            "submit_dir": "/tmp",
            "script_path": "/tmp/x.sbatch",
            "cache_dir": "/tmp/cache",
            "batch_log": "/tmp/batch.log",
            "service_logs": {},
        }))
        .expect("submission record")
    }

    fn snapshot_for(plan: &RuntimePlan, state: &str) -> StatusSnapshot {
        // A minimal synthetic status snapshot: round-trips JSON so the test does
        // not depend on private field construction. Mirrors what
        // build_status_snapshot would produce for a degraded probe.
        let services = plan
            .ordered_services
            .iter()
            .map(|service| {
                serde_json::json!({
                    "service_name": service.name,
                    "path": "/tmp/log",
                    "present": false,
                    "updated_at": null,
                    "updated_age_seconds": null,
                    "nodelist": "gpu042,gpu043",
                    "status": "running",
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "record": {
                "schema_version": 1,
                "backend": "slurm",
                "kind": "main",
                "job_id": "12345",
                "submitted_at": 1,
                "compose_file": "/tmp/compose.yaml",
                "submit_dir": "/tmp",
                "script_path": "/tmp/x.sbatch",
                "cache_dir": "/tmp/cache",
                "batch_log": "/tmp/batch.log",
                "service_logs": {},
            },
            "scheduler": {
                "state": state,
                "source": "squeue",
                "terminal": false,
                "failed": false,
                "detail": null,
            },
            "log_dir": "/tmp",
            "batch_log": {
                "path": "/tmp/batch.log",
                "present": false,
                "updated_at": null,
                "updated_age_seconds": null,
            },
            "services": services,
            "attempt": null,
            "is_resume": null,
            "resume_dir": null,
        });
        serde_json::from_value(value).expect("status snapshot")
    }

    const TCP_COMPOSE: &str = r#"name: exp-test
x-slurm:
  time: "00:10:00"
services:
  api:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: tcp
      port: 8000
  worker:
    image: docker://python:3.12
    command: ["true"]
"#;

    #[test]
    fn assembles_aggregate_with_field_mapping() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "RUNNING");
        let mut image_refs = BTreeMap::new();
        image_refs.insert("api".to_string(), "docker://python:3.12".to_string());
        let mut record = record_for("12345");
        record.provenance = Some(JobProvenance {
            tool_version: "9.9.9".to_string(),
            git: Some(GitProvenance {
                sha: "abc123".to_string(),
                dirty: false,
                branch: Some("main".to_string()),
            }),
            image_refs,
            source_content_hash: None,
        });
        record.tags = vec!["baseline".to_string(), "lr-bug".to_string()];
        record.notes = vec![hpc_compose::job::JobNote {
            text: "diverged after epoch 3".to_string(),
            created_at: 42,
        }];

        let output =
            build_experiment_show_output(&record, &plan, &snapshot, None, None, Some("login01"));

        assert_eq!(output.job_id, "12345");
        assert_eq!(output.name, "exp-test");
        assert_eq!(output.state, "RUNNING");
        assert_eq!(output.services.len(), 2);
        let api = output
            .services
            .iter()
            .find(|service| service.name == "api")
            .expect("api service");
        assert_eq!(api.nodelist.as_deref(), Some("gpu042,gpu043"));
        assert_eq!(api.status.as_deref(), Some("running"));
        // TCP readiness -> a tunnel hint with the first node and login host.
        let hint = api.tunnel_hint.as_deref().expect("tunnel hint");
        assert!(hint.contains("-L 8000:gpu042:8000 login01"), "hint: {hint}");
        assert!(hint.contains("ControlMaster=auto"), "multiplex: {hint}");
        assert!(output.provenance.is_some());
        // Tags and notes on the record surface in the aggregate.
        assert_eq!(output.tags, vec!["baseline", "lr-bug"]);
        assert_eq!(output.notes.len(), 1);
        assert_eq!(output.notes[0].text, "diverged after epoch 3");
        assert_eq!(output.notes[0].created_at, 42);
    }

    #[test]
    fn tunnel_hint_only_for_tcp_or_http_services_and_placeholders_when_unknown() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "RUNNING");
        // No login host and the worker has no readiness port.
        let output =
            build_experiment_show_output(&record_for("12345"), &plan, &snapshot, None, None, None);

        let worker = output
            .services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker service");
        assert!(worker.tunnel_hint.is_none(), "no readiness port -> no hint");

        let api = output
            .services
            .iter()
            .find(|service| service.name == "api")
            .expect("api service");
        let hint = api.tunnel_hint.as_deref().expect("tunnel hint");
        // Compute node is present in the snapshot; login host degrades.
        assert!(
            hint.contains("-L 8000:gpu042:8000 <login-node>"),
            "hint: {hint}"
        );
    }

    #[test]
    fn degrades_to_none_for_legacy_and_missing_data() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "PENDING");
        let output =
            build_experiment_show_output(&record_for("12345"), &plan, &snapshot, None, None, None);
        assert!(
            output.provenance.is_none(),
            "legacy record -> no provenance"
        );
        assert!(output.results.is_none(), "no manifest -> no results");
        assert!(output.efficiency.is_none(), "non-terminal -> no efficiency");
        assert_eq!(output.state, "PENDING");
    }

    #[test]
    fn results_round_trip_into_aggregate() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "COMPLETED");
        let manifest: ArtifactManifest = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "job_id": "12345",
            "collect_policy": "always",
            "collected_at": "2026-01-01T00:00:00Z",
            "job_outcome": "completed",
            "copied_relative_paths": ["a.txt", "b.txt"],
        }))
        .expect("manifest");
        let output = build_experiment_show_output(
            &record_for("12345"),
            &plan,
            &snapshot,
            Some(manifest),
            None,
            None,
        );
        let results = output.results.expect("results");
        assert_eq!(results.copied_relative_paths.len(), 2);
    }

    #[test]
    fn next_commands_reference_only_shipped_commands_with_multiplex_hint() {
        let commands = experiment_next_commands("12345", true);
        // Every entry names a shipped command path (or is the ssh multiplex hint).
        for command in &commands {
            assert!(
                command.starts_with("hpc-compose status")
                    || command.starts_with("hpc-compose score")
                    || command.starts_with("hpc-compose artifacts")
                    || command.starts_with("hpc-compose pull")
                    || command.starts_with("hpc-compose down")
                    || command.starts_with("ssh "),
                "unexpected next_command: {command}"
            );
        }
        // The export step is surfaced only when an export_dir is configured.
        assert!(
            commands
                .iter()
                .any(|command| command == "hpc-compose artifacts --job-id 12345"),
            "artifacts export hint must appear when export_dir is configured: {commands:?}"
        );
        assert!(
            experiment_next_commands("12345", false)
                .iter()
                .all(|command| !command.starts_with("hpc-compose artifacts")),
            "artifacts export hint must be omitted without an export_dir"
        );
        // The ControlMaster/ControlPath/ControlPersist multiplexing line is present.
        let ssh = commands
            .iter()
            .find(|command| command.starts_with("ssh "))
            .expect("ssh multiplex hint");
        assert!(ssh.contains("ControlMaster=auto"), "control master: {ssh}");
        assert!(ssh.contains("ControlPath="), "control path: {ssh}");
        assert!(ssh.contains("ControlPersist="), "control persist: {ssh}");
    }
}
