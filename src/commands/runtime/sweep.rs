use super::*;
use crate::time_util::unix_timestamp_now;

const DEFAULT_SWEEP_MAX_TRIALS: usize = 100;

#[derive(Debug, Serialize)]
struct SweepSubmitOutput<'a> {
    dry_run: bool,
    manifest_path: Option<PathBuf>,
    manifest: &'a SweepManifest,
}

#[derive(Debug, Serialize)]
struct SweepStatusOutput {
    sweep_id: String,
    compose_file: PathBuf,
    submitted_at: u64,
    summary: BTreeMap<String, usize>,
    trials: Vec<SweepStatusTrialOutput>,
}

#[derive(Debug, Serialize)]
struct SweepStatusTrialOutput {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    job_id: Option<String>,
    status: String,
    scheduler_state: Option<String>,
    record_path: Option<PathBuf>,
    submit_error: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct SweepListOutput {
    compose_file: PathBuf,
    sweeps: Vec<SweepManifest>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sweep_submit(
    context: ResolvedContext,
    dry_run: bool,
    max_trials: Option<usize>,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    format: Option<OutputFormat>,
    quiet: bool,
) -> Result<()> {
    let file = context.compose_file.value.clone();
    let sweep = ComposeSpec::load_sweep(&file)?.with_context(|| {
        format!(
            "{} does not contain a top-level sweep block",
            file.display()
        )
    })?;
    let sweep_id = generate_sweep_id();
    let max_trials = max_trials.unwrap_or(DEFAULT_SWEEP_MAX_TRIALS);
    let expansion = expand_sweep_with_limit(&sweep, &sweep_id, Some(max_trials))?;

    let output_format = output::resolve_output_format(format, false);
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let manifest_path = sweep_manifest_path_for(&file, &sweep_id);
    let sweep_root = manifest_path
        .parent()
        .context("sweep manifest path has no parent")?
        .to_path_buf();
    let submitted_at = unix_timestamp_now();
    let mut manifest = SweepManifest {
        schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
        sweep_id: sweep_id.clone(),
        compose_file: file.clone(),
        submitted_at,
        matrix: expansion.matrix.clone(),
        seed: expansion.seed.clone(),
        total_combinations: expansion.total_combinations,
        objective: sweep.objective.clone(),
        best_trial: None,
        stopped_at: None,
        stop_reason: None,
        trials: expansion
            .trials
            .iter()
            .map(|trial| SweepManifestTrial {
                trial_id: trial.trial_id.clone(),
                index: trial.index,
                variables: trial.variables.clone(),
                script_path: sweep_root.join(format!("{}.sbatch", trial.trial_id)),
                job_id: None,
                record_path: None,
                submitted_at: None,
                submit_error: None,
                objective: None,
                objective_error: None,
                observed_at: None,
            })
            .collect(),
    };

    if dry_run {
        let cluster_profile = load_discovered_cluster_profile(&context)?;
        for trial in &expansion.trials {
            validate_sweep_trial_plan(&context, trial, &sweep_id, cluster_profile.clone())?;
        }
        print_sweep_submit_output(output_format, true, None, &manifest)?;
        return Ok(());
    }

    write_sweep_manifest(&manifest).context("failed to persist initial sweep manifest")?;
    let cluster_profile = load_discovered_cluster_profile(&context)?;
    let progress = ProgressReporter::new(!quiet && output_format == OutputFormat::Text);

    for (index, trial) in expansion.trials.iter().enumerate() {
        let result = submit_sweep_trial(
            &context,
            trial,
            &sweep_id,
            &submit_dir,
            &manifest.trials[index].script_path,
            skip_prepare,
            force_rebuild,
            no_preflight,
            cluster_profile.clone(),
            &progress,
            output_format,
            quiet,
        );
        match result {
            Ok(record) => {
                let record_path = latest_record_path(&record);
                manifest.trials[index].job_id = Some(record.job_id.clone());
                manifest.trials[index].record_path = Some(record_path);
                manifest.trials[index].submitted_at = Some(record.submitted_at);
                write_sweep_manifest(&manifest)
                    .context("failed to persist sweep manifest after trial submission")?;
                if output_format == OutputFormat::Text {
                    println!(
                        "submitted {} job {} ({})",
                        trial.trial_id,
                        record.job_id,
                        format_sweep_variables(&trial.variables)
                    );
                }
            }
            Err(err) => {
                manifest.trials[index].submit_error = Some(err.to_string());
                write_sweep_manifest(&manifest)
                    .context("failed to persist sweep manifest after trial failure")?;
                return Err(err.context(format!("sweep trial {} failed", trial.trial_id)));
            }
        }
    }

    print_sweep_submit_output(output_format, false, Some(manifest_path), &manifest)
}

fn validate_sweep_trial_plan(
    context: &ResolvedContext,
    trial: &SweepExpansionTrial,
    sweep_id: &str,
    cluster_profile: Option<hpc_compose::cluster::ClusterProfile>,
) -> Result<()> {
    let vars = sweep_interpolation_vars(context, sweep_id, trial);
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "sweep submit does not support x-slurm.array; each sweep trial is already a separate allocation"
        );
    }
    render_script_with_options(
        &runtime_plan,
        &RenderOptions {
            apptainer_bin: context.binaries.apptainer.value.clone(),
            singularity_bin: context.binaries.singularity.value.clone(),
            cluster_profile,
            runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                &context.cwd,
                runtime_plan.slurm.runtime_root.as_deref(),
            )),
        },
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn submit_sweep_trial(
    context: &ResolvedContext,
    trial: &SweepExpansionTrial,
    sweep_id: &str,
    submit_dir: &Path,
    script_path: &Path,
    skip_prepare: bool,
    force_rebuild: bool,
    no_preflight: bool,
    cluster_profile: Option<hpc_compose::cluster::ClusterProfile>,
    progress: &ProgressReporter,
    output_format: OutputFormat,
    quiet: bool,
) -> Result<SubmissionRecord> {
    let vars = sweep_interpolation_vars(context, sweep_id, trial);
    let file = context.compose_file.value.clone();
    let effective_config =
        output::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml = output::effective_config_yaml(
        &effective_config,
        &crate::redaction::secret_value_set(
            &context.interpolation_vars,
            &context.interpolation_var_sources,
        ),
    )?;
    let runtime_plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "sweep submit does not support x-slurm.array; each sweep trial is already a separate allocation"
        );
    }

    if !no_preflight {
        let report = progress.run_checked_result(
            format!("Running preflight checks for {}", trial.trial_id),
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.sbatch.value.clone(),
                        srun_bin: context.binaries.srun.value.clone(),
                        scontrol_bin: context.binaries.scontrol.value.clone(),
                        require_submit_tools: true,
                        skip_prepare,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if !quiet || report.has_errors() {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed for sweep trial {}", trial.trial_id);
        }
    }

    if !skip_prepare {
        let prepare_progress =
            PrepareProgress::new(&runtime_plan, !quiet && output_format == OutputFormat::Text);
        let summary = progress.run_result(format!("Preparing {}", trial.trial_id), || {
            prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    keep_failed_prep: false,
                    force_rebuild,
                },
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && output_format == OutputFormat::Text {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result(format!("Rendering {}", trial.trial_id), || {
        render_script_with_options(
            &runtime_plan,
            &RenderOptions {
                apptainer_bin: context.binaries.apptainer.value.clone(),
                singularity_bin: context.binaries.singularity.value.clone(),
                cluster_profile,
                runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                    &context.cwd,
                    runtime_plan.slurm.runtime_root.as_deref(),
                )),
            },
        )
    })?;
    crate::secure_io::write(script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    let prepared = PreparedSlurmSubmission {
        file,
        submit_dir: submit_dir.to_path_buf(),
        script_path: script_path.to_path_buf(),
        runtime_plan: runtime_plan.clone(),
        record_options: SubmissionRecordBuildOptions {
            kind: SubmissionKind::SweepTrial,
            service_name: None,
            command_override: None,
            requested_walltime: requested_walltime(&runtime_plan),
            slurm_array: None,
            sweep: Some(SweepTrialMetadata {
                sweep_id: sweep_id.to_string(),
                trial_id: trial.trial_id.clone(),
                trial_index: trial.index,
                variables: trial.variables.clone(),
            }),
            config_snapshot_yaml: Some(effective_config_yaml),
            cached_artifacts: tracked_cached_artifacts(&runtime_plan),
            provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
        },
        output_format,
    };
    let outcome = submit_prepared_slurm_submission(context, &prepared, progress)?;
    let Some((record, persisted)) = outcome.tracked_submission else {
        bail!(
            "sbatch output for sweep trial {} did not include a numeric Slurm job id",
            trial.trial_id
        );
    };
    if !persisted {
        bail!(
            "tracking metadata could not be written for sweep trial {} job {}",
            trial.trial_id,
            record.job_id
        );
    }
    Ok(record)
}

fn sweep_interpolation_vars(
    context: &ResolvedContext,
    sweep_id: &str,
    trial: &SweepExpansionTrial,
) -> BTreeMap<String, String> {
    let mut vars = context.interpolation_vars.clone();
    vars.extend(interpolation_vars_for_sweep_trial(sweep_id, trial));
    vars
}

fn print_sweep_submit_output(
    output_format: OutputFormat,
    dry_run: bool,
    manifest_path: Option<PathBuf>,
    manifest: &SweepManifest,
) -> Result<()> {
    match output_format {
        OutputFormat::Text => {
            println!("sweep: {}", manifest.sweep_id);
            println!("trials: {}", manifest.trials.len());
            if let Some(seed) = &manifest.seed {
                println!("seed: {seed}");
            }
            if dry_run {
                println!("dry run: no scripts written and no jobs submitted");
            } else if let Some(path) = &manifest_path {
                println!("manifest: {}", path.display());
            }
            for trial in &manifest.trials {
                let status = trial
                    .job_id
                    .as_deref()
                    .or(trial.submit_error.as_deref())
                    .unwrap_or("pending submit");
                println!(
                    "  {} {} {}",
                    trial.trial_id,
                    status,
                    format_sweep_variables(&trial.variables)
                );
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&SweepSubmitOutput {
                    dry_run,
                    manifest_path,
                    manifest,
                })
                .context("failed to serialize sweep submit output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn sweep_status(
    context: ResolvedContext,
    sweep_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
    let options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value,
        sacct_bin: context.binaries.sacct.value,
    };
    let mut summary = BTreeMap::new();
    let trials = manifest
        .trials
        .iter()
        .map(|trial| {
            let output = status_for_sweep_trial(&manifest, trial, &options);
            *summary.entry(output.status.clone()).or_insert(0) += 1;
            output
        })
        .collect::<Vec<_>>();
    let report = SweepStatusOutput {
        sweep_id: manifest.sweep_id,
        compose_file: manifest.compose_file,
        submitted_at: manifest.submitted_at,
        summary,
        trials,
    };
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => print_sweep_status_output(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize sweep status output")?
            );
            Ok(())
        }
    }
}

fn status_for_sweep_trial(
    manifest: &SweepManifest,
    trial: &SweepManifestTrial,
    options: &SchedulerOptions,
) -> SweepStatusTrialOutput {
    if let Some(error) = &trial.submit_error {
        return SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: trial.job_id.clone(),
            status: "submit_failed".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: Some(error.clone()),
            detail: None,
        };
    }
    let Some(job_id) = trial.job_id.as_deref() else {
        return SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: None,
            status: "unknown".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: Some("trial has no recorded job id".to_string()),
        };
    };
    match build_status_snapshot(&manifest.compose_file, Some(job_id), options) {
        Ok(snapshot) => SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: Some(job_id.to_string()),
            status: categorize_sweep_status(&snapshot),
            scheduler_state: Some(snapshot.scheduler.state),
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: None,
        },
        Err(err) => SweepStatusTrialOutput {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            job_id: Some(job_id.to_string()),
            status: "missing_tracking".to_string(),
            scheduler_state: None,
            record_path: trial.record_path.clone(),
            submit_error: None,
            detail: Some(err.to_string()),
        },
    }
}

fn categorize_sweep_status(snapshot: &hpc_compose::job::StatusSnapshot) -> String {
    if snapshot.scheduler.state == "PENDING" {
        return "pending".to_string();
    }
    if snapshot.scheduler.state == "RUNNING" {
        return "running".to_string();
    }
    let service_failed = snapshot.services.iter().any(|service| {
        service.status.as_deref() == Some("failed")
            || service
                .assertions
                .as_ref()
                .is_some_and(|assertions| !assertions.failures.is_empty())
    });
    if snapshot.scheduler.terminal {
        if snapshot.scheduler.failed || service_failed {
            "failed".to_string()
        } else {
            "completed".to_string()
        }
    } else {
        "unknown".to_string()
    }
}

fn print_sweep_status_output(report: &SweepStatusOutput) -> Result<()> {
    println!("sweep: {}", report.sweep_id);
    println!("trials: {}", report.trials.len());
    print!("summary:");
    for (status, count) in &report.summary {
        print!(" {status}={count}");
    }
    println!();
    for trial in &report.trials {
        let job = trial.job_id.as_deref().unwrap_or("-");
        let scheduler = trial.scheduler_state.as_deref().unwrap_or("-");
        println!(
            "  {} {:<16} job={} scheduler={} {}",
            trial.trial_id,
            trial.status,
            job,
            scheduler,
            format_sweep_variables(&trial.variables)
        );
    }
    Ok(())
}

pub(crate) fn sweep_list(context: ResolvedContext, format: Option<OutputFormat>) -> Result<()> {
    let sweeps = scan_sweep_manifests(&context.compose_file.value)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("compose: {}", context.compose_file.value.display());
            println!("sweeps: {}", sweeps.len());
            for sweep in &sweeps {
                println!(
                    "  {} trials={} submitted_at={}",
                    sweep.sweep_id,
                    sweep.trials.len(),
                    sweep.submitted_at
                );
            }
            Ok(())
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&SweepListOutput {
                    compose_file: context.compose_file.value,
                    sweeps,
                })
                .context("failed to serialize sweep list output")?
            );
            Ok(())
        }
    }
}

fn format_sweep_variables(vars: &BTreeMap<String, String>) -> String {
    vars.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parses one trial's objective value from its tracked log or artifacts.
///
/// Returns `Ok(Some(value))` on success, `Ok(None)` when the trial is not yet
/// terminal or has no parseable objective, and `Err` only on unexpected IO.
fn parse_trial_objective(
    trial: &SweepManifestTrial,
    manifest: &SweepManifest,
    options: &SchedulerOptions,
) -> Result<Option<f64>> {
    let Some(job_id) = trial.job_id.as_deref() else {
        return Ok(None);
    };
    let objective = match manifest.objective.as_ref() {
        Some(objective) => objective,
        None => return Ok(None),
    };
    let snapshot = build_status_snapshot(&manifest.compose_file, Some(job_id), options)?;
    if !snapshot.scheduler.terminal {
        return Ok(None);
    }
    if let Some(pattern) = &objective.log_pattern {
        let re = regex::Regex::new(pattern).with_context(|| {
            format!("sweep.objective.log_pattern '{pattern}' is not a valid regex")
        })?;
        let group = objective.group as usize;
        for service in &snapshot.services {
            let Some(log_path) = &service.log_path else {
                continue;
            };
            let Ok(text) = fs::read_to_string(log_path) else {
                continue;
            };
            if let Some(captures) = re.captures(&text)
                && let Some(matched) = captures.get(group)
                && let Ok(value) = matched.as_str().parse::<f64>()
            {
                return Ok(Some(value));
            }
        }
        return Ok(None);
    }
    // json_path source: read from the trial job's artifact tree.
    if let (Some(json_rel), Some(field)) = (&objective.json_path, &objective.json_field) {
        let record_path = trial.record_path.as_deref().with_context(|| {
            format!(
                "trial {} has no record path for json objective",
                trial.trial_id
            )
        })?;
        let record_dir = record_path.parent().unwrap_or_else(|| Path::new("."));
        let job_root = crate::tracked_paths::runtime_job_root(record_dir, job_id);
        let artifacts_dir = crate::tracked_paths::latest_artifacts_dir(&job_root);
        let json_path = artifacts_dir.join(json_rel);
        if let Ok(text) = fs::read_to_string(&json_path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
            && let Some(num) = value.get(field).and_then(|v| v.as_f64())
        {
            return Ok(Some(num));
        }
        return Ok(None);
    }
    Ok(None)
}

/// Selects the best trial id from observed objectives given the direction.
fn best_trial_id(
    trials: &[SweepManifestTrial],
    direction: hpc_compose::spec::ObjectiveDirection,
) -> Option<String> {
    let scored = trials.iter().filter_map(|t| {
        t.objective
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|value| (value, &t.trial_id))
    });
    match direction {
        hpc_compose::spec::ObjectiveDirection::Minimize => scored
            .min_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, id)| id.clone()),
        hpc_compose::spec::ObjectiveDirection::Maximize => scored
            .max_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, id)| id.clone()),
    }
}

/// One comparison operator for `--stop-when` evaluation.
type StopOperator = (&'static str, fn(f64, f64) -> bool);

fn evaluate_stop_condition(expr: &str, best: Option<f64>) -> Result<bool> {
    let expr = expr.trim();
    let operators: &[StopOperator] = &[
        ("<=", |a, b| a <= b),
        (">=", |a, b| a >= b),
        ("<", |a, b| a < b),
        (">", |a, b| a > b),
    ];
    let Some(rest) = expr.strip_prefix("objective") else {
        bail!("--stop-when must look like `objective < 0.05` or `objective >= 0.9` (got '{expr}')")
    };
    let rest = rest.trim();
    for (op, cmp) in operators {
        if let Some(threshold_str) = rest.strip_prefix(op) {
            let threshold: f64 = threshold_str.trim().parse().with_context(|| {
                format!(
                    "--stop-when threshold '{}' is not a number",
                    threshold_str.trim()
                )
            })?;
            return Ok(best.is_some_and(|value| cmp(value, threshold)));
        }
    }
    bail!("--stop-when must look like `objective < 0.05` or `objective >= 0.9` (got '{expr}')");
}

#[derive(Debug, Serialize)]
struct SweepObserveOutput {
    sweep_id: String,
    objective_configured: bool,
    best_trial: Option<String>,
    best_objective: Option<String>,
    trials: Vec<SweepObserveTrial>,
}

#[derive(Debug, Clone, Serialize)]
struct SweepObserveTrial {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    job_id: Option<String>,
    status: String,
    objective: Option<String>,
    objective_error: Option<String>,
}

pub(crate) fn sweep_observe(
    context: ResolvedContext,
    sweep_id: Option<String>,
    watch: bool,
    stop_when: Option<String>,
    poll_interval: Duration,
    timeout: Option<Duration>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let output_format = output::resolve_output_format(format, false);
    let deadline = timeout
        .filter(|t| *t > Duration::ZERO)
        .map(|t| Instant::now() + t);
    loop {
        let mut manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
        let objective_configured = manifest.objective.is_some();
        let now = unix_timestamp_now();

        let direction = manifest
            .objective
            .as_ref()
            .map(|o| o.direction)
            .unwrap_or(hpc_compose::spec::ObjectiveDirection::Minimize);

        // Pass 1 (immutable): compute each trial's status and parsed objective.
        let mut results: Vec<(String, Option<f64>, Option<String>)> = Vec::new();
        for trial in &manifest.trials {
            let status = status_for_sweep_trial(&manifest, trial, &scheduler_options);
            let status_label = status.status.clone();
            let (parsed, error) = match parse_trial_objective(trial, &manifest, &scheduler_options)
            {
                Ok(Some(value)) => (Some(value), None),
                Ok(None) => (None, None),
                Err(err) => (None, Some(format!("{err:#}"))),
            };
            results.push((status_label, parsed, error));
        }

        let mut trial_outputs = Vec::new();
        let best_objective = if objective_configured {
            // Pass 2 (mutable): write objective state back into the manifest.
            for (trial, (status_label, parsed, error)) in manifest.trials.iter_mut().zip(results) {
                trial.objective = parsed.map(|v| v.to_string());
                trial.objective_error = error.clone();
                trial.observed_at = Some(now);
                trial_outputs.push(SweepObserveTrial {
                    trial_id: trial.trial_id.clone(),
                    index: trial.index,
                    variables: trial.variables.clone(),
                    job_id: trial.job_id.clone(),
                    status: status_label,
                    objective: trial.objective.clone(),
                    objective_error: error,
                });
            }
            manifest.best_trial = best_trial_id(&manifest.trials, direction);
            let best_objective = manifest.best_trial.as_ref().and_then(|id| {
                manifest
                    .trials
                    .iter()
                    .find(|t| &t.trial_id == id)
                    .and_then(|t| t.objective.clone())
            });
            write_sweep_manifest(&manifest)?;
            best_objective
        } else {
            for (trial, (status_label, _parsed, _error)) in manifest.trials.iter().zip(results) {
                trial_outputs.push(SweepObserveTrial {
                    trial_id: trial.trial_id.clone(),
                    index: trial.index,
                    variables: trial.variables.clone(),
                    job_id: trial.job_id.clone(),
                    status: status_label,
                    objective: None,
                    objective_error: None,
                });
            }
            None
        };

        let report = SweepObserveOutput {
            sweep_id: manifest.sweep_id.clone(),
            objective_configured,
            best_trial: manifest.best_trial.clone(),
            best_objective: best_objective.clone(),
            trials: trial_outputs,
        };
        match output_format {
            OutputFormat::Text => print_sweep_observe_output(&report, direction),
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .context("failed to serialize sweep observe output")?
                );
            }
        }

        if !watch {
            return Ok(());
        }
        // --watch --stop-when: stop the sweep when the condition is met.
        if let Some(expr) = stop_when.as_deref() {
            let best_value = best_objective
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok());
            if evaluate_stop_condition(expr, best_value)? {
                if output_format == OutputFormat::Text {
                    println!(
                        "{}",
                        term::styled_success(&format!(
                            "stop condition '{expr}' met; stopping sweep {}",
                            manifest.sweep_id
                        ))
                    );
                }
                let reason = format!("stop-when condition '{expr}' satisfied");
                let report = sweep_stop_inner(
                    &context,
                    sweep_id.as_deref(),
                    true,
                    Some(reason),
                    output_format == OutputFormat::Text,
                )?;
                if output_format == OutputFormat::Text {
                    print_sweep_stop_output(&report);
                }
                return Ok(());
            }
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            bail!("sweep observe --watch timed out before --stop-when was satisfied");
        }
        thread::sleep(poll_interval);
    }
}

fn print_sweep_observe_output(
    report: &SweepObserveOutput,
    direction: hpc_compose::spec::ObjectiveDirection,
) {
    println!("sweep: {}", report.sweep_id);
    if !report.objective_configured {
        println!(
            "{}",
            term::styled_warning("no sweep.objective configured; nothing to observe")
        );
        return;
    }
    println!(
        "direction: {}",
        match direction {
            hpc_compose::spec::ObjectiveDirection::Minimize => "minimize",
            hpc_compose::spec::ObjectiveDirection::Maximize => "maximize",
        }
    );
    if let Some(best) = &report.best_trial {
        let label = report.best_objective.as_deref().unwrap_or("?");
        println!("best: {} (objective={})", best, label);
    }
    // Rank: best first.
    let mut ranked = report.trials.clone();
    ranked.sort_by(|a, b| {
        let av = a.objective.as_deref().and_then(|s| s.parse::<f64>().ok());
        let bv = b.objective.as_deref().and_then(|s| s.parse::<f64>().ok());
        match (av, bv) {
            (Some(a), Some(b)) => match direction {
                hpc_compose::spec::ObjectiveDirection::Minimize => a.partial_cmp(&b),
                hpc_compose::spec::ObjectiveDirection::Maximize => b.partial_cmp(&a),
            }
            .unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    for trial in &ranked {
        let objective = trial.objective.as_deref().unwrap_or("-");
        println!(
            "  {} status={} objective={} {}",
            trial.trial_id,
            trial.status,
            objective,
            format_sweep_variables(&trial.variables)
        );
        if let Some(error) = &trial.objective_error {
            println!("    objective_error: {error}");
        }
    }
}

pub(crate) fn sweep_stop(
    context: ResolvedContext,
    sweep_id: Option<String>,
    yes: bool,
    reason: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    let report = sweep_stop_inner(
        &context,
        sweep_id.as_deref(),
        yes,
        reason,
        output_format == OutputFormat::Text,
    )?;
    match output_format {
        OutputFormat::Text => print_sweep_stop_output(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize sweep stop output")?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SweepStopOutput {
    sweep_id: String,
    cancelled_count: usize,
    skipped_count: usize,
    cancelled_trials: Vec<String>,
    skipped_trials: Vec<String>,
    stopped_at: u64,
    stop_reason: String,
}

fn sweep_stop_inner(
    context: &ResolvedContext,
    sweep_id: Option<&str>,
    yes: bool,
    reason: Option<String>,
    print_confirmation_hint: bool,
) -> Result<SweepStopOutput> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let mut manifest = load_sweep_manifest(&context.compose_file.value, sweep_id)?;
    if !yes {
        if print_confirmation_hint {
            println!(
                "About to cancel all non-terminal trials of sweep {} ({}). Re-run with --yes to proceed.",
                manifest.sweep_id,
                manifest.trials.len()
            );
        }
        bail!("--yes not set; refusing to cancel sweep trials");
    }
    let mut cancelled = Vec::new();
    let mut skipped = Vec::new();
    for trial in &manifest.trials {
        let Some(job_id) = trial.job_id.as_deref() else {
            skipped.push(trial.trial_id.clone());
            continue;
        };
        let status = status_for_sweep_trial(&manifest, trial, &scheduler_options);
        let terminal = matches!(
            status.status.as_str(),
            "completed" | "failed" | "submit_failed"
        );
        if terminal {
            skipped.push(trial.trial_id.clone());
            continue;
        }
        match output::cancel_job(job_id, &context.binaries.scancel.value) {
            Ok(()) => cancelled.push(trial.trial_id.clone()),
            Err(err) => {
                skipped.push(trial.trial_id.clone());
                let _ = writeln!(
                    io::stderr(),
                    "warning: failed to cancel trial {} (job {}): {err}",
                    trial.trial_id,
                    job_id
                );
            }
        }
    }
    let now = unix_timestamp_now();
    manifest.stopped_at = Some(now);
    manifest.stop_reason = reason.or_else(|| Some("manual sweep stop".to_string()));
    let stop_reason = manifest
        .stop_reason
        .clone()
        .unwrap_or_else(|| "manual sweep stop".to_string());
    write_sweep_manifest(&manifest)?;
    Ok(SweepStopOutput {
        sweep_id: manifest.sweep_id,
        cancelled_count: cancelled.len(),
        skipped_count: skipped.len(),
        cancelled_trials: cancelled,
        skipped_trials: skipped,
        stopped_at: now,
        stop_reason,
    })
}

fn print_sweep_stop_output(report: &SweepStopOutput) {
    println!(
        "stopped sweep {}: {} trial(s) cancelled, {} skipped",
        report.sweep_id, report.cancelled_count, report.skipped_count
    );
}
