use super::*;
use crate::time_util::unix_timestamp_now;

const DEFAULT_SWEEP_MAX_TRIALS: usize = 100;

/// One per-config rollup row: the replicate objectives of a single parameter
/// config summarized as mean±std(n). Emitted in the `groups` field of sweep
/// status/observe/results output.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct SweepConfigGroup {
    config_key: String,
    variables: BTreeMap<String, String>,
    replicates: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    mean: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    std: Option<f64>,
    /// Number of replicates with an observed objective contributing to the rollup.
    n: usize,
}

/// One (config_key, variables, objective) sample feeding the rollup grouping.
struct TrialSample<'a> {
    config_key: &'a str,
    variables: &'a BTreeMap<String, String>,
    objective: Option<f64>,
}

/// Groups trial samples by `config_key` and rolls each group up into a
/// mean±std(n) row. Groups are sorted by `config_key` for stable output. Each
/// group's `variables` come from the first trial seen for that config.
fn build_config_groups(samples: &[TrialSample<'_>]) -> Vec<SweepConfigGroup> {
    let mut order: Vec<&str> = Vec::new();
    let mut variables_by_key: BTreeMap<&str, &BTreeMap<String, String>> = BTreeMap::new();
    let mut total_by_key: BTreeMap<&str, usize> = BTreeMap::new();
    let mut values_by_key: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for sample in samples {
        if !variables_by_key.contains_key(sample.config_key) {
            order.push(sample.config_key);
            variables_by_key.insert(sample.config_key, sample.variables);
        }
        *total_by_key.entry(sample.config_key).or_insert(0) += 1;
        if let Some(value) = sample.objective {
            values_by_key
                .entry(sample.config_key)
                .or_default()
                .push(value);
        } else {
            values_by_key.entry(sample.config_key).or_default();
        }
    }
    order.sort_unstable();
    order
        .into_iter()
        .map(|config_key| {
            let values = values_by_key.get(config_key).cloned().unwrap_or_default();
            let stats = hpc_compose::job::replicate_rollup(&values);
            SweepConfigGroup {
                config_key: config_key.to_string(),
                variables: variables_by_key
                    .get(config_key)
                    .map(|vars| (*vars).clone())
                    .unwrap_or_default(),
                replicates: total_by_key.get(config_key).copied().unwrap_or(0),
                mean: stats.map(|s| s.mean),
                std: stats.map(|s| s.std),
                n: stats.map(|s| s.n).unwrap_or(0),
            }
        })
        .collect()
}

/// Selects the best config group's representative trial id by the group MEAN
/// objective (not the single luckiest replicate), per the optimization
/// direction. Returns the lowest-id trial of the winning group.
fn best_config_trial_id(
    groups: &[SweepConfigGroup],
    trials_by_group: &BTreeMap<String, Vec<String>>,
    direction: hpc_compose::spec::ObjectiveDirection,
) -> Option<String> {
    let scored = groups
        .iter()
        .filter_map(|group| group.mean.map(|mean| (mean, group.config_key.as_str())));
    let winner =
        match direction {
            hpc_compose::spec::ObjectiveDirection::Minimize => scored
                .min_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
            hpc_compose::spec::ObjectiveDirection::Maximize => scored
                .max_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
        }
        .map(|(_, config_key)| config_key)?;
    trials_by_group
        .get(winner)
        .and_then(|ids| ids.iter().min())
        .cloned()
}

/// Returns the best config group's MEAN objective by direction. Used to report
/// the headline objective for a replicated sweep (mean of the winning config).
fn best_group_mean(
    groups: &[SweepConfigGroup],
    direction: hpc_compose::spec::ObjectiveDirection,
) -> Option<f64> {
    let means = groups.iter().filter_map(|group| group.mean);
    match direction {
        hpc_compose::spec::ObjectiveDirection::Minimize => {
            means.min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        hpc_compose::spec::ObjectiveDirection::Maximize => {
            means.max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepSubmitOutput<'a> {
    pub(crate) schema_version: u32,
    dry_run: bool,
    manifest_path: Option<PathBuf>,
    manifest: &'a SweepManifest,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStatusOutput {
    pub(crate) schema_version: u32,
    sweep_id: String,
    compose_file: PathBuf,
    submitted_at: u64,
    summary: BTreeMap<String, usize>,
    /// Per-config replicate rollup (mean±std(n) over the trial objectives of
    /// each parameter config). Omitted when no replicates are configured.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    groups: Vec<SweepConfigGroup>,
    trials: Vec<SweepStatusTrialOutput>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
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

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepListOutput {
    pub(crate) schema_version: u32,
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

    let output_format = output::resolve_output_format(format);
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
                config_key: trial.config_key.clone(),
                replicate: trial.replicate,
                seed: trial.seed.clone(),
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
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
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
            huggingface_cli_bin: context.huggingface_cli_bin.clone(),
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
        load::load_effective_config_with_interpolation_vars_cache_default_and_resource_profiles(
            &file,
            &vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let effective_config_yaml =
        output::effective_config_yaml(&effective_config, &context.secret_values())?;
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
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
        let summary = prepare_progress.run(format!("Preparing {}", trial.trial_id), || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep: false,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
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
                huggingface_cli_bin: context.huggingface_cli_bin.clone(),
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
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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
    // Batch the scheduler probe across every Slurm-backed trial (one squeue +
    // one gated sacct) instead of a probe pair per trial.
    let probes = batch_probe_sweep_trials(&manifest, &options);
    let mut summary = BTreeMap::new();
    let trials = manifest
        .trials
        .iter()
        .map(|trial| {
            let prefetched = trial
                .job_id
                .as_deref()
                .and_then(|job_id| probes.get(job_id).cloned());
            let output = status_for_sweep_trial_with(&manifest, trial, &options, prefetched);
            *summary.entry(output.status.clone()).or_insert(0) += 1;
            output
        })
        .collect::<Vec<_>>();
    // Roll up persisted objectives per config when the sweep used replicates.
    // Status does not re-parse objectives; it reuses the values `sweep observe`
    // recorded on the manifest.
    let groups = if manifest_uses_replicates(&manifest) {
        config_groups_from_trials(&manifest.trials).0
    } else {
        Vec::new()
    };
    let report = SweepStatusOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        sweep_id: manifest.sweep_id,
        compose_file: manifest.compose_file,
        submitted_at: manifest.submitted_at,
        summary,
        groups,
        trials,
    };
    match output::resolve_output_format(format) {
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

/// Collects the raw scheduler probe for every Slurm-backed trial with a job id
/// in one squeue + one gated sacct. Local trials probe no scheduler and are
/// excluded; missing/corrupt records are skipped (their per-trial snapshot
/// re-derives the error).
fn batch_probe_sweep_trials(
    manifest: &SweepManifest,
    options: &SchedulerOptions,
) -> BTreeMap<String, (SchedulerStatus, Option<QueueDiagnostics>)> {
    let job_ids = manifest
        .trials
        .iter()
        .filter_map(|trial| trial.job_id.as_deref())
        .filter(|job_id| {
            load_submission_record_optional(&manifest.compose_file, Some(job_id))
                .is_some_and(|record| record.backend == SubmissionBackend::Slurm)
        })
        .collect::<Vec<_>>();
    probe_scheduler_status_many(&job_ids, options)
}

fn status_for_sweep_trial(
    manifest: &SweepManifest,
    trial: &SweepManifestTrial,
    options: &SchedulerOptions,
) -> SweepStatusTrialOutput {
    status_for_sweep_trial_with(manifest, trial, options, None)
}

fn status_for_sweep_trial_with(
    manifest: &SweepManifest,
    trial: &SweepManifestTrial,
    options: &SchedulerOptions,
    prefetched: Option<(SchedulerStatus, Option<QueueDiagnostics>)>,
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
    match build_status_snapshot_with_status(
        &manifest.compose_file,
        Some(job_id),
        options,
        prefetched,
    ) {
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
    let scheduler_state = hpc_compose::job::JobState::parse(&snapshot.scheduler.state);
    if scheduler_state == hpc_compose::job::JobState::Pending {
        return "pending".to_string();
    }
    if scheduler_state == hpc_compose::job::JobState::Running {
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

/// Prints the per-config replicate rollup (mean±std(n)) section. No-op when no
/// groups are present (non-replicated sweeps).
fn print_sweep_config_groups(groups: &[SweepConfigGroup]) {
    if groups.is_empty() {
        return;
    }
    println!("replicate rollup (mean+/-std over n replicates per config):");
    for group in groups {
        let label = if group.config_key.is_empty() {
            "(no parameters)".to_string()
        } else {
            group.config_key.clone()
        };
        match (group.mean, group.std) {
            (Some(mean), Some(std)) => println!(
                "  {label}: mean={mean:.6} std={std:.6} n={} ({} replicate(s))",
                group.n, group.replicates
            ),
            _ => println!(
                "  {label}: no observed objective ({} replicate(s))",
                group.replicates
            ),
        }
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
    print_sweep_config_groups(&report.groups);
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
    match output::resolve_output_format(format) {
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
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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

/// Compiles the sweep objective's `log_pattern` once so callers can reuse the
/// regex across all trials (and poll iterations) instead of recompiling the same
/// immutable pattern per trial.
fn compile_objective_log_regex(manifest: &SweepManifest) -> Result<Option<regex::Regex>> {
    let Some(pattern) = manifest
        .objective
        .as_ref()
        .and_then(|objective| objective.log_pattern.as_ref())
    else {
        return Ok(None);
    };
    Ok(Some(regex::Regex::new(pattern).with_context(|| {
        format!("sweep.objective.log_pattern '{pattern}' is not a valid regex")
    })?))
}

/// Parses one trial's objective value from its tracked log or artifacts.
///
/// `log_regex` is the pre-compiled `objective.log_pattern` (see
/// [`compile_objective_log_regex`]); pass `None` when no log pattern is set.
///
/// Returns `Ok(Some(value))` on success, `Ok(None)` when the trial is not yet
/// terminal or has no parseable objective, and `Err` only on unexpected IO.
fn parse_trial_objective(
    trial: &SweepManifestTrial,
    manifest: &SweepManifest,
    options: &SchedulerOptions,
    log_regex: Option<&regex::Regex>,
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
    if objective.log_pattern.is_some() {
        let Some(re) = log_regex else {
            return Ok(None);
        };
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

/// Returns one trial's observed wall-clock runtime in seconds, mirroring the
/// terminal-only gating of [`parse_trial_objective`].
///
/// Returns `Ok(Some(seconds))` only when the trial is terminal and at least one
/// tracked service reports a `duration_seconds`; in that case the maximum across
/// services is used (the trial finishes when its longest service finishes).
/// Returns `Ok(None)` for trials with no job id, non-terminal trials, or trials
/// whose services report no duration. No runtime is ever fabricated.
fn sweep_runtime_seconds(
    trial: &SweepManifestTrial,
    manifest: &SweepManifest,
    options: &SchedulerOptions,
) -> Result<Option<u64>> {
    let Some(job_id) = trial.job_id.as_deref() else {
        return Ok(None);
    };
    let snapshot = build_status_snapshot(&manifest.compose_file, Some(job_id), options)?;
    if !snapshot.scheduler.terminal {
        return Ok(None);
    }
    Ok(snapshot
        .services
        .iter()
        .filter_map(|service| service.duration_seconds)
        .max())
}

/// Returns whether this sweep fanned out into replicates (any trial has a
/// non-zero replicate index). v2 manifests and `replicates: 1` sweeps return
/// `false`, keeping their output byte-identical to pre-#12 behavior.
fn manifest_uses_replicates(manifest: &SweepManifest) -> bool {
    manifest.trials.iter().any(|trial| trial.replicate > 0)
}

/// Builds the per-config rollup groups and a config_key -> trial ids index for
/// a set of manifest trials, using each trial's parsed `objective` value.
fn config_groups_from_trials(
    trials: &[SweepManifestTrial],
) -> (Vec<SweepConfigGroup>, BTreeMap<String, Vec<String>>) {
    let samples: Vec<TrialSample<'_>> = trials
        .iter()
        .map(|trial| TrialSample {
            config_key: trial.config_key.as_str(),
            variables: &trial.variables,
            objective: trial
                .objective
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok()),
        })
        .collect();
    let groups = build_config_groups(&samples);
    let mut trials_by_group: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for trial in trials {
        trials_by_group
            .entry(trial.config_key.clone())
            .or_default()
            .push(trial.trial_id.clone());
    }
    (groups, trials_by_group)
}

/// Selects the best trial id, ranking by the per-config-group MEAN objective
/// (never the single luckiest replicate). Returns the lowest-id trial of the
/// winning config group.
fn best_trial_id(
    trials: &[SweepManifestTrial],
    direction: hpc_compose::spec::ObjectiveDirection,
) -> Option<String> {
    let (groups, trials_by_group) = config_groups_from_trials(trials);
    best_config_trial_id(&groups, &trials_by_group, direction)
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

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepObserveOutput {
    pub(crate) schema_version: u32,
    sweep_id: String,
    objective_configured: bool,
    best_trial: Option<String>,
    best_objective: Option<String>,
    /// Per-config replicate rollup (mean±std(n)). The best trial is selected by
    /// the best group MEAN, not the single luckiest replicate. Omitted when the
    /// sweep did not use replicates.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    groups: Vec<SweepConfigGroup>,
    /// Post-hoc scaling report (objective vs `scaling_axis`). Present only when
    /// `--scaling` was requested and `sweep.objective.scaling_axis` is set;
    /// otherwise omitted so the default observe output is byte-unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    scaling: Option<ScalingReport>,
    trials: Vec<SweepObserveTrial>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct SweepObserveTrial {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    job_id: Option<String>,
    status: String,
    objective: Option<String>,
    objective_error: Option<String>,
}

/// Post-hoc descriptive scaling report: the per-config group objective means
/// plotted against a numeric sweep parameter (`scaling_axis`), summarized with a
/// log-log least-squares slope and speedup/efficiency relative to a baseline
/// group. Output-only (never persisted); see `sweep observe --scaling`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct ScalingReport {
    /// The sweep parameter used as the x-axis.
    axis: String,
    /// The objective optimization direction, echoed for interpretation.
    direction: String,
    /// One row per config group that has both a numeric axis value and a group
    /// mean objective, sorted ascending by axis value.
    points: Vec<ScalingPoint>,
    /// Least-squares slope of `ln(objective_mean)` vs `ln(axis_value)` over the
    /// points with positive axis and mean. `None` when fewer than two such
    /// points exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    loglog_slope: Option<f64>,
    /// The axis value of the baseline group (the smallest axis value with
    /// terminal runtime data) used for speedup/efficiency. `None` when no point
    /// has runtime.
    #[serde(skip_serializing_if = "Option::is_none")]
    baseline_axis: Option<f64>,
}

/// One config group's scaling sample: its axis value, group mean objective, and
/// observed max runtime, plus speedup/efficiency relative to the baseline group.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct ScalingPoint {
    /// The numeric `scaling_axis` value for this group.
    axis_value: f64,
    /// The config_key identifying the group.
    config_key: String,
    /// The group mean objective (mean over the group's terminal replicates).
    #[serde(skip_serializing_if = "Option::is_none")]
    objective_mean: Option<f64>,
    /// The maximum observed runtime (seconds) across the group's trials.
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_seconds_max: Option<u64>,
    /// `baseline_runtime / this_runtime`. `None` unless both this point and the
    /// baseline have runtime.
    #[serde(skip_serializing_if = "Option::is_none")]
    speedup: Option<f64>,
    /// `speedup * baseline_axis / axis_value` (parallel efficiency). `None`
    /// unless `speedup` is defined and `axis_value > 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    efficiency: Option<f64>,
    /// Number of replicates contributing to this group's mean.
    n: usize,
}

/// Builds the IO-free scaling report from the per-config groups and a
/// per-group max runtime map.
///
/// `axis` names the sweep parameter read from each group's `variables`. Groups
/// whose axis value does not parse as `f64`, or that have no group mean, are
/// excluded (no zero-fill). The baseline is the smallest-axis point that has
/// runtime data; speedup/efficiency are reported relative to it.
fn build_scaling_report(
    axis: &str,
    direction: hpc_compose::spec::ObjectiveDirection,
    groups: &[SweepConfigGroup],
    runtime_by_group: &BTreeMap<String, u64>,
) -> ScalingReport {
    let mut points: Vec<ScalingPoint> = groups
        .iter()
        .filter_map(|group| {
            let axis_value = group.variables.get(axis)?.parse::<f64>().ok()?;
            // A group contributes a point only if it has a mean objective; this
            // also excludes config groups with no terminal/observed objective.
            group.mean?;
            Some(ScalingPoint {
                axis_value,
                config_key: group.config_key.clone(),
                objective_mean: group.mean,
                runtime_seconds_max: runtime_by_group.get(&group.config_key).copied(),
                speedup: None,
                efficiency: None,
                n: group.n,
            })
        })
        .collect();
    points.sort_by(|a, b| {
        a.axis_value
            .partial_cmp(&b.axis_value)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Baseline: the smallest-axis point that has positive runtime data. A 0
    // baseline runtime would zero every speedup/efficiency; require > 0 to match
    // the per-point guard below, so absent/zero runtime renders as "-" instead.
    let baseline = points
        .iter()
        .filter(|p| p.runtime_seconds_max.is_some_and(|s| s > 0))
        .min_by(|a, b| {
            a.axis_value
                .partial_cmp(&b.axis_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|p| {
            (
                p.axis_value,
                p.runtime_seconds_max.expect("filtered to Some > 0"),
            )
        });

    if let Some((baseline_axis, baseline_runtime)) = baseline {
        for point in &mut points {
            if let Some(runtime) = point.runtime_seconds_max
                && runtime > 0
            {
                let speedup = baseline_runtime as f64 / runtime as f64;
                point.speedup = Some(speedup);
                if point.axis_value > 0.0 {
                    point.efficiency = Some(speedup * baseline_axis / point.axis_value);
                }
            }
        }
    }

    let loglog_slope = loglog_slope(
        points
            .iter()
            .filter_map(|p| p.objective_mean.map(|mean| (p.axis_value, mean))),
    );

    ScalingReport {
        axis: axis.to_string(),
        direction: match direction {
            hpc_compose::spec::ObjectiveDirection::Minimize => "minimize".to_string(),
            hpc_compose::spec::ObjectiveDirection::Maximize => "maximize".to_string(),
        },
        points,
        loglog_slope,
        baseline_axis: baseline.map(|(axis_value, _)| axis_value),
    }
}

/// Least-squares slope of `ln(y)` vs `ln(x)` over the `(x, y)` pairs with
/// `x > 0` and `y > 0`. Returns `None` when fewer than two usable points exist
/// or when all usable x-values are identical (zero variance).
fn loglog_slope(points: impl Iterator<Item = (f64, f64)>) -> Option<f64> {
    let logs: Vec<(f64, f64)> = points
        .filter(|(x, y)| *x > 0.0 && *y > 0.0)
        .map(|(x, y)| (x.ln(), y.ln()))
        .collect();
    if logs.len() < 2 {
        return None;
    }
    let n = logs.len() as f64;
    let sum_x: f64 = logs.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = logs.iter().map(|(_, y)| y).sum();
    let mean_x = sum_x / n;
    let mean_y = sum_y / n;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for (x, y) in &logs {
        let dx = x - mean_x;
        numerator += dx * (y - mean_y);
        denominator += dx * dx;
    }
    if denominator == 0.0 {
        return None;
    }
    Some(numerator / denominator)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sweep_observe(
    context: ResolvedContext,
    sweep_id: Option<String>,
    watch: bool,
    stop_when: Option<String>,
    poll_interval: Duration,
    timeout: Option<Duration>,
    format: Option<OutputFormat>,
    scaling: bool,
) -> Result<()> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let output_format = output::resolve_output_format(format);
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
        // Compile the objective regex once per poll, not once per trial.
        let objective_log_regex = compile_objective_log_regex(&manifest);
        let mut results: Vec<(String, Option<f64>, Option<String>)> = Vec::new();
        for trial in &manifest.trials {
            let status = status_for_sweep_trial(&manifest, trial, &scheduler_options);
            let status_label = status.status.clone();
            let (parsed, error) = match objective_log_regex.as_ref() {
                Ok(re) => {
                    match parse_trial_objective(trial, &manifest, &scheduler_options, re.as_ref()) {
                        Ok(Some(value)) => (Some(value), None),
                        Ok(None) => (None, None),
                        Err(err) => (None, Some(format!("{err:#}"))),
                    }
                }
                Err(err) => (None, Some(format!("{err:#}"))),
            };
            results.push((status_label, parsed, error));
        }

        let uses_replicates = manifest_uses_replicates(&manifest);
        let mut trial_outputs = Vec::new();
        let mut groups = Vec::new();
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
            // Best selection ranks on the per-config GROUP MEAN, never the single
            // luckiest replicate. `best_objective` likewise reports the winning
            // group's mean when replicates are used.
            let (computed_groups, _trials_by_group) = config_groups_from_trials(&manifest.trials);
            manifest.best_trial = best_trial_id(&manifest.trials, direction);
            let best_objective = if uses_replicates {
                best_group_mean(&computed_groups, direction).map(|mean| mean.to_string())
            } else {
                manifest.best_trial.as_ref().and_then(|id| {
                    manifest
                        .trials
                        .iter()
                        .find(|t| &t.trial_id == id)
                        .and_then(|t| t.objective.clone())
                })
            };
            if uses_replicates {
                groups = computed_groups;
            }
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

        // Post-hoc scaling report (objective vs scaling_axis). Output-only: it
        // is never persisted and is built only when requested and configured.
        let scaling_report = if scaling && objective_configured {
            if let Some(axis) = manifest
                .objective
                .as_ref()
                .and_then(|o| o.scaling_axis.clone())
            {
                // Per-group max runtime over terminal trials only (no fabrication).
                let mut runtime_by_group: BTreeMap<String, u64> = BTreeMap::new();
                for trial in &manifest.trials {
                    if let Some(seconds) =
                        sweep_runtime_seconds(trial, &manifest, &scheduler_options)?
                    {
                        runtime_by_group
                            .entry(trial.config_key.clone())
                            .and_modify(|max| *max = (*max).max(seconds))
                            .or_insert(seconds);
                    }
                }
                let (scaling_groups, _trials_by_group) =
                    config_groups_from_trials(&manifest.trials);
                Some(build_scaling_report(
                    &axis,
                    direction,
                    &scaling_groups,
                    &runtime_by_group,
                ))
            } else {
                None
            }
        } else {
            None
        };

        let report = SweepObserveOutput {
            schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
            sweep_id: manifest.sweep_id.clone(),
            objective_configured,
            best_trial: manifest.best_trial.clone(),
            best_objective: best_objective.clone(),
            groups,
            scaling: scaling_report,
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
                let report = sweep_stop_inner(&context, sweep_id.as_deref(), true, Some(reason))?;
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
        if report.groups.is_empty() {
            println!("best: {} (objective={})", best, label);
        } else {
            // With replicates, the headline objective is the winning config's
            // group mean, and `best` is that group's representative trial.
            println!("best config: {} (mean objective={})", best, label);
        }
    }
    print_sweep_config_groups(&report.groups);
    if let Some(scaling) = &report.scaling {
        print_scaling_report(scaling);
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

fn print_scaling_report(report: &ScalingReport) {
    println!(
        "scaling ({} objective vs {}):",
        report.direction, report.axis
    );
    if report.points.is_empty() {
        println!(
            "{}",
            term::styled_warning(
                "  no terminal trials with a numeric scaling_axis value and an observed objective"
            )
        );
        return;
    }
    if let Some(baseline) = report.baseline_axis {
        println!("  baseline {}={}", report.axis, format_axis_value(baseline));
    }
    for point in &report.points {
        let mean = point
            .objective_mean
            .map(|m| format!("{m:.6}"))
            .unwrap_or_else(|| "-".to_string());
        let runtime = point
            .runtime_seconds_max
            .map(|s| format!("{s}s"))
            .unwrap_or_else(|| "-".to_string());
        let speedup = point
            .speedup
            .map(|s| format!("{s:.3}x"))
            .unwrap_or_else(|| "-".to_string());
        let efficiency = point
            .efficiency
            .map(|e| format!("{:.1}%", e * 100.0))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  {}={} mean={} runtime={} speedup={} efficiency={} (n={})",
            report.axis,
            format_axis_value(point.axis_value),
            mean,
            runtime,
            speedup,
            efficiency,
            point.n
        );
    }
    match report.loglog_slope {
        Some(slope) => println!("  log-log slope (objective vs {}): {slope:.4}", report.axis),
        None => println!(
            "  log-log slope: insufficient positive points (need >= 2 with axis>0 and objective>0)"
        ),
    }
}

/// Formats a numeric axis value compactly: integral values print without a
/// trailing `.0` (e.g. `nodes=4`), fractional values keep their precision.
fn format_axis_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

pub(crate) fn sweep_stop(
    context: ResolvedContext,
    sweep_id: Option<String>,
    yes: bool,
    reason: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let output_format = output::resolve_output_format(format);
    let report = sweep_stop_inner(&context, sweep_id.as_deref(), yes, reason)?;
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

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStopOutput {
    pub(crate) schema_version: u32,
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
) -> Result<SweepStopOutput> {
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let mut manifest = load_sweep_manifest(&context.compose_file.value, sweep_id)?;
    crate::commands::confirm::confirm_destructive_action(
        &format!(
            "cancel {} sweep trials for sweep {}",
            manifest.trials.len(),
            manifest.sweep_id
        ),
        yes,
    )?;
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
        match crate::job::cancel_job(job_id, &context.binaries.scancel.value) {
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
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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

// --- sweep results / score --sweep / stats --sweep -------------------------

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepResultsOutput {
    pub(crate) schema_version: u32,
    sweep_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    objective_direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_trial: Option<String>,
    variable_columns: Vec<String>,
    /// Per-config replicate rollup (mean±std(n)). The best trial is selected by
    /// the best group MEAN. Omitted when the sweep did not use replicates.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    groups: Vec<SweepConfigGroup>,
    rows: Vec<SweepResultRow>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SweepResultRow {
    trial_id: String,
    index: usize,
    variables: BTreeMap<String, String>,
    /// Stable key grouping replicates of the same parameter config.
    #[serde(skip_serializing_if = "String::is_empty")]
    config_key: String,
    /// Zero-based replicate index within this config.
    replicate: u32,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduler_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    objective: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    objective_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    energy_kwh: Option<f64>,
}

/// Parses `--include` tokens into (want_score, want_energy); rejects unknowns.
fn parse_sweep_include(include: &[String]) -> Result<(bool, bool)> {
    let mut want_score = false;
    let mut want_energy = false;
    for token in include {
        match token.trim() {
            "" => {}
            "score" => want_score = true,
            "energy" => want_energy = true,
            other => bail!("unknown --include value '{other}'; valid values are: score, energy"),
        }
    }
    Ok((want_score, want_energy))
}

/// Sorted union of variable keys across all trials, used as stable columns.
fn sweep_variable_columns<'a>(
    variable_maps: impl Iterator<Item = &'a BTreeMap<String, String>>,
) -> Vec<String> {
    let mut columns: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for map in variable_maps {
        columns.extend(map.keys().cloned());
    }
    columns.into_iter().collect()
}

fn manifest_trial_interpolation_vars(
    base: &BTreeMap<String, String>,
    sweep_id: &str,
    trial: &SweepManifestTrial,
) -> BTreeMap<String, String> {
    let mut vars = base.clone();
    vars.extend(trial.variables.clone());
    vars.insert("HPC_COMPOSE_SWEEP_ID".to_string(), sweep_id.to_string());
    vars.insert(
        "HPC_COMPOSE_SWEEP_TRIAL".to_string(),
        trial.trial_id.clone(),
    );
    vars.insert(
        "HPC_COMPOSE_SWEEP_TRIAL_INDEX".to_string(),
        trial.index.to_string(),
    );
    vars.insert(
        "HPC_COMPOSE_SWEEP_REPLICATE".to_string(),
        trial.replicate.to_string(),
    );
    if let Some(seed) = &trial.seed {
        vars.insert("HPC_COMPOSE_SWEEP_SEED".to_string(), seed.clone());
    }
    vars
}

/// Loads the per-trial efficiency report (resolves the tracked record + runtime
/// plan, then builds the score). Static read of tracked state plus the same
/// terminal-only accounting probe `score` performs.
fn trial_efficiency_report(
    context: &ResolvedContext,
    manifest: &SweepManifest,
    trial: &SweepManifestTrial,
    options: &EfficiencyScoreOptions,
) -> Result<hpc_compose::job::EfficiencyScoreReport> {
    let job_id = trial
        .job_id
        .as_deref()
        .context("trial has no recorded job id")?;
    let record = resolve_tracked_record(context, Some(job_id))?
        .with_context(|| format!("no tracked record for trial job {job_id}"))?;
    let vars =
        manifest_trial_interpolation_vars(&context.interpolation_vars, &manifest.sweep_id, trial);
    let plan = load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
        &record.compose_file,
        &vars,
        Some(&context.cache_dir.value),
        &context.resource_profiles,
    )?;
    build_efficiency_score_report(&plan, &record, options)
}

/// Parses and prints one tidy row per trial (read-only; never writes back to
/// the manifest, unlike `sweep observe`).
pub(crate) fn sweep_results(
    context: ResolvedContext,
    sweep_id: Option<String>,
    format: Option<SweepResultsFormat>,
    include: Vec<String>,
) -> Result<()> {
    let (want_score, want_energy) = parse_sweep_include(&include)?;
    let manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
    let scheduler = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let direction = manifest
        .objective
        .as_ref()
        .map(|objective| objective.direction);
    let variable_columns = sweep_variable_columns(manifest.trials.iter().map(|t| &t.variables));
    let score_options = EfficiencyScoreOptions {
        scheduler: SchedulerOptions {
            squeue_bin: context.binaries.squeue.value.clone(),
            sacct_bin: context.binaries.sacct.value.clone(),
        },
        sstat_bin: context.binaries.sstat.value.clone(),
        ..EfficiencyScoreOptions::default()
    };

    let uses_replicates = manifest_uses_replicates(&manifest);
    let mut rows = Vec::new();
    // Track parsed objectives per trial so best selection can rank on the
    // per-config GROUP MEAN (not the single luckiest replicate).
    let mut parsed_by_trial: BTreeMap<String, Option<f64>> = BTreeMap::new();
    // Compile the objective regex once, not once per trial.
    let objective_log_regex = compile_objective_log_regex(&manifest);
    for trial in &manifest.trials {
        let status = status_for_sweep_trial(&manifest, trial, &scheduler);
        let (parsed, objective, objective_error) = match objective_log_regex.as_ref() {
            Ok(re) => match parse_trial_objective(trial, &manifest, &scheduler, re.as_ref()) {
                Ok(Some(value)) => (Some(value), Some(value.to_string()), None),
                Ok(None) => (None, None, None),
                Err(err) => (None, None, Some(format!("{err:#}"))),
            },
            Err(err) => (None, None, Some(format!("{err:#}"))),
        };
        parsed_by_trial.insert(trial.trial_id.clone(), parsed);
        let (score, energy_kwh) = if want_score || want_energy {
            match trial_efficiency_report(&context, &manifest, trial, &score_options) {
                Ok(report) => (
                    want_score.then_some(report.score),
                    if want_energy { report.energy_kwh } else { None },
                ),
                Err(_) => (None, None),
            }
        } else {
            (None, None)
        };
        rows.push(SweepResultRow {
            trial_id: trial.trial_id.clone(),
            index: trial.index,
            variables: trial.variables.clone(),
            config_key: trial.config_key.clone(),
            replicate: trial.replicate,
            status: status.status,
            scheduler_state: status.scheduler_state,
            objective,
            objective_error,
            score,
            energy_kwh,
        });
    }

    // Group the freshly parsed objectives by config for the rollup and for
    // group-mean best selection.
    let samples: Vec<TrialSample<'_>> = manifest
        .trials
        .iter()
        .map(|trial| TrialSample {
            config_key: trial.config_key.as_str(),
            variables: &trial.variables,
            objective: parsed_by_trial.get(&trial.trial_id).copied().flatten(),
        })
        .collect();
    let groups = build_config_groups(&samples);
    let mut trials_by_group: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for trial in &manifest.trials {
        trials_by_group
            .entry(trial.config_key.clone())
            .or_default()
            .push(trial.trial_id.clone());
    }
    let best_trial =
        direction.and_then(|direction| best_config_trial_id(&groups, &trials_by_group, direction));

    let output = SweepResultsOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        sweep_id: manifest.sweep_id,
        objective_direction: direction.map(|direction| match direction {
            hpc_compose::spec::ObjectiveDirection::Minimize => "minimize".to_string(),
            hpc_compose::spec::ObjectiveDirection::Maximize => "maximize".to_string(),
        }),
        best_trial,
        variable_columns,
        groups: if uses_replicates { groups } else { Vec::new() },
        rows,
    };

    match output::resolve_sweep_results_format(format) {
        SweepResultsFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize sweep results output")?
            );
        }
        SweepResultsFormat::Csv => println!("{}", sweep_results_csv(&output)),
        SweepResultsFormat::Text => print_sweep_results_output(&output),
    }
    Ok(())
}

fn sweep_results_csv(output: &SweepResultsOutput) -> String {
    let has_score = output.rows.iter().any(|row| row.score.is_some());
    let has_energy = output.rows.iter().any(|row| row.energy_kwh.is_some());
    // Only surface replicate columns when the sweep actually fanned out, so
    // non-replicated sweeps keep their existing CSV header byte-identical.
    let has_replicates = output.rows.iter().any(|row| row.replicate > 0);
    let mut header = vec!["trial_id".to_string(), "index".to_string()];
    if has_replicates {
        header.push("config_key".to_string());
        header.push("replicate".to_string());
    }
    header.extend(output.variable_columns.iter().cloned());
    header.push("status".to_string());
    header.push("objective".to_string());
    if has_score {
        header.push("score".to_string());
    }
    if has_energy {
        header.push("energy_kwh".to_string());
    }
    let mut lines = vec![
        header
            .iter()
            .map(|field| output::csv_field(field))
            .collect::<Vec<_>>()
            .join(","),
    ];
    for row in &output.rows {
        let mut fields = vec![
            output::csv_field(&row.trial_id),
            output::csv_field(&row.index.to_string()),
        ];
        if has_replicates {
            fields.push(output::csv_field(&row.config_key));
            fields.push(output::csv_field(&row.replicate.to_string()));
        }
        for column in &output.variable_columns {
            fields.push(output::csv_field(
                row.variables.get(column).map(String::as_str).unwrap_or(""),
            ));
        }
        fields.push(output::csv_field(&row.status));
        fields.push(output::csv_field(row.objective.as_deref().unwrap_or("")));
        if has_score {
            fields.push(output::csv_field(
                &row.score.map(|score| score.to_string()).unwrap_or_default(),
            ));
        }
        if has_energy {
            fields.push(output::csv_field(
                &row.energy_kwh
                    .map(|energy| format!("{energy:.6}"))
                    .unwrap_or_default(),
            ));
        }
        lines.push(fields.join(","));
    }
    lines.join("\n")
}

fn print_sweep_results_output(output: &SweepResultsOutput) {
    println!("sweep {}", output.sweep_id);
    if let Some(best) = &output.best_trial {
        println!("best trial: {best}");
    }
    print_sweep_config_groups(&output.groups);
    for row in &output.rows {
        let vars = output
            .variable_columns
            .iter()
            .map(|column| {
                format!(
                    "{column}={}",
                    row.variables.get(column).map(String::as_str).unwrap_or("")
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let mut line = format!("{}  {}  {vars}", row.trial_id, row.status);
        if let Some(objective) = &row.objective {
            line.push_str(&format!("  objective={objective}"));
        }
        if let Some(score) = row.score {
            line.push_str(&format!("  score={score}"));
        }
        if let Some(energy) = row.energy_kwh {
            line.push_str(&format!("  energy_kwh={energy:.4}"));
        }
        println!("{line}");
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepScoreOutput {
    pub(crate) schema_version: u32,
    sweep_id: String,
    /// Per-config rollup of the efficiency score (mean±std(n)) when the sweep
    /// used replicates. Omitted otherwise.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    groups: Vec<SweepConfigGroup>,
    trials: Vec<SweepScoreTrial>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SweepScoreTrial {
    trial_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    config_key: String,
    replicate: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<hpc_compose::job::EfficiencyScoreReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Per-trial efficiency score collection over a sweep (read-only).
pub(crate) fn score_sweep(
    context: ResolvedContext,
    sweep_id: Option<String>,
    format: Option<OutputFormat>,
    pue: f64,
    gpu_tdp_w: f64,
    cpu_watts_per_core: f64,
) -> Result<()> {
    let manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
    let options = EfficiencyScoreOptions {
        scheduler: SchedulerOptions {
            squeue_bin: context.binaries.squeue.value.clone(),
            sacct_bin: context.binaries.sacct.value.clone(),
        },
        sstat_bin: context.binaries.sstat.value.clone(),
        pue,
        gpu_tdp_w,
        cpu_watts_per_core,
    };
    let uses_replicates = manifest_uses_replicates(&manifest);
    let trials: Vec<SweepScoreTrial> = manifest
        .trials
        .iter()
        .map(|trial| {
            let (report, error) =
                match trial_efficiency_report(&context, &manifest, trial, &options) {
                    Ok(report) => (Some(report), None),
                    Err(err) => (None, Some(format!("{err:#}"))),
                };
            SweepScoreTrial {
                trial_id: trial.trial_id.clone(),
                config_key: trial.config_key.clone(),
                replicate: trial.replicate,
                job_id: trial.job_id.clone(),
                report,
                error,
            }
        })
        .collect();
    // When the sweep used replicates, roll the efficiency score up per config.
    let groups = if uses_replicates {
        let samples: Vec<TrialSample<'_>> = trials
            .iter()
            .zip(&manifest.trials)
            .map(|(scored, trial)| TrialSample {
                config_key: scored.config_key.as_str(),
                variables: &trial.variables,
                objective: scored.report.as_ref().map(|report| f64::from(report.score)),
            })
            .collect();
        build_config_groups(&samples)
    } else {
        Vec::new()
    };
    let output = SweepScoreOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        sweep_id: manifest.sweep_id,
        groups,
        trials,
    };
    match output::resolve_output_format(format) {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&output)
                .context("failed to serialize sweep score output")?
        ),
        OutputFormat::Text => {
            println!("sweep {}", output.sweep_id);
            print_sweep_config_groups(&output.groups);
            for trial in &output.trials {
                match (&trial.report, &trial.error) {
                    (Some(report), _) => println!(
                        "{}  score={}  grade={}",
                        trial.trial_id, report.score, report.grade
                    ),
                    (None, Some(error)) => println!("{}  error: {error}", trial.trial_id),
                    (None, None) => println!("{}  (no score)", trial.trial_id),
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct SweepStatsOutput {
    pub(crate) schema_version: u32,
    sweep_id: String,
    trials: Vec<SweepStatsTrial>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SweepStatsTrial {
    trial_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    config_key: String,
    replicate: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<hpc_compose::job::StatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Per-trial runtime metrics/step-stats collection over a sweep (read-only).
pub(crate) fn stats_sweep(
    context: ResolvedContext,
    sweep_id: Option<String>,
    format: Option<StatsOutputFormat>,
    accounting: bool,
) -> Result<()> {
    let manifest = load_sweep_manifest(&context.compose_file.value, sweep_id.as_deref())?;
    let options = StatsOptions {
        scheduler: SchedulerOptions {
            squeue_bin: context.binaries.squeue.value.clone(),
            sacct_bin: context.binaries.sacct.value.clone(),
        },
        sstat_bin: context.binaries.sstat.value.clone(),
        accounting,
    };
    // Batch the scheduler probe across every Slurm-backed trial (one squeue +
    // one gated sacct); sstat is still probed per trial by build_stats_snapshot.
    let probes = batch_probe_sweep_trials(&manifest, &options.scheduler);
    let trials = manifest
        .trials
        .iter()
        .map(|trial| {
            let (snapshot, error) = match trial.job_id.as_deref() {
                Some(job_id) => {
                    let prefetched = probes.get(job_id).map(|(status, _)| status.clone());
                    match build_stats_snapshot_with_status(
                        &manifest.compose_file,
                        Some(job_id),
                        &options,
                        prefetched,
                    ) {
                        Ok(snapshot) => (Some(snapshot), None),
                        Err(err) => (None, Some(format!("{err:#}"))),
                    }
                }
                None => (None, Some("trial has no recorded job id".to_string())),
            };
            SweepStatsTrial {
                trial_id: trial.trial_id.clone(),
                config_key: trial.config_key.clone(),
                replicate: trial.replicate,
                job_id: trial.job_id.clone(),
                snapshot,
                error,
            }
        })
        .collect();
    let output = SweepStatsOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        sweep_id: manifest.sweep_id,
        trials,
    };
    // The collection is naturally a document; emit JSON for json/csv/jsonl and a
    // compact per-trial summary for text.
    match format.unwrap_or(StatsOutputFormat::Text) {
        StatsOutputFormat::Text => {
            println!("sweep {}", output.sweep_id);
            for trial in &output.trials {
                match &trial.error {
                    Some(error) => println!("{}  error: {error}", trial.trial_id),
                    None => println!(
                        "{}  job={}",
                        trial.trial_id,
                        trial.job_id.as_deref().unwrap_or("-")
                    ),
                }
            }
        }
        _ => println!(
            "{}",
            serde_json::to_string_pretty(&output)
                .context("failed to serialize sweep stats output")?
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn manifest_trial_interpolation_vars_overlay_trial_and_reserved_values() {
        let base = vars(&[("LR", "base"), ("KEEP", "yes")]);
        let trial = SweepManifestTrial {
            trial_id: "t000r1".into(),
            index: 3,
            variables: vars(&[("LR", "0.2"), ("BATCH", "64")]),
            config_key: "BATCH=64;LR=0.2".into(),
            replicate: 1,
            seed: Some("seed-1".into()),
            script_path: PathBuf::from("t000r1.sbatch"),
            job_id: None,
            record_path: None,
            submitted_at: None,
            submit_error: None,
            objective: None,
            objective_error: None,
            observed_at: None,
        };

        let resolved = manifest_trial_interpolation_vars(&base, "sweep-123", &trial);

        assert_eq!(resolved.get("KEEP").map(String::as_str), Some("yes"));
        assert_eq!(resolved.get("LR").map(String::as_str), Some("0.2"));
        assert_eq!(resolved.get("BATCH").map(String::as_str), Some("64"));
        assert_eq!(
            resolved.get("HPC_COMPOSE_SWEEP_ID").map(String::as_str),
            Some("sweep-123")
        );
        assert_eq!(
            resolved.get("HPC_COMPOSE_SWEEP_TRIAL").map(String::as_str),
            Some("t000r1")
        );
        assert_eq!(
            resolved
                .get("HPC_COMPOSE_SWEEP_TRIAL_INDEX")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            resolved
                .get("HPC_COMPOSE_SWEEP_REPLICATE")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            resolved.get("HPC_COMPOSE_SWEEP_SEED").map(String::as_str),
            Some("seed-1")
        );
    }

    #[test]
    fn parse_sweep_include_accepts_known_and_rejects_unknown() {
        assert_eq!(
            parse_sweep_include(&["score".into(), "energy".into()]).unwrap(),
            (true, true)
        );
        assert_eq!(parse_sweep_include(&[]).unwrap(), (false, false));
        assert!(parse_sweep_include(&["bogus".into()]).is_err());
    }

    #[test]
    fn sweep_variable_columns_is_sorted_union() {
        let a = vars(&[("lr", "0.1"), ("bs", "32")]);
        let b = vars(&[("lr", "0.2"), ("wd", "0.01")]);
        let columns = sweep_variable_columns([&a, &b].into_iter());
        assert_eq!(columns, vec!["bs", "lr", "wd"]);
    }

    fn result_row(
        trial_id: &str,
        config_key: &str,
        replicate: u32,
        objective: &str,
    ) -> SweepResultRow {
        SweepResultRow {
            trial_id: trial_id.to_string(),
            index: 0,
            variables: vars(&[("lr", "0.1")]),
            config_key: config_key.to_string(),
            replicate,
            status: "completed".to_string(),
            scheduler_state: Some("COMPLETED".to_string()),
            objective: Some(objective.to_string()),
            objective_error: None,
            score: None,
            energy_kwh: None,
        }
    }

    #[test]
    fn sweep_results_csv_quotes_and_orders_columns() {
        let output = SweepResultsOutput {
            schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
            sweep_id: "s1".to_string(),
            objective_direction: Some("minimize".to_string()),
            best_trial: Some("t000".to_string()),
            variable_columns: vec!["lr".to_string(), "note".to_string()],
            groups: Vec::new(),
            rows: vec![SweepResultRow {
                trial_id: "t000".to_string(),
                index: 0,
                variables: vars(&[("lr", "0.1"), ("note", "a,b\"c")]),
                config_key: String::new(),
                replicate: 0,
                status: "completed".to_string(),
                scheduler_state: Some("COMPLETED".to_string()),
                objective: Some("0.05".to_string()),
                objective_error: None,
                score: None,
                energy_kwh: None,
            }],
        };
        let csv = sweep_results_csv(&output);
        let mut lines = csv.lines();
        // Non-replicated sweep: header stays byte-identical to pre-#12 output.
        assert_eq!(
            lines.next().unwrap(),
            "\"trial_id\",\"index\",\"lr\",\"note\",\"status\",\"objective\""
        );
        // The comma/quote-containing value is escaped per RFC4180.
        assert_eq!(
            lines.next().unwrap(),
            "\"t000\",\"0\",\"0.1\",\"a,b\"\"c\",\"completed\",\"0.05\""
        );
    }

    #[test]
    fn sweep_results_csv_adds_replicate_columns_when_fanned_out() {
        let output = SweepResultsOutput {
            schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
            sweep_id: "s1".to_string(),
            objective_direction: Some("minimize".to_string()),
            best_trial: Some("t000r0".to_string()),
            variable_columns: vec!["lr".to_string()],
            groups: Vec::new(),
            rows: vec![
                result_row("t000r0", "lr=0.1", 0, "0.05"),
                result_row("t000r1", "lr=0.1", 1, "0.07"),
            ],
        };
        let csv = sweep_results_csv(&output);
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "\"trial_id\",\"index\",\"config_key\",\"replicate\",\"lr\",\"status\",\"objective\""
        );
        assert_eq!(
            lines.next().unwrap(),
            "\"t000r0\",\"0\",\"lr=0.1\",\"0\",\"0.1\",\"completed\",\"0.05\""
        );
        assert_eq!(
            lines.next().unwrap(),
            "\"t000r1\",\"0\",\"lr=0.1\",\"1\",\"0.1\",\"completed\",\"0.07\""
        );
    }

    fn manifest_trial(
        trial_id: &str,
        config_key: &str,
        replicate: u32,
        objective: Option<&str>,
    ) -> SweepManifestTrial {
        SweepManifestTrial {
            trial_id: trial_id.to_string(),
            index: 0,
            variables: vars(&[("lr", "0.1")]),
            config_key: config_key.to_string(),
            replicate,
            seed: Some("seed".to_string()),
            script_path: PathBuf::from(format!("/tmp/{trial_id}.sbatch")),
            job_id: Some("1".to_string()),
            record_path: None,
            submitted_at: None,
            submit_error: None,
            objective: objective.map(str::to_string),
            objective_error: None,
            observed_at: None,
        }
    }

    #[test]
    fn best_trial_ranks_on_group_mean_not_luckiest_replicate() {
        // Config A: one lucky low replicate (0.01) but a worse MEAN than config B.
        //   A objectives: 0.01, 0.50 -> mean 0.255
        //   B objectives: 0.20, 0.20 -> mean 0.200
        // Minimize: config B wins on mean, even though A has the single best run.
        let trials = vec![
            manifest_trial("t000r0", "cfg=a", 0, Some("0.01")),
            manifest_trial("t000r1", "cfg=a", 1, Some("0.50")),
            manifest_trial("t001r0", "cfg=b", 0, Some("0.20")),
            manifest_trial("t001r1", "cfg=b", 1, Some("0.20")),
        ];
        let best = best_trial_id(&trials, hpc_compose::spec::ObjectiveDirection::Minimize)
            .expect("a best trial");
        // The representative trial of the winning group (config B) is returned,
        // never the lucky single replicate t000r0.
        assert_eq!(best, "t001r0");
        assert_ne!(best, "t000r0");

        // Maximize: the winner flips to the higher-mean config A.
        let best_max = best_trial_id(&trials, hpc_compose::spec::ObjectiveDirection::Maximize)
            .expect("a best trial");
        assert_eq!(best_max, "t000r0");
    }

    #[test]
    fn config_groups_roll_up_mean_std_per_config() {
        let trials = vec![
            manifest_trial("t000r0", "cfg=a", 0, Some("1.0")),
            manifest_trial("t000r1", "cfg=a", 1, Some("3.0")),
            manifest_trial("t001r0", "cfg=b", 0, Some("10.0")),
        ];
        let (groups, _) = config_groups_from_trials(&trials);
        assert_eq!(groups.len(), 2);
        let a = groups
            .iter()
            .find(|g| g.config_key == "cfg=a")
            .expect("group a");
        assert_eq!(a.mean, Some(2.0));
        assert_eq!(a.std, Some(1.0));
        assert_eq!(a.n, 2);
        assert_eq!(a.replicates, 2);
        let b = groups
            .iter()
            .find(|g| g.config_key == "cfg=b")
            .expect("group b");
        assert_eq!(b.mean, Some(10.0));
        assert_eq!(b.std, Some(0.0));
        assert_eq!(b.n, 1);
    }

    #[test]
    fn manifest_uses_replicates_detects_fan_out() {
        let single = vec![manifest_trial("t000", "cfg=a", 0, Some("1.0"))];
        assert!(!manifest_uses_replicates(&SweepManifest {
            schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
            sweep_id: "s".into(),
            compose_file: PathBuf::from("/tmp/c.yaml"),
            submitted_at: 0,
            matrix: "full".into(),
            seed: None,
            total_combinations: 1,
            objective: None,
            best_trial: None,
            stopped_at: None,
            stop_reason: None,
            trials: single,
        }));
        let fanned = vec![
            manifest_trial("t000r0", "cfg=a", 0, Some("1.0")),
            manifest_trial("t000r1", "cfg=a", 1, Some("2.0")),
        ];
        assert!(manifest_uses_replicates(&SweepManifest {
            schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
            sweep_id: "s".into(),
            compose_file: PathBuf::from("/tmp/c.yaml"),
            submitted_at: 0,
            matrix: "full".into(),
            seed: None,
            total_combinations: 1,
            objective: None,
            best_trial: None,
            stopped_at: None,
            stop_reason: None,
            trials: fanned,
        }));
    }

    fn scaling_group(
        config_key: &str,
        axis: &str,
        axis_value: &str,
        mean: Option<f64>,
        n: usize,
    ) -> SweepConfigGroup {
        SweepConfigGroup {
            config_key: config_key.to_string(),
            variables: vars(&[(axis, axis_value)]),
            replicates: n.max(1),
            mean,
            std: mean.map(|_| 0.0),
            n,
        }
    }

    #[test]
    fn loglog_slope_recovers_known_power_law() {
        // y = 2 * x^-1 -> log-log slope is exactly -1 over clean points.
        let points = [(1.0, 2.0), (2.0, 1.0), (4.0, 0.5), (8.0, 0.25)];
        let slope = loglog_slope(points.into_iter()).expect("slope");
        assert!((slope + 1.0).abs() < 1e-9, "expected ~-1, got {slope}");
    }

    #[test]
    fn loglog_slope_needs_two_positive_points() {
        // A single usable point yields no slope.
        assert!(loglog_slope([(1.0, 2.0)].into_iter()).is_none());
        // Non-positive axis/objective values are excluded before fitting.
        assert!(loglog_slope([(0.0, 2.0), (-1.0, 3.0), (4.0, 0.0)].into_iter()).is_none());
    }

    #[test]
    fn build_scaling_report_computes_baseline_speedup_and_efficiency() {
        let groups = vec![
            scaling_group("nodes=1", "nodes", "1", Some(8.0), 1),
            scaling_group("nodes=2", "nodes", "2", Some(4.0), 1),
            scaling_group("nodes=4", "nodes", "4", Some(2.0), 1),
        ];
        let runtime = BTreeMap::from([
            ("nodes=1".to_string(), 100_u64),
            ("nodes=2".to_string(), 50_u64),
            ("nodes=4".to_string(), 25_u64),
        ]);
        let report = build_scaling_report(
            "nodes",
            hpc_compose::spec::ObjectiveDirection::Minimize,
            &groups,
            &runtime,
        );
        assert_eq!(report.axis, "nodes");
        assert_eq!(report.baseline_axis, Some(1.0));
        // Points are sorted ascending by axis value.
        let axes: Vec<f64> = report.points.iter().map(|p| p.axis_value).collect();
        assert_eq!(axes, vec![1.0, 2.0, 4.0]);
        // Ideal strong scaling -> speedup == nodes, efficiency == 1.0.
        let four = report
            .points
            .iter()
            .find(|p| p.axis_value == 4.0)
            .expect("nodes=4 point");
        assert!((four.speedup.expect("speedup") - 4.0).abs() < 1e-9);
        assert!((four.efficiency.expect("efficiency") - 1.0).abs() < 1e-9);
        // objective 8,4,2 vs nodes 1,2,4 is y = 8 * x^-1 -> slope -1.
        let slope = report.loglog_slope.expect("slope");
        assert!((slope + 1.0).abs() < 1e-9, "expected ~-1, got {slope}");
    }

    #[test]
    fn build_scaling_report_skips_groups_missing_objective_and_baseline_falls_back() {
        let groups = vec![
            // Smallest axis has no objective -> excluded from points entirely.
            scaling_group("nodes=1", "nodes", "1", None, 0),
            // Has objective but no runtime -> a point, but cannot be the baseline.
            scaling_group("nodes=2", "nodes", "2", Some(4.0), 1),
            scaling_group("nodes=4", "nodes", "4", Some(2.0), 1),
            // Non-numeric axis value -> excluded.
            scaling_group("nodes=auto", "nodes", "auto", Some(1.0), 1),
        ];
        let runtime = BTreeMap::from([("nodes=4".to_string(), 25_u64)]);
        let report = build_scaling_report(
            "nodes",
            hpc_compose::spec::ObjectiveDirection::Minimize,
            &groups,
            &runtime,
        );
        // Only the two numeric groups with an objective survive.
        let axes: Vec<f64> = report.points.iter().map(|p| p.axis_value).collect();
        assert_eq!(axes, vec![2.0, 4.0]);
        // Baseline falls back to the smallest axis that actually has runtime (nodes=4).
        assert_eq!(report.baseline_axis, Some(4.0));
        // The runtime-less point reports no speedup/efficiency (no fabrication).
        let two = report
            .points
            .iter()
            .find(|p| p.axis_value == 2.0)
            .expect("nodes=2 point");
        assert!(two.runtime_seconds_max.is_none());
        assert!(two.speedup.is_none());
        assert!(two.efficiency.is_none());
    }
}
