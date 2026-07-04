use super::notebook::{
    NotebookArgs, build_connection, build_connection_output, build_notebook_service_spec,
    build_server_command, generate_token, preset_for, readiness_spec, resolve_image,
};
use super::resources::{
    build_ephemeral_runtime_plan, build_synthetic_service_plan, parse_env_entries,
    push_slurm_salloc_options, push_slurm_srun_options, slurm_from_resource_options,
};
use super::*;
use hpc_compose::cli::OutputFormat;

pub(crate) fn alloc(
    context: ResolvedContext,
    command: Vec<String>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if runtime_plan.slurm.array.is_some() {
        bail!(
            "alloc does not support x-slurm.array; interactive allocations run one compose allocation"
        );
    }
    if runtime_plan.slurm.has_scheduler_dependency() {
        bail!("alloc does not support Slurm job dependencies");
    }
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let progress = ProgressReporter::new(!quiet);
    let cluster_profile = load_discovered_cluster_profile(&context)?;

    if !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: context.binaries.salloc.value.clone(),
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
            bail!("preflight failed; fix the reported errors before opening an allocation");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let bootstrap = allocation_bootstrap_script(&context, &runtime_plan, &submit_dir);
    let mut args = Vec::new();
    push_slurm_salloc_options(&mut args, &runtime_plan.slurm);
    args.push("bash".to_string());
    args.push("-lc".to_string());
    args.push(bootstrap);
    args.push("hpc-compose-alloc".to_string());
    args.extend(command);

    if !quiet {
        println!(
            "opening Slurm allocation with {}",
            context.binaries.salloc.value
        );
    }
    let status = Command::new(&context.binaries.salloc.value)
        .args(&args)
        .current_dir(&submit_dir)
        .status()
        .with_context(|| format!("failed to execute '{}'", context.binaries.salloc.value))?;
    if !status.success() {
        if let Some(code) = status.code() {
            return Err(crate::exit::ExitCodeError(code).into());
        }
        bail!("salloc failed with status {status}");
    }
    Ok(())
}

pub(crate) fn run_service(
    context: ResolvedContext,
    service_name: String,
    command: Vec<String>,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    let file = context.compose_file.value.clone();
    let progress = ProgressReporter::new(!quiet);
    let active_allocation_job_id = active_allocation_job_id();
    let mut runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &context.compose_file.value,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let Some(mut service) = runtime_plan
        .ordered_services
        .iter()
        .find(|candidate| candidate.name == service_name)
        .cloned()
    else {
        bail!(
            "service '{}' does not exist in {}",
            service_name,
            file.display()
        );
    };
    service.depends_on.clear();
    service.execution = ExecutionSpec::Exec(command.clone());
    runtime_plan.name = format!("{}-{}-run", runtime_plan.name, service_name);
    runtime_plan.slurm.resume = None;
    runtime_plan.ordered_services = vec![service];

    ensure_batch_submission_supported(&runtime_plan, true, false)?;

    let cluster_profile = load_discovered_cluster_profile(&context)?;

    if !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
            || {
                Ok::<_, anyhow::Error>(run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: context.binaries.enroot.value.clone(),
                        apptainer_bin: context.binaries.apptainer.value.clone(),
                        singularity_bin: context.binaries.singularity.value.clone(),
                        sbatch_bin: active_allocation_job_id
                            .as_ref()
                            .map(|_| context.binaries.srun.value.clone())
                            .unwrap_or_else(|| context.binaries.sbatch.value.clone()),
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
            bail!("preflight failed; fix the reported errors before running");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let script = progress.run_result("Rendering run script", || {
        let script = render_script_with_options(
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
        Ok::<_, anyhow::Error>(if active_allocation_job_id.is_some() {
            strip_sbatch_directives(&script)
        } else {
            script
        })
    })?;
    let script_path = script_out.unwrap_or_else(|| default_run_script_path(&file, &service_name));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if let Some(job_id) = active_allocation_job_id.as_deref() {
        println!("using active Slurm allocation {job_id}");
        let record = build_submission_record_with_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            job_id,
            &SubmissionRecordBuildOptions {
                kind: SubmissionKind::Run,
                service_name: Some(service_name.clone()),
                command_override: Some(command.clone()),
                requested_walltime: requested_walltime(&runtime_plan),
                slurm_array: None,
                sweep: None,
                config_snapshot_yaml: None,
                cached_artifacts: tracked_cached_artifacts(&runtime_plan),
                provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
            },
        )?;
        write_submission_record(&record)
            .context("failed to persist tracking metadata for in-allocation run")?;
        output::print_submit_summary_box(
            &runtime_plan,
            &record.job_id,
            &script_path,
            Some(&latest_record_path(&record)),
        );
        let status = Command::new("bash")
            .arg(&script_path)
            .current_dir(&submit_dir)
            .status()
            .with_context(|| format!("failed to execute '{}'", script_path.display()))?;
        if !status.success() {
            if let Some(code) = status.code() {
                return Err(crate::exit::ExitCodeError(code).into());
            }
            bail!("in-allocation run failed with status {status}");
        }
        return Ok(());
    }

    super::ensure_default_batch_log_dir(&submit_dir, &runtime_plan)?;
    let output_result = progress.run_result("Submitting run job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
            .args(sbatch_cli_args(&runtime_plan))
            .arg(&script_path)
            .output()
            .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
    })?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            enrich_sbatch_failure(&String::from_utf8_lossy(&output_result.stderr))
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout);
    print!("{stdout}");
    output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;

    let Some(job_id) = output::extract_job_id(stdout.trim()) else {
        println!(
            "note: sbatch output did not include a numeric Slurm job id, so this run is not trackable"
        );
        return Ok(());
    };

    let record = build_submission_record_with_options(
        &file,
        &submit_dir,
        &script_path,
        &runtime_plan,
        job_id,
        &SubmissionRecordBuildOptions {
            kind: SubmissionKind::Run,
            service_name: Some(service_name.clone()),
            command_override: Some(command),
            requested_walltime: requested_walltime(&runtime_plan),
            slurm_array: runtime_plan.slurm.array.clone(),
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: tracked_cached_artifacts(&runtime_plan),
            provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
        },
    )?;
    write_submission_record(&record)?;
    output::print_submit_summary_box(
        &runtime_plan,
        &record.job_id,
        &script_path,
        Some(&latest_record_path(&record)),
    );
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            Some(&service_name),
            100,
            WatchMode::Auto,
            HoldOnExit::Failure,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ephemeral(
    context: ResolvedContext,
    image: String,
    command: Vec<String>,
    resource_options: ResourceCliOptions,
    dataset: Option<PathBuf>,
    output: Option<PathBuf>,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    local: bool,
    quiet: bool,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    if image.trim().is_empty() {
        bail!("run --image requires a non-empty image");
    }
    if command.is_empty() {
        bail!("run --image requires a command after --");
    }
    // The planner only normalizes mount/host paths lexically and never checks
    // that they exist, so validate the dataset path here before any rendering
    // or submission. `run` only prepares and submits locally-built scripts; it
    // never opens a connection or copies the dataset itself.
    if let Some(dataset) = dataset.as_deref() {
        let raw = dataset.to_string_lossy();
        if raw.trim().is_empty() {
            bail!("run --dataset requires a non-empty path");
        }
        // Path-based only: reject remote/registry schemes such as `hf://`.
        if let Some((scheme, _)) = raw.split_once("://") {
            bail!(
                "run --dataset must be a filesystem path, not a '{scheme}://' URL; copy the dataset onto the shared filesystem first"
            );
        }
        match dataset.try_exists() {
            Ok(true) => {}
            Ok(false) => bail!("run --dataset path does not exist: {}", dataset.display()),
            Err(err) => bail!(
                "run --dataset path {} could not be accessed: {err}",
                dataset.display()
            ),
        }
    }
    if let Some(output) = output.as_deref()
        && output.as_os_str().is_empty()
    {
        bail!("run --output requires a non-empty directory");
    }
    let file = context.cwd.join("hpc-compose-run.yaml");
    let progress = ProgressReporter::new(!quiet);
    let runtime_plan = build_ephemeral_runtime_plan(
        &context,
        image,
        command.clone(),
        &resource_options,
        dataset.as_deref(),
        output.as_deref(),
    )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    } else {
        ensure_batch_submission_supported(&runtime_plan, true, false)?;
    }

    let cluster_profile = if local {
        None
    } else {
        load_discovered_cluster_profile(&context)?
    };

    if !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
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
                        require_submit_tools: !local,
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
            bail!("preflight failed; fix the reported errors before running");
        }
    }

    if !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet {
            output::print_prepare_summary(&summary);
        }
    }

    let local_job_id = local.then(generate_local_job_id);
    let script = progress.run_result("Rendering run script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script_with_options(
                &runtime_plan,
                job_id,
                &context.binaries.enroot.value,
                &LocalRenderOptions {
                    runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                        &context.cwd,
                        runtime_plan.slurm.runtime_root.as_deref(),
                    )),
                    ..LocalRenderOptions::default()
                },
            )
        } else {
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
        }
    })?;
    let script_path =
        script_out.unwrap_or_else(|| default_ephemeral_run_script_path(&context.cwd, local));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Run,
        service_name: Some("run".to_string()),
        command_override: Some(command),
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: None,
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
    };

    if local {
        let record = build_submission_record_with_backend_and_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            local_job_id
                .as_deref()
                .context("missing synthetic local job id")?,
            SubmissionBackend::Local,
            &record_options,
        )?;
        write_submission_record(&record)
            .context("failed to persist tracking metadata for local launch")?;
        let supervisor_pid =
            match spawn_local_supervisor(&submit_dir, &script_path, &record.batch_log) {
                Ok(pid) => pid,
                Err(err) => {
                    rollback_local_tracking(&record, None);
                    return Err(err);
                }
            };
        if let Err(err) = write_local_runtime_state_stub(&record, &runtime_plan, supervisor_pid) {
            rollback_local_tracking(&record, Some(supervisor_pid));
            return Err(err);
        }
        print_local_launch_details(&record, &runtime_plan, &script_path);
        output::print_submit_summary_box(
            &runtime_plan,
            &record.job_id,
            &script_path,
            Some(&latest_record_path(&record)),
        );
        return output::finish_watch(
            &record,
            watch_with_fallback(
                &record,
                &SchedulerOptions {
                    squeue_bin: context.binaries.squeue.value.clone(),
                    sacct_bin: context.binaries.sacct.value.clone(),
                },
                Some("run"),
                100,
                WatchMode::Auto,
                HoldOnExit::Failure,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )?,
        );
    }

    super::ensure_default_batch_log_dir(&submit_dir, &runtime_plan)?;
    let output_result = progress.run_result("Submitting run job to Slurm", || {
        Command::new(&context.binaries.sbatch.value)
            .args(sbatch_cli_args(&runtime_plan))
            .arg(&script_path)
            .output()
            .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
    })?;
    if !output_result.status.success() {
        bail!(
            "sbatch failed: {}",
            enrich_sbatch_failure(&String::from_utf8_lossy(&output_result.stderr))
        );
    }

    let stdout = String::from_utf8_lossy(&output_result.stdout);
    print!("{stdout}");
    output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;

    let Some(job_id) = output::extract_job_id(stdout.trim()) else {
        println!(
            "note: sbatch output did not include a numeric Slurm job id, so this run is not trackable"
        );
        return Ok(());
    };

    let record = build_submission_record_with_options(
        &file,
        &submit_dir,
        &script_path,
        &runtime_plan,
        job_id,
        &record_options,
    )?;
    write_submission_record(&record)?;
    output::print_submit_summary_box(
        &runtime_plan,
        &record.job_id,
        &script_path,
        Some(&latest_record_path(&record)),
    );
    output::finish_watch(
        &record,
        watch_with_fallback(
            &record,
            &SchedulerOptions {
                squeue_bin: context.binaries.squeue.value.clone(),
                sacct_bin: context.binaries.sacct.value.clone(),
            },
            Some("run"),
            100,
            WatchMode::Auto,
            HoldOnExit::Failure,
            watch_ui::WatchPrefs::resolve(&context.watch),
        )?,
    )
}

pub(crate) fn shell(
    context: ResolvedContext,
    image: String,
    resource_options: ResourceCliOptions,
) -> Result<()> {
    if image.trim().is_empty() {
        bail!("shell --image requires a non-empty image");
    }
    let env_map = parse_env_entries(&resource_options.env)?;
    let mut slurm = slurm_from_resource_options("hpc-compose-shell", &resource_options)?;
    apply_resource_profile_defaults(&mut slurm, &context.resource_profiles)?;
    slurm.validate()?;
    ensure_batch_submission_supported(
        &RuntimePlan {
            name: "hpc-compose-shell".to_string(),
            cache_dir: context.cache_dir.value.clone(),
            runtime: RuntimeConfig::default(),
            slurm: slurm.clone(),
            ordered_services: Vec::new(),
        },
        false,
        false,
    )?;

    let mut args = Vec::new();
    push_slurm_srun_options(&mut args, &slurm);
    args.push("--pty".to_string());
    args.push(format!("--container-image={image}"));
    if !env_map.is_empty() {
        args.push(format!(
            "--container-env={}",
            env_map.keys().cloned().collect::<Vec<_>>().join(",")
        ));
    }
    args.push("bash".to_string());
    args.push("-l".to_string());

    let mut command = Command::new(&context.binaries.srun.value);
    command.args(&args);
    for (key, value) in env_map {
        command.env(key, value);
    }
    let status = command
        .status()
        .with_context(|| format!("failed to execute '{}'", context.binaries.srun.value))?;
    if !status.success() {
        if let Some(code) = status.code() {
            return Err(crate::exit::ExitCodeError(code).into());
        }
        bail!("srun failed with status {status}");
    }
    Ok(())
}

fn default_notebook_script_path(cwd: &Path, local: bool) -> PathBuf {
    if local {
        cwd.join("hpc-compose-notebook.local.sh")
    } else {
        cwd.join("hpc-compose-notebook.sbatch")
    }
}

/// Best-effort fully-qualified hostname of the current host, used as the SSH
/// jump host in the Jupyter tunnel hint and `reach`. Returns `None` when it
/// cannot be determined so the hint degrades to a placeholder.
pub(crate) fn current_hostname() -> Option<String> {
    if let Some(name) = env::var_os("HOSTNAME")
        && !name.is_empty()
        && name.to_string_lossy() != "127.0.0.1"
    {
        return Some(name.to_string_lossy().into_owned());
    }
    Command::new("hostname")
        .arg("-f")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

/// Polls the notebook service log until *pattern* matches or *timeout* elapses.
///
/// Returns the full log text at match time so the caller can scrape the
/// connection URL. Reuses the same regex-on-log approach as
/// `readiness_util::wait_for_log`.
fn wait_for_notebook_log(log_path: &Path, pattern: &str, timeout: Duration) -> Result<String> {
    let regex = regex::Regex::new(pattern)
        .with_context(|| format!("notebook readiness pattern '{pattern}' is not a valid regex"))?;
    let started = Instant::now();
    loop {
        match fs::read_to_string(log_path) {
            Ok(content) if regex.is_match(&content) => return Ok(content),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => bail!("failed to read notebook log {}: {err}", log_path.display()),
        }
        if started.elapsed() >= timeout {
            bail!(
                "notebook did not become ready within {}s; inspect the log at {} and the tracked job with `hpc-compose status`",
                timeout.as_secs(),
                log_path.display()
            );
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn print_notebook_connection(connection: &notebook::NotebookConnection) {
    println!();
    println!("{}", term::styled_section_header("Notebook ready"));
    println!(
        "{}",
        term::styled_success(&format!("Open: {}", connection.url))
    );
    if let Some(hint) = connection.tunnel_hint.as_deref() {
        println!();
        println!("{}", hint);
    }
}

#[derive(Debug, Serialize)]
struct NotebookDryRunOutput {
    dry_run: bool,
    submitted: bool,
    kind: String,
    script_path: PathBuf,
    cache_dir: PathBuf,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn notebook(
    context: ResolvedContext,
    nb_args: NotebookArgs,
    resource_options: ResourceCliOptions,
    script_out: Option<PathBuf>,
    ready_timeout: Duration,
    follow: bool,
    dry_run: bool,
    flags: PrepareFlags,
    local: bool,
    quiet: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let PrepareFlags {
        keep_failed_prep,
        skip_prepare,
        force_rebuild,
        no_preflight,
    } = flags;
    // JSON output emits a single document on stdout, so every human-readable
    // print on the submit path is gated on `!json_mode`. `--follow` streams a
    // live log view that cannot coexist with a single JSON document.
    let json_mode = matches!(
        output::resolve_output_format(format, false),
        OutputFormat::Json
    );
    if json_mode && follow {
        bail!("--format json is incompatible with --follow (which streams a live log view)");
    }
    let preset = preset_for(nb_args.kind);
    let image = resolve_image(&nb_args, &preset)?;
    let token = nb_args.token.clone().unwrap_or_else(generate_token);
    let command = build_server_command(&nb_args, &token);
    let readiness = readiness_spec(&preset);
    let service = build_notebook_service_spec(&nb_args, &image, command.clone(), readiness);
    let job_name = format!("hpc-compose-notebook-{}", preset.kind.as_str());
    let runtime_plan = build_synthetic_service_plan(
        &context,
        &job_name,
        "notebook",
        service,
        &resource_options,
        None,
    )?;
    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    let file = context.cwd.join(format!("{job_name}.yaml"));

    if local {
        ensure_local_submit_supported(&runtime_plan)?;
        warn_local_ignored_scheduler_settings(&runtime_plan);
    } else {
        ensure_batch_submission_supported(&runtime_plan, false, local)?;
    }
    let cluster_profile = if local {
        None
    } else {
        load_discovered_cluster_profile(&context)?
    };

    let progress = ProgressReporter::new(!quiet && !json_mode);
    // --dry-run is a static preview: skip preflight and image preparation so
    // it works without a runtime backend or network access.
    if !dry_run && !no_preflight {
        let report = progress.run_checked_result(
            "Running preflight checks",
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
                        require_submit_tools: !local,
                        skip_prepare,
                        cluster_profile: cluster_profile.clone(),
                    },
                ))
            },
            |report| report.has_errors(),
        )?;
        if report.has_errors() || (!quiet && !json_mode) {
            output::print_report(&report, false);
        }
        if report.has_errors() {
            bail!("preflight failed; fix the reported errors before launching the notebook");
        }
    }

    if !dry_run && !skip_prepare {
        let prepare_progress = PrepareProgress::new(&runtime_plan, !quiet);
        let summary = prepare_progress.run("Preparing runtime artifacts", || {
            prepare_runtime_plan_with_reporter(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin: context.binaries.enroot.value.clone(),
                    apptainer_bin: context.binaries.apptainer.value.clone(),
                    singularity_bin: context.binaries.singularity.value.clone(),
                    huggingface_cli_bin: context.huggingface_cli_bin.clone(),
                    keep_failed_prep,
                    force_rebuild,
                    enroot_temp_dir: context.enroot_temp_dir.clone(),
                },
                &prepare_progress,
            )
        })?;
        prepare_progress.finish_from_summary(&summary);
        if !quiet && !json_mode {
            output::print_prepare_summary(&summary);
        }
    }

    let local_job_id = local.then(generate_local_job_id);
    let script = progress.run_result("Rendering notebook script", || {
        if let Some(job_id) = local_job_id.as_deref() {
            render_local_script_with_options(
                &runtime_plan,
                job_id,
                &context.binaries.enroot.value,
                &LocalRenderOptions {
                    runtime_root: Some(crate::tracked_paths::resolve_runtime_root(
                        &context.cwd,
                        runtime_plan.slurm.runtime_root.as_deref(),
                    )),
                    ..LocalRenderOptions::default()
                },
            )
        } else {
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
        }
    })?;
    let script_path =
        script_out.unwrap_or_else(|| default_notebook_script_path(&context.cwd, local));
    crate::secure_io::write(&script_path, script, true).with_context(|| {
        format!(
            "failed to write rendered script to {}",
            script_path.display()
        )
    })?;

    if dry_run {
        if json_mode {
            println!(
                "{}",
                serde_json::to_string_pretty(&NotebookDryRunOutput {
                    dry_run: true,
                    submitted: false,
                    kind: nb_args.kind.as_str().to_string(),
                    script_path,
                    cache_dir: runtime_plan.cache_dir,
                })
                .context("failed to serialize notebook dry-run output")?
            );
        } else {
            println!(
                "{}",
                term::styled_success(&format!(
                    "rendered notebook launcher: {}",
                    script_path.display()
                ))
            );
        }
        return Ok(());
    }

    let record_options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::Notebook,
        service_name: Some("notebook".to_string()),
        command_override: Some(command),
        requested_walltime: requested_walltime(&runtime_plan),
        slurm_array: runtime_plan.slurm.array.clone(),
        sweep: None,
        config_snapshot_yaml: None,
        cached_artifacts: tracked_cached_artifacts(&runtime_plan),
        provenance: collect_submit_provenance(&context.cwd, &runtime_plan),
    };

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    // Submit -----------------------------------------------------------------
    let record = if local {
        let record = build_submission_record_with_backend_and_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            local_job_id
                .as_deref()
                .context("missing synthetic local job id")?,
            SubmissionBackend::Local,
            &record_options,
        )?;
        write_submission_record(&record)
            .context("failed to persist tracking metadata for local notebook")?;
        let supervisor_pid =
            match spawn_local_supervisor(&submit_dir, &script_path, &record.batch_log) {
                Ok(pid) => pid,
                Err(err) => {
                    rollback_local_tracking(&record, None);
                    return Err(err);
                }
            };
        if let Err(err) = write_local_runtime_state_stub(&record, &runtime_plan, supervisor_pid) {
            rollback_local_tracking(&record, Some(supervisor_pid));
            return Err(err);
        }
        if !json_mode {
            print_local_launch_details(&record, &runtime_plan, &script_path);
        }
        record
    } else {
        super::ensure_default_batch_log_dir(&submit_dir, &runtime_plan)?;
        let output_result = progress.run_result("Submitting notebook job to Slurm", || {
            Command::new(&context.binaries.sbatch.value)
                .args(sbatch_cli_args(&runtime_plan))
                .arg(&script_path)
                .output()
                .with_context(|| format!("failed to execute '{}'", context.binaries.sbatch.value))
        })?;
        if !output_result.status.success() {
            bail!(
                "sbatch failed: {}",
                enrich_sbatch_failure(&String::from_utf8_lossy(&output_result.stderr))
            );
        }
        let stdout = String::from_utf8_lossy(&output_result.stdout);
        if !json_mode {
            print!("{stdout}");
            output::print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
        }
        let Some(job_id) = output::extract_job_id(stdout.trim()) else {
            bail!(
                "sbatch output did not include a numeric Slurm job id; cannot track the notebook"
            );
        };
        let record = build_submission_record_with_options(
            &file,
            &submit_dir,
            &script_path,
            &runtime_plan,
            job_id,
            &record_options,
        )?;
        write_submission_record(&record)?;
        // Wait until the allocation is RUNNING before polling the log.
        wait_for_job_start(&record, &scheduler_options, None)?;
        record
    };

    if !json_mode {
        output::print_submit_summary_box(
            &runtime_plan,
            &record.job_id,
            &script_path,
            Some(&latest_record_path(&record)),
        );
    }

    // Readiness gate --------------------------------------------------------
    let log_path = record
        .service_logs
        .get("notebook")
        .with_context(|| "tracked notebook service log path was not recorded")?;
    if !json_mode {
        println!(
            "{}",
            term::styled_dim(&format!(
                "waiting for notebook to become ready (timeout {}s)...",
                ready_timeout.as_secs()
            ))
        );
    }
    let log_text = wait_for_notebook_log(log_path, preset.readiness_log_pattern, ready_timeout)?;

    let (compute_node, login_node) = if local {
        (None, None)
    } else {
        let snapshot = build_status_snapshot(&file, Some(&record.job_id), &scheduler_options)?;
        let compute = snapshot
            .services
            .iter()
            .find(|row| row.service_name == "notebook")
            .and_then(|row| row.nodelist.clone())
            .and_then(|nodes| nodes.split(',').next().map(str::to_string));
        // Configured login_host wins over the hostname guess; both may be None,
        // in which case the hint degrades to a <login-node> placeholder.
        (
            compute,
            context.login_host.clone().or_else(current_hostname),
        )
    };
    let connection = build_connection(
        &nb_args,
        &preset,
        &token,
        &log_text,
        compute_node.as_deref(),
        login_node.as_deref(),
        local,
    )?;
    if json_mode {
        let out = build_connection_output(
            &connection,
            compute_node.as_deref(),
            login_node.as_deref(),
            &record.job_id,
            &file,
        );
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        print_notebook_connection(&connection);
        println!(
            "{}",
            term::styled_dim(&format!(
                "manage with: `hpc-compose status -f {}` / `hpc-compose cancel -f {}`",
                file.display(),
                file.display()
            ))
        );
    }

    if follow {
        return output::finish_watch(
            &record,
            watch_with_fallback(
                &record,
                &scheduler_options,
                Some("notebook"),
                100,
                WatchMode::Auto,
                HoldOnExit::Failure,
                watch_ui::WatchPrefs::resolve(&context.watch),
            )?,
        );
    }
    Ok(())
}
