use std::env;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{
    CacheCommands, Cli, Commands, DoctorCommands, ExamplesCommands, ExperimentCommands,
    JobsCommands, OutputFormat, RendezvousCommands, SchemaKind, SweepCommands, WorkspaceCommands,
};
use hpc_compose::context::{
    BinaryOverrides, ResolveRequest, ResolvedContext, resolve, resolve_binaries_only,
};
use hpc_compose::job::parse_queue_warn_after_duration;
use hpc_compose::term;
use hpc_compose::when::{
    AfterJobCondition, FreeNodesCondition, TimeWindow, WhenConditions, parse_after_job_condition,
    parse_duration as parse_when_duration, parse_poll_interval,
};

mod cache;
mod confirm;
pub(crate) mod doctor;
pub(crate) mod evolve;
pub(crate) mod examples;
pub(crate) mod init;
pub(crate) mod load;
pub(crate) mod runtime;
pub(crate) mod spec;
mod weather;
pub(crate) mod workspace;

#[derive(Debug, Clone, Default)]
struct GlobalCommandOptions {
    quiet: bool,
    profile: Option<String>,
    settings_file: Option<PathBuf>,
    raw_args: Vec<OsString>,
    assume_explicit_values: bool,
}

/// Dispatches a parsed CLI invocation using the provided raw argument vector.
pub fn run_cli(cli: Cli, raw_args: &[OsString]) -> Result<()> {
    term::init_color(cli.color);
    run_command_with_options(
        cli.command,
        &GlobalCommandOptions {
            quiet: cli.quiet,
            profile: cli.profile,
            settings_file: cli.settings_file,
            raw_args: raw_args.to_vec(),
            assume_explicit_values: false,
        },
    )
}

#[cfg(test)]
pub(crate) fn run_command(command: Commands) -> Result<()> {
    run_command_with_options(
        command,
        &GlobalCommandOptions {
            assume_explicit_values: true,
            ..GlobalCommandOptions::default()
        },
    )
}

fn run_command_with_options(command: Commands, options: &GlobalCommandOptions) -> Result<()> {
    match command {
        Commands::Validate {
            file,
            strict_env,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::validate(context, strict_env, format)
        }
        Commands::Lint {
            file,
            strict_env,
            allow_warnings,
            fix,
            dry_run,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::lint(context, strict_env, allow_warnings, fix, dry_run, format)
        }
        Commands::Render {
            file,
            output,
            annotate,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::render(context, output, annotate, format)
        }
        Commands::Explain {
            file,
            field,
            line,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::explain(context, field, line, format)
        }
        Commands::Prepare {
            file,
            enroot_bin,
            apptainer_bin,
            singularity_bin,
            huggingface_cli_bin,
            keep_failed_prep,
            force_rebuild,
            format,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                    ("--huggingface-cli-bin", &huggingface_cli_bin),
                ],
            )?;
            spec::prepare(
                context,
                keep_failed_prep,
                force_rebuild,
                format,
                options.quiet,
            )
        }
        Commands::Preflight {
            file,
            strict,
            verbose,
            format,
            enroot_bin,
            sbatch_bin,
            srun_bin,
            scontrol_bin,
            apptainer_bin,
            singularity_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--scontrol-bin", &scontrol_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            )?;
            spec::preflight(context, strict, verbose, format, options.quiet)
        }
        Commands::Inspect {
            file,
            verbose,
            tree,
            rightsize,
            dependencies,
            dependencies_format,
            job_id,
            sstat_bin,
            squeue_bin,
            sacct_bin,
            format,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--sstat-bin", &sstat_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                ],
            )?;
            spec::inspect(
                context,
                verbose,
                tree,
                rightsize,
                dependencies,
                dependencies_format,
                job_id,
                format,
            )
        }
        Commands::Config {
            file,
            format,
            variables,
            show_values,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::config(context, format, variables, show_values)
        }
        Commands::Schema { kind, output } => print_schema(kind, output),
        Commands::Plan {
            file,
            strict_env,
            verbose,
            tree,
            show_script,
            annotate,
            explain,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            spec::plan(
                context,
                strict_env,
                verbose,
                tree,
                show_script,
                annotate,
                explain,
                format,
            )
        }
        Commands::Doctor {
            command,
            file,
            format,
            cluster_report,
            cluster_report_out,
            mpi_smoke,
            fabric_smoke,
            checks,
            service,
            submit,
            script_out,
            timeout,
            sbatch_bin,
            srun_bin,
            scontrol_bin,
            enroot_bin,
            apptainer_bin,
            singularity_bin,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--scontrol-bin", &scontrol_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            );
            if let Some(command) = command {
                return run_doctor_subcommand(command, options, binary_overrides, format);
            }
            if cluster_report || mpi_smoke || fabric_smoke {
                let _ = writeln!(
                    io::stderr(),
                    "warning: doctor flag-based interface is deprecated; use 'doctor cluster-report', 'doctor mpi-smoke', 'doctor fabric-smoke', or 'doctor readiness' subcommands instead"
                );
            }
            if mpi_smoke && fabric_smoke {
                bail!("doctor --mpi-smoke cannot be combined with --fabric-smoke");
            }
            let timeout_seconds = parse_doctor_timeout(&timeout)?;
            if mpi_smoke {
                if cluster_report {
                    bail!("doctor --mpi-smoke cannot be combined with --cluster-report");
                }
                if checks.is_some() {
                    bail!("doctor --checks requires --fabric-smoke");
                }
                let context = resolve_command_context(options, file, binary_overrides, None)?;
                doctor::doctor_mpi_smoke(
                    context,
                    format,
                    service,
                    submit,
                    script_out,
                    timeout_seconds,
                    options.quiet,
                )
            } else if fabric_smoke {
                if cluster_report {
                    bail!("doctor --fabric-smoke cannot be combined with --cluster-report");
                }
                let context = resolve_command_context(options, file, binary_overrides, None)?;
                doctor::doctor_fabric_smoke(
                    context,
                    doctor::FabricSmokeOptions {
                        format,
                        service_name: service,
                        checks,
                        submit,
                        script_out,
                        timeout_seconds,
                        quiet: options.quiet,
                    },
                )
            } else {
                if submit || service.is_some() || script_out.is_some() || checks.is_some() {
                    bail!(
                        "doctor --submit, --service, --script-out, and --checks require --mpi-smoke or --fabric-smoke"
                    );
                }
                let binaries = resolve_command_binaries(options, binary_overrides)?;
                doctor::doctor(format, &binaries, cluster_report, cluster_report_out)
            }
        }
        Commands::Examples { command } => run_examples_subcommand(command),
        Commands::Weather {
            format,
            sinfo_bin,
            squeue_bin,
            sshare_bin,
            sprio_bin,
        } => {
            let binaries = resolve_bins(
                options,
                &[
                    ("--sinfo-bin", &sinfo_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sshare-bin", &sshare_bin),
                    ("--sprio-bin", &sprio_bin),
                ],
            )?;
            weather::weather(format, &binaries)
        }
        Commands::Up {
            launch,
            prepare_verbose,
            script_out,
            sbatch_bin,
            srun_bin,
            squeue_bin,
            sacct_bin,
            local,
            allow_resume_changes,
            resume_diff_only,
            dry_run,
            detach,
            watch_queue,
            queue_warn_after,
            watch_mode,
            hold_on_exit,
            format,
            print_endpoints,
            metrics_interval,
            no_metrics,
            remote,
            remote_install,
        } => {
            if format.is_some() && !detach && !dry_run {
                bail!("up --format requires --detach or --dry-run");
            }
            if watch_queue && detach {
                bail!("up --watch-queue cannot be combined with --detach");
            }
            if watch_queue && dry_run {
                bail!("up --watch-queue cannot be combined with --dry-run");
            }
            if watch_queue && local {
                bail!("up --watch-queue cannot be combined with --local");
            }
            if queue_warn_after.is_some() && !watch_queue {
                bail!("up --queue-warn-after requires --watch-queue");
            }
            // `--prepare-verbose` is sugar for HPC_COMPOSE_PREPARE_VERBOSE; honor a
            // locally-set env too so either form works. Enable it for a local
            // prepare here, and forward it over --remote below (the env does not
            // cross SSH).
            let prepare_verbose =
                prepare_verbose || hpc_compose::prepare::prepare_verbose_enabled();
            if prepare_verbose {
                // SAFETY: CLI dispatch is single-threaded and runs before prepare
                // spawns its output-streaming threads, so no concurrent env access.
                unsafe { std::env::set_var(hpc_compose::prepare::PREPARE_VERBOSE_ENV, "1") };
            }
            let metrics_overrides = runtime::MetricsOverrides {
                disable: no_metrics,
                interval_seconds: metrics_interval,
            };
            metrics_overrides.validate()?;
            let queue_warn_after_seconds = if watch_queue {
                parse_queue_warn_after_duration(queue_warn_after.as_deref().unwrap_or("10m"))?
            } else {
                None
            };
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            // Thin laptop -> login-node delegation: rsync the project to the
            // login node and run `up` there over SSH. Resolved after context so
            // login_host and the compose path are available; bypasses the local
            // Slurm path entirely (so it works from macOS, which is otherwise
            // authoring-only).
            if let Some(remote_target) = remote {
                if watch_queue {
                    bail!("up --remote cannot be combined with --watch-queue");
                }
                if script_out.is_some() {
                    bail!(
                        "up --remote cannot be combined with --script-out: the rendered script is produced in the remote staged project"
                    );
                }
                return runtime::remote_up(
                    &context,
                    &remote_target,
                    local,
                    runtime::RemoteUpOptions {
                        keep_failed_prep: launch.keep_failed_prep,
                        skip_prepare: launch.skip_prepare,
                        force_rebuild: launch.force_rebuild,
                        no_preflight: launch.no_preflight,
                        allow_resume_changes,
                        resume_diff_only,
                        dry_run,
                        detach,
                        format,
                        print_endpoints,
                        metrics_overrides,
                        watch_mode,
                        hold_on_exit,
                        quiet: options.quiet,
                        install_mode: remote_install,
                        prepare_verbose,
                    },
                );
            }
            runtime::up(
                context,
                script_out,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                runtime::UpOptions {
                    local,
                    allow_resume_changes,
                    resume_diff_only,
                    dry_run,
                    detach,
                    watch_queue,
                    print_endpoints,
                    quiet: options.quiet,
                },
                queue_warn_after_seconds,
                watch_mode,
                hold_on_exit,
                format,
                metrics_overrides,
            )
        }
        Commands::Test {
            launch,
            local,
            submit,
            time,
            timeout,
            script_out,
            sbatch_bin,
            srun_bin,
            squeue_bin,
            sacct_bin,
            scancel_bin,
            format,
        } => {
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--scancel-bin", &scancel_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::smoke_test(
                context,
                local,
                submit,
                time,
                timeout,
                script_out,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                format,
                options.quiet,
            )
        }
        Commands::Dev {
            launch,
            watch_paths,
            debounce_ms,
            keep_running,
            script_out,
            tui,
        } => {
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::dev(
                context,
                watch_paths,
                debounce_ms,
                keep_running,
                script_out,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                options.quiet,
                tui,
            )
        }
        Commands::Tmux {
            launch,
            job_id,
            session,
            tmux_bin,
            no_attach,
            lines,
            script_out,
        } => {
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::tmux(
                context,
                job_id,
                session,
                tmux_bin,
                no_attach,
                lines,
                script_out,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                options.quiet,
            )
        }
        Commands::Sweep { command } => match command {
            SweepCommands::Submit {
                file,
                dry_run,
                max_trials,
                skip_prepare,
                force_rebuild,
                no_preflight,
                resume,
                sweep_id,
                format,
                sbatch_bin,
                srun_bin,
                scontrol_bin,
                enroot_bin,
                apptainer_bin,
                singularity_bin,
                huggingface_cli_bin,
            } => {
                let context = resolve_ctx(
                    options,
                    file,
                    &[
                        ("--enroot-bin", &enroot_bin),
                        ("--sbatch-bin", &sbatch_bin),
                        ("--srun-bin", &srun_bin),
                        ("--scontrol-bin", &scontrol_bin),
                        ("--apptainer-bin", &apptainer_bin),
                        ("--singularity-bin", &singularity_bin),
                        ("--huggingface-cli-bin", &huggingface_cli_bin),
                    ],
                )?;
                runtime::sweep_submit(
                    context,
                    dry_run,
                    max_trials,
                    skip_prepare,
                    force_rebuild,
                    no_preflight,
                    format,
                    resume,
                    sweep_id,
                    options.quiet,
                )
            }
            SweepCommands::Status {
                file,
                sweep_id,
                format,
                squeue_bin,
                sacct_bin,
            } => {
                let context = resolve_ctx(
                    options,
                    file,
                    &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
                )?;
                runtime::sweep_status(context, sweep_id, format)
            }
            SweepCommands::List { file, format } => {
                let context =
                    resolve_command_context(options, file, BinaryOverrides::default(), None)?;
                runtime::sweep_list(context, format)
            }
            SweepCommands::Observe {
                file,
                sweep_id,
                watch,
                stop_when,
                poll_interval,
                timeout,
                format,
                scaling,
                squeue_bin,
                sacct_bin,
                scancel_bin,
            } => {
                use hpc_compose::spec::parse_short_duration;
                let context = resolve_ctx(
                    options,
                    file,
                    &[
                        ("--squeue-bin", &squeue_bin),
                        ("--sacct-bin", &sacct_bin),
                        ("--scancel-bin", &scancel_bin),
                    ],
                )?;
                let poll = parse_short_duration(&poll_interval).with_context(|| {
                    format!("--poll-interval '{poll_interval}' must be like 30s, 5m")
                })?;
                let timeout_dur = match timeout.as_deref() {
                    Some(raw) => {
                        let secs = parse_short_duration(raw)
                            .with_context(|| format!("--timeout '{raw}' must be like 10m or 0s"))?;
                        Some(std::time::Duration::from_secs(secs))
                    }
                    None => None,
                };
                runtime::sweep_observe(
                    context,
                    sweep_id,
                    watch,
                    stop_when,
                    std::time::Duration::from_secs(poll),
                    timeout_dur,
                    format,
                    scaling,
                )
            }
            SweepCommands::Stop {
                file,
                sweep_id,
                yes,
                reason,
                format,
                squeue_bin,
                sacct_bin,
                scancel_bin,
            } => {
                let context = resolve_ctx(
                    options,
                    file,
                    &[
                        ("--squeue-bin", &squeue_bin),
                        ("--sacct-bin", &sacct_bin),
                        ("--scancel-bin", &scancel_bin),
                    ],
                )?;
                runtime::sweep_stop(context, sweep_id, yes, reason, format)
            }
            SweepCommands::Results {
                file,
                sweep_id,
                format,
                include,
                squeue_bin,
                sacct_bin,
                sstat_bin,
            } => {
                let context = resolve_ctx(
                    options,
                    file,
                    &[
                        ("--squeue-bin", &squeue_bin),
                        ("--sacct-bin", &sacct_bin),
                        ("--sstat-bin", &sstat_bin),
                    ],
                )?;
                runtime::sweep_results(context, sweep_id, format, include)
            }
        },
        Commands::Germinate {
            launch,
            script_out,
            canary_time,
            metrics_interval,
            timeout,
            min_cpus,
            min_mem,
            min_gpus,
            sbatch_bin,
            srun_bin,
            squeue_bin,
            sacct_bin,
            sstat_bin,
            dry_run,
            format,
        } => {
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--sstat-bin", &sstat_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::germinate(
                context,
                script_out,
                canary_time,
                metrics_interval,
                timeout,
                min_cpus,
                min_mem,
                min_gpus,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                dry_run,
                format,
                options.quiet,
            )
        }
        Commands::When {
            launch,
            partition,
            free_nodes,
            after_job,
            after_job_condition,
            between,
            poll_interval,
            timeout,
            script_out,
            sbatch_bin,
            srun_bin,
            sinfo_bin,
            squeue_bin,
            sacct_bin,
            allow_resume_changes,
            detach,
            watch_mode,
            hold_on_exit,
            format,
        } => {
            if format.is_some() && !detach {
                bail!("when --format requires --detach");
            }
            let free_nodes = match free_nodes {
                Some(0) => bail!("when --free-nodes must be greater than zero"),
                Some(minimum_idle_nodes) => {
                    let partition = partition
                        .clone()
                        .context("when --free-nodes requires --partition")?;
                    Some(FreeNodesCondition {
                        partition,
                        minimum_idle_nodes,
                    })
                }
                None => None,
            };
            if after_job.is_none() && after_job_condition != "afterany" {
                bail!("when --after-job-condition requires --after-job");
            }
            let after_job = match after_job {
                Some(job_id) => {
                    validate_when_job_id(&job_id)?;
                    Some(AfterJobCondition {
                        job_id,
                        condition: parse_after_job_condition(&after_job_condition)?,
                    })
                }
                None => None,
            };
            let time_window = between.as_deref().map(TimeWindow::parse).transpose()?;
            let conditions = WhenConditions {
                free_nodes,
                after_job,
                time_window,
            };
            if conditions.is_empty() {
                bail!("when requires at least one of --free-nodes, --after-job, or --between");
            }
            let poll_interval = parse_poll_interval(Some(&poll_interval))?;
            let timeout = timeout.as_deref().map(parse_when_duration).transpose()?;
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--sinfo-bin", &sinfo_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::when(
                context,
                conditions,
                poll_interval,
                timeout,
                script_out,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                runtime::WhenOptions {
                    allow_resume_changes,
                    detach,
                    quiet: options.quiet,
                },
                watch_mode,
                hold_on_exit,
                format,
            )
        }
        Commands::Alloc {
            launch,
            mut command,
            salloc_bin,
            srun_bin,
            scontrol_bin,
        } => {
            if command.first().is_some_and(|arg| arg == "--") {
                command.remove(0);
            }
            let context = resolve_ctx(
                options,
                launch.file,
                &[
                    ("--salloc-bin", &salloc_bin),
                    ("--srun-bin", &srun_bin),
                    ("--scontrol-bin", &scontrol_bin),
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                ],
            )?;
            runtime::alloc(
                context,
                command,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                options.quiet,
            )
        }
        Commands::Status {
            file,
            job_id,
            remote,
            format,
            array,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            )?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else {
                runtime::status(context, job_id, format, array)
            }
        }
        Commands::Stats {
            file,
            job_id,
            sweep,
            remote,
            format,
            accounting,
            sstat_bin,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--sstat-bin", &sstat_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                ],
            )?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else if sweep.is_some() {
                runtime::stats_sweep(context, sweep, format, accounting)
            } else {
                runtime::stats(context, job_id, false, format, accounting)
            }
        }
        Commands::MetricsProbe {
            duration_seconds,
            format,
            compare_nvidia_smi,
        } => runtime::metrics_probe(duration_seconds, format, compare_nvidia_smi),
        Commands::Score {
            job_id,
            sweep,
            file,
            remote,
            format,
            pue,
            gpu_tdp_w,
            cpu_watts_per_core,
            sstat_bin,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--sstat-bin", &sstat_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                ],
            )?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else if sweep.is_some() {
                runtime::score_sweep(context, sweep, format, pue, gpu_tdp_w, cpu_watts_per_core)
            } else {
                runtime::score(context, job_id, format, pue, gpu_tdp_w, cpu_watts_per_core)
            }
        }
        Commands::Diff {
            job_id_1,
            job_id_2,
            across,
            jobs,
            against_spec,
            job_id,
            fail_on_change,
            file,
            format,
            matrix_format,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            )?;
            if against_spec {
                runtime::diff_against_spec(context, job_id, fail_on_change, format)
            } else if across.is_some() || !jobs.is_empty() {
                runtime::diff_matrix(context, across, jobs, matrix_format)
            } else {
                runtime::diff(context, job_id_1, job_id_2, format)
            }
        }
        Commands::Artifacts {
            file,
            job_id,
            format,
            bundles,
            tarball,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            runtime::artifacts(context, job_id, format, bundles, tarball)
        }
        Commands::Pull {
            file,
            job_id,
            into,
            remote,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else {
                runtime::pull(context, job_id, into, format)
            }
        }
        Commands::Logs {
            file,
            job_id,
            service,
            follow,
            grep,
            since,
            lines,
            remote,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else {
                runtime::logs(context, job_id, service, follow, lines, grep, since)
            }
        }
        Commands::Ps {
            file,
            job_id,
            remote,
            format,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            )?;
            if let Some(result) = runtime::maybe_remote_followup(&context, remote.remote.as_deref())
            {
                result
            } else {
                runtime::ps(context, job_id, format)
            }
        }
        Commands::Watch {
            file,
            job_id,
            service,
            lines,
            squeue_bin,
            sacct_bin,
            watch_mode,
            hold_on_exit,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            )?;
            runtime::watch(context, job_id, service, lines, watch_mode, hold_on_exit)
        }
        Commands::Replay {
            file,
            job_id,
            service,
            speed,
            lines,
            watch_mode,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            runtime::replay(context, job_id, service, speed, lines, watch_mode, format)
        }
        Commands::Checkpoints {
            file,
            job_id,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            runtime::checkpoints(context, job_id, format)
        }
        Commands::Debug {
            file,
            job_id,
            service,
            lines,
            preflight,
            format,
            squeue_bin,
            sacct_bin,
            enroot_bin,
            sbatch_bin,
            srun_bin,
            scontrol_bin,
            apptainer_bin,
            singularity_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--scontrol-bin", &scontrol_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            )?;
            runtime::debug(
                context,
                job_id,
                service,
                lines,
                preflight,
                format,
                options.quiet,
            )
        }
        Commands::Cancel {
            file,
            job_id,
            scancel_bin,
            purge_cache,
            no_export,
            yes,
            format,
        } => {
            if cancel_requires_confirmation(job_id.as_deref(), purge_cache) {
                confirm::confirm_destructive_action(
                    &cancel_confirmation_action("cancel", job_id.as_deref(), purge_cache),
                    yes,
                )?;
            }
            let binary_overrides =
                resolve_binary_overrides(options, &[("--scancel-bin", &scancel_bin)]);
            let context = resolve_command_context(options, file, binary_overrides, None)?;
            runtime::cancel(context, job_id, purge_cache, no_export, format)
        }
        Commands::Down {
            file,
            job_id,
            scancel_bin,
            purge_cache,
            no_export,
            yes,
            format,
        } => {
            if cancel_requires_confirmation(job_id.as_deref(), purge_cache) {
                confirm::confirm_destructive_action(
                    &cancel_confirmation_action("down", job_id.as_deref(), purge_cache),
                    yes,
                )?;
            }
            let binary_overrides =
                resolve_binary_overrides(options, &[("--scancel-bin", &scancel_bin)]);
            let context = resolve_command_context(options, file, binary_overrides, None)?;
            runtime::cancel(context, job_id, purge_cache, no_export, format)
        }
        Commands::Run {
            launch,
            args,
            image,
            resources,
            time,
            mem,
            cpus_per_task,
            gpus,
            partition,
            env,
            dataset,
            output,
            local,
            script_out,
            sbatch_bin,
            srun_bin,
            squeue_bin,
            sacct_bin,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                ],
            );
            let huggingface_cli_bin = explicit_huggingface_cli_bin(
                options,
                &[("--huggingface-cli-bin", &launch.huggingface_cli_bin)],
            );
            if let Some(image) = image {
                if launch.file.is_some() {
                    bail!("run --image cannot be combined with -f/--file or service mode");
                }
                ensure_run_image_mode_uses_separator(options)?;
                let context = resolve_command_context(
                    options,
                    None,
                    binary_overrides,
                    huggingface_cli_bin.clone(),
                )?;
                runtime::run_ephemeral(
                    context,
                    image,
                    args,
                    runtime::ResourceCliOptions {
                        resources,
                        time,
                        mem,
                        cpus_per_task,
                        gpus,
                        partition,
                        env,
                    },
                    dataset,
                    output,
                    script_out,
                    runtime::PrepareFlags {
                        keep_failed_prep: launch.keep_failed_prep,
                        skip_prepare: launch.skip_prepare,
                        force_rebuild: launch.force_rebuild,
                        no_preflight: launch.no_preflight,
                    },
                    local,
                    options.quiet,
                )
            } else {
                if resources.is_some()
                    || time.is_some()
                    || mem.is_some()
                    || cpus_per_task.is_some()
                    || gpus.is_some()
                    || partition.is_some()
                    || !env.is_empty()
                    || dataset.is_some()
                    || output.is_some()
                    || local
                {
                    bail!(
                        "run resource flags, --env, --dataset, --output, and --local require --image; service mode uses the compose spec"
                    );
                }
                let mut args = args.into_iter();
                let service = args
                    .next()
                    .context("run service mode requires SERVICE -- CMD")?;
                let mut cmd = args.collect::<Vec<_>>();
                if cmd.first().is_some_and(|arg| arg == "--") {
                    cmd.remove(0);
                }
                if cmd.is_empty() {
                    bail!("run service mode requires a command after the service name");
                }
                let context = resolve_command_context(
                    options,
                    launch.file,
                    binary_overrides,
                    huggingface_cli_bin,
                )?;
                runtime::run_service(
                    context,
                    service,
                    cmd,
                    script_out,
                    runtime::PrepareFlags {
                        keep_failed_prep: launch.keep_failed_prep,
                        skip_prepare: launch.skip_prepare,
                        force_rebuild: launch.force_rebuild,
                        no_preflight: launch.no_preflight,
                    },
                    options.quiet,
                )
            }
        }
        Commands::Shell {
            image,
            resources,
            time,
            mem,
            cpus_per_task,
            gpus,
            partition,
            env,
            srun_bin,
        } => {
            let context = resolve_ctx(options, None, &[("--srun-bin", &srun_bin)])?;
            runtime::shell(
                context,
                image,
                runtime::ResourceCliOptions {
                    resources,
                    time,
                    mem,
                    cpus_per_task,
                    gpus,
                    partition,
                    env,
                },
            )
        }
        Commands::New {
            template,
            list_templates,
            describe_template,
            name,
            cache_dir,
            output,
            force,
            format,
        } => init::new_command(
            template,
            list_templates,
            describe_template,
            name,
            cache_dir,
            output,
            force,
            format,
        ),
        Commands::Evolve {
            lesson,
            list_lessons,
            describe_lesson,
            name,
            cache_dir,
            output,
            force,
            yes,
            until,
            format,
        } => evolve::command(
            lesson,
            list_lessons,
            describe_lesson,
            name,
            cache_dir,
            output,
            force,
            yes,
            until,
            format,
        ),
        Commands::Rendezvous { command } => match command {
            RendezvousCommands::Register {
                name,
                host,
                port,
                job_id,
                service,
                protocol,
                path,
                ttl_seconds,
                cache_dir,
                format,
            } => {
                let cache_dir = match cache_dir {
                    Some(path) => path,
                    None => {
                        let context = resolve_command_context(
                            options,
                            None,
                            BinaryOverrides::default(),
                            None,
                        )?;
                        context.cache_dir.value
                    }
                };
                let job_id = job_id
                    .or_else(|| env::var("SLURM_JOB_ID").ok())
                    .context("rendezvous register requires --job-id outside a Slurm job")?;
                runtime::rendezvous_register(
                    cache_dir,
                    name,
                    job_id,
                    service,
                    host,
                    port,
                    protocol,
                    path,
                    ttl_seconds,
                    format,
                )
            }
            RendezvousCommands::Resolve {
                name,
                cache_dir,
                format,
            } => {
                let cache_dir = match cache_dir {
                    Some(path) => path,
                    None => {
                        let context = resolve_command_context(
                            options,
                            None,
                            BinaryOverrides::default(),
                            None,
                        )?;
                        context.cache_dir.value
                    }
                };
                runtime::rendezvous_resolve(cache_dir, name, format)
            }
            RendezvousCommands::List { cache_dir, format } => {
                let cache_dir = match cache_dir {
                    Some(path) => path,
                    None => {
                        let context = resolve_command_context(
                            options,
                            None,
                            BinaryOverrides::default(),
                            None,
                        )?;
                        context.cache_dir.value
                    }
                };
                runtime::rendezvous_list(cache_dir, format)
            }
            RendezvousCommands::Prune { cache_dir, format } => {
                let cache_dir = match cache_dir {
                    Some(path) => path,
                    None => {
                        let context = resolve_command_context(
                            options,
                            None,
                            BinaryOverrides::default(),
                            None,
                        )?;
                        context.cache_dir.value
                    }
                };
                runtime::rendezvous_prune(cache_dir, format)
            }
        },
        Commands::Cache { command } => match command {
            CacheCommands::List { cache_dir, format } => {
                let cache_dir = match cache_dir {
                    Some(path) => Some(path),
                    None => {
                        let context = resolve_command_context(
                            options,
                            None,
                            BinaryOverrides::default(),
                            None,
                        )?;
                        Some(context.cache_dir.value)
                    }
                };
                cache::list(cache_dir, format)
            }
            CacheCommands::Inspect {
                file,
                service,
                format,
            } => {
                let context =
                    resolve_command_context(options, file, BinaryOverrides::default(), None)?;
                cache::inspect(context, service, format)
            }
            CacheCommands::Prune {
                file,
                cache_dir,
                age,
                all_unused,
                yes,
                format,
            } => {
                if age.is_none() && !all_unused {
                    bail!("cache prune requires either --age DAYS or --all-unused");
                }
                if age.is_some() && all_unused {
                    bail!("cache prune accepts only one strategy at a time");
                }
                if all_unused {
                    let file = file.context(
                        "--all-unused requires -f/--file so the current plan can define which artifacts are still referenced",
                    )?;
                    confirm::confirm_destructive_action(
                        "prune cached artifacts that the current compose plan no longer references",
                        yes,
                    )?;
                    let context = resolve_command_context(
                        options,
                        Some(file),
                        BinaryOverrides::default(),
                        None,
                    )?;
                    cache::prune(context, cache_dir, age, all_unused, format)
                } else if cache_dir.is_none() {
                    confirm::confirm_destructive_action("prune cached artifacts by age", yes)?;
                    let context =
                        resolve_command_context(options, file, BinaryOverrides::default(), None)?;
                    cache::prune(context, cache_dir, age, all_unused, format)
                } else {
                    confirm::confirm_destructive_action("prune cached artifacts by age", yes)?;
                    cache::prune_no_context(cache_dir, age, format)
                }
            }
        },
        Commands::Workspace { command } => match command {
            WorkspaceCommands::Status { tools, format } => {
                let context =
                    resolve_command_context(options, None, BinaryOverrides::default(), None)?;
                workspace::status(context, &tools, format)
            }
            WorkspaceCommands::Allocate {
                duration_days,
                tools,
                format,
            } => {
                let context =
                    resolve_command_context(options, None, BinaryOverrides::default(), None)?;
                workspace::allocate(context, duration_days, &tools, format)
            }
            WorkspaceCommands::Extend {
                days,
                tools,
                format,
            } => {
                let context =
                    resolve_command_context(options, None, BinaryOverrides::default(), None)?;
                workspace::extend(context, days, &tools, format)
            }
            WorkspaceCommands::Release { yes, tools, format } => {
                let context =
                    resolve_command_context(options, None, BinaryOverrides::default(), None)?;
                workspace::release(context, yes, &tools, format)
            }
        },
        Commands::Jobs { command } => match command {
            JobsCommands::List {
                disk_usage,
                tag,
                format,
            } => runtime::jobs_list(disk_usage, tag, format),
        },
        Commands::Clean {
            file,
            age,
            all,
            dry_run,
            yes,
            disk_usage,
            format,
        } => {
            if age.is_none() && !all {
                bail!("clean requires either --age DAYS or --all");
            }
            if !dry_run {
                let action = if all {
                    "remove tracked job directories except the latest one"
                } else {
                    "remove tracked job directories by age"
                };
                confirm::confirm_destructive_action(action, yes)?;
            }
            let context = resolve_command_context(options, file, BinaryOverrides::default(), None)?;
            runtime::clean(context, age, all, dry_run, disk_usage, format)
        }
        Commands::Context {
            format,
            show_values,
        } => {
            let context = resolve_command_context(options, None, BinaryOverrides::default(), None)?;
            spec::context(context, format, show_values)
        }
        Commands::Setup {
            profile_name,
            compose_file,
            env_files,
            env,
            binaries,
            cache_dir,
            login_host,
            login_user,
            default_profile,
            non_interactive,
            format,
        } => init::setup(
            options.settings_file.clone(),
            options.profile.clone(),
            profile_name,
            compose_file,
            env_files,
            env,
            binaries,
            cache_dir,
            login_host,
            login_user,
            default_profile,
            non_interactive,
            format,
        ),
        Commands::Notebook {
            kind,
            image,
            port,
            token,
            volumes,
            working_dir,
            tunnel_name,
            timeout,
            follow,
            dry_run,
            args,
            launch,
            resources,
            time,
            mem,
            cpus_per_task,
            gpus,
            partition,
            env,
            local,
            script_out,
            sbatch_bin,
            srun_bin,
            squeue_bin,
            sacct_bin,
            format,
        } => {
            use hpc_compose::cli::NotebookKindArg;
            use hpc_compose::spec::parse_short_duration;
            use runtime::NotebookKind;
            if launch.file.is_some() {
                bail!("notebook does not accept -f/--file; it synthesizes its own spec");
            }
            let notebook_kind = match kind {
                NotebookKindArg::Jupyter => NotebookKind::Jupyter,
                NotebookKindArg::Vscode => NotebookKind::VsCode,
            };
            let ready_timeout_seconds = parse_short_duration(&timeout)
                .with_context(|| format!("--timeout '{timeout}' must be like 30s, 10m, or 1h"))?;
            let mut extra_args = args;
            if extra_args.first().is_some_and(|arg| arg == "--") {
                extra_args.remove(0);
            }
            let context = resolve_ctx(
                options,
                None,
                &[
                    ("--enroot-bin", &launch.enroot_bin),
                    ("--apptainer-bin", &launch.apptainer_bin),
                    ("--singularity-bin", &launch.singularity_bin),
                    ("--huggingface-cli-bin", &launch.huggingface_cli_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                ],
            )?;
            runtime::notebook(
                context,
                runtime::notebook::NotebookArgs {
                    kind: notebook_kind,
                    image,
                    port,
                    token,
                    working_dir,
                    volumes,
                    tunnel_name,
                    extra_args,
                },
                runtime::ResourceCliOptions {
                    resources,
                    time,
                    mem,
                    cpus_per_task,
                    gpus,
                    partition,
                    env,
                },
                script_out,
                std::time::Duration::from_secs(ready_timeout_seconds),
                follow,
                dry_run,
                runtime::PrepareFlags {
                    keep_failed_prep: launch.keep_failed_prep,
                    skip_prepare: launch.skip_prepare,
                    force_rebuild: launch.force_rebuild,
                    no_preflight: launch.no_preflight,
                },
                local,
                options.quiet,
                format,
            )
        }
        Commands::Reach {
            service,
            file,
            job_id,
            port,
            open,
            format,
            squeue_bin,
            sacct_bin,
        } => {
            let context = resolve_ctx(
                options,
                file,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            )?;
            runtime::reach(context, service, job_id, port, open, format)
        }
        Commands::Experiment { command } => match command {
            ExperimentCommands::Show {
                job_id,
                file,
                format,
                pue,
                gpu_tdp_w,
                cpu_watts_per_core,
                sstat_bin,
                squeue_bin,
                sacct_bin,
            } => {
                let context = resolve_ctx(
                    options,
                    file,
                    &[
                        ("--sstat-bin", &sstat_bin),
                        ("--squeue-bin", &squeue_bin),
                        ("--sacct-bin", &sacct_bin),
                    ],
                )?;
                runtime::experiment_show(
                    context,
                    job_id,
                    format,
                    pue,
                    gpu_tdp_w,
                    cpu_watts_per_core,
                )
            }
            ExperimentCommands::Tag {
                tags,
                remove,
                job_id,
                file,
                format,
            } => {
                let context = resolve_ctx(options, file, &[])?;
                runtime::experiment_tag(context, tags, remove, job_id, format)
            }
            ExperimentCommands::Note {
                text,
                job_id,
                file,
                format,
            } => {
                let context = resolve_ctx(options, file, &[])?;
                runtime::experiment_note(context, text, job_id, format)
            }
            ExperimentCommands::Bundle {
                job_id,
                file,
                output,
                dir,
                strict,
                format,
            } => {
                let context = resolve_ctx(options, file, &[])?;
                runtime::experiment_bundle(context, job_id, output, dir, strict, format)
            }
        },
        Commands::Completions { shell } => init::completions(shell),
    }
}

fn cancel_requires_confirmation(job_id: Option<&str>, purge_cache: bool) -> bool {
    job_id.is_none() || purge_cache
}

fn cancel_confirmation_action(command: &str, job_id: Option<&str>, purge_cache: bool) -> String {
    let target = job_id
        .map(|id| format!("tracked job {id}"))
        .unwrap_or_else(|| "the latest tracked job".to_string());
    if purge_cache {
        format!("{command} {target} and purge tracked cache artifacts")
    } else {
        format!("{command} {target}")
    }
}

fn validate_when_job_id(job_id: &str) -> Result<()> {
    let valid_decimal =
        |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    let valid = if let Some((job, task)) = job_id.split_once('_') {
        valid_decimal(job) && valid_decimal(task)
    } else {
        valid_decimal(job_id)
    };
    if !valid {
        bail!("when --after-job must be a Slurm job id like 12345 or array task id like 12345_7");
    }
    Ok(())
}

fn ensure_run_image_mode_uses_separator(options: &GlobalCommandOptions) -> Result<()> {
    if options.assume_explicit_values {
        return Ok(());
    }
    let Some(run_index) = options
        .raw_args
        .iter()
        .position(|arg| arg.to_str() == Some("run"))
    else {
        return Ok(());
    };
    let mut skip_next = false;
    let mut saw_separator = false;
    for arg in options.raw_args.iter().skip(run_index + 1) {
        let Some(value) = arg.to_str() else {
            continue;
        };
        if value == "--" {
            saw_separator = true;
            break;
        }
        if skip_next {
            skip_next = false;
            continue;
        }
        if run_flag_takes_value(value) {
            skip_next = !value.contains('=');
            continue;
        }
        if value.starts_with('-') {
            continue;
        }
        bail!("run --image mode requires the command after -- and cannot include a service name");
    }
    if !saw_separator {
        bail!("run --image mode requires the command after --");
    }
    Ok(())
}

fn run_flag_takes_value(value: &str) -> bool {
    matches!(
        value.split_once('=').map_or(value, |(flag, _)| flag),
        "-f" | "--file"
            | "--image"
            | "--resources"
            | "--time"
            | "--mem"
            | "--cpus-per-task"
            | "--gpus"
            | "--partition"
            | "--env"
            | "--dataset"
            | "--output"
            | "--script-out"
            | "--salloc-bin"
            | "--sbatch-bin"
            | "--srun-bin"
            | "--enroot-bin"
            | "--apptainer-bin"
            | "--singularity-bin"
            | "--squeue-bin"
            | "--sacct-bin"
    )
}

fn run_doctor_subcommand(
    command: DoctorCommands,
    options: &GlobalCommandOptions,
    binary_overrides: BinaryOverrides,
    parent_format: Option<OutputFormat>,
) -> Result<()> {
    match command {
        DoctorCommands::ClusterReport { format, out } => {
            let binaries = resolve_command_binaries(options, binary_overrides)?;
            doctor::doctor(
                Some(format.or(parent_format).unwrap_or(OutputFormat::Text)),
                &binaries,
                true,
                out,
            )
        }
        DoctorCommands::MpiSmoke {
            file,
            format,
            service,
            submit,
            script_out,
            timeout,
        } => {
            let timeout_seconds = parse_doctor_timeout(&timeout)?;
            let context = resolve_command_context(options, file, binary_overrides, None)?;
            doctor::doctor_mpi_smoke(
                context,
                format.or(parent_format),
                service,
                submit,
                script_out,
                timeout_seconds,
                options.quiet,
            )
        }
        DoctorCommands::FabricSmoke {
            file,
            format,
            service,
            checks,
            submit,
            script_out,
            timeout,
        } => {
            let timeout_seconds = parse_doctor_timeout(&timeout)?;
            let context = resolve_command_context(options, file, binary_overrides, None)?;
            doctor::doctor_fabric_smoke(
                context,
                doctor::FabricSmokeOptions {
                    format: format.or(parent_format),
                    service_name: service,
                    checks,
                    submit,
                    script_out,
                    timeout_seconds,
                    quiet: options.quiet,
                },
            )
        }
        DoctorCommands::Readiness {
            file,
            format,
            service,
            run,
            log_file,
            timeout,
        } => {
            let timeout_seconds = timeout.as_deref().map(parse_doctor_timeout).transpose()?;
            let context = resolve_command_context(options, file, binary_overrides, None)?;
            doctor::doctor_readiness(
                context,
                format.or(parent_format),
                service,
                run,
                log_file,
                timeout_seconds,
                options.quiet,
            )
        }
    }
}

/// Parses a doctor `--timeout` DURATION string (`30s`, `5m`, `1h`, or a bare
/// number of seconds) into whole seconds.
fn parse_doctor_timeout(raw: &str) -> Result<u64> {
    hpc_compose::spec::parse_short_duration(raw)
        .with_context(|| format!("doctor --timeout '{raw}' must be like 30s or 5m"))
}

fn run_examples_subcommand(command: ExamplesCommands) -> Result<()> {
    match command {
        ExamplesCommands::List { tag, format } => examples::list(tag, format),
        ExamplesCommands::Search { query, format } => examples::search(query, format),
        ExamplesCommands::Recommend {
            query,
            tags,
            limit,
            format,
        } => examples::recommend(query, tags, limit, format),
        ExamplesCommands::Coverage { format } => examples::coverage(format),
    }
}

fn resolve_command_context(
    options: &GlobalCommandOptions,
    compose_file: Option<PathBuf>,
    binary_overrides: BinaryOverrides,
    huggingface_cli_bin: Option<String>,
) -> Result<ResolvedContext> {
    let cwd = env::current_dir().context("failed to determine current working directory")?;
    resolve(&ResolveRequest {
        cwd,
        profile: options.profile.clone(),
        settings_file: options.settings_file.clone(),
        compose_file_override: compose_file,
        binary_overrides,
        huggingface_cli_bin,
    })
}

/// Convenience wrapper that resolves per-command binary overrides and then
/// the full command context in one call. Equivalent to calling
/// [`resolve_binary_overrides`] followed by [`resolve_command_context`];
/// exists to keep each command-dispatch arm focused on its own logic instead
/// of repeating the resolve-then-resolve pair.
fn resolve_ctx(
    options: &GlobalCommandOptions,
    compose_file: Option<PathBuf>,
    explicit_binaries: &[(&str, &str)],
) -> Result<ResolvedContext> {
    let overrides = resolve_binary_overrides(options, explicit_binaries);
    let huggingface_cli_bin = explicit_huggingface_cli_bin(options, explicit_binaries);
    resolve_command_context(options, compose_file, overrides, huggingface_cli_bin)
}

/// Extracts an explicit `--huggingface-cli-bin` value when the flag was set on
/// the command line. Unlike the cluster-binary overrides, this is not a
/// laptop-probed binary, so it is threaded straight into the context rather than
/// through [`BinaryOverrides`].
fn explicit_huggingface_cli_bin(
    options: &GlobalCommandOptions,
    explicit_values: &[(&str, &str)],
) -> Option<String> {
    explicit_values
        .iter()
        .find(|(flag, _)| *flag == "--huggingface-cli-bin")
        .filter(|_| value_is_explicit(options, "--huggingface-cli-bin"))
        .map(|(_, value)| (*value).to_string())
}

/// Convenience wrapper that resolves per-command binary overrides and then
/// only the resolved binary paths (skipping the rest of the [`ResolvedContext`]).
fn resolve_bins(
    options: &GlobalCommandOptions,
    explicit_binaries: &[(&str, &str)],
) -> Result<hpc_compose::context::ResolvedBinaries> {
    let overrides = resolve_binary_overrides(options, explicit_binaries);
    resolve_command_binaries(options, overrides)
}

fn resolve_command_binaries(
    options: &GlobalCommandOptions,
    binary_overrides: BinaryOverrides,
) -> Result<hpc_compose::context::ResolvedBinaries> {
    let cwd = env::current_dir().context("failed to determine current working directory")?;
    resolve_binaries_only(&ResolveRequest {
        cwd,
        profile: options.profile.clone(),
        settings_file: options.settings_file.clone(),
        compose_file_override: None,
        binary_overrides,
        huggingface_cli_bin: None,
    })
}

fn value_is_explicit(options: &GlobalCommandOptions, long_flag: &str) -> bool {
    if options.assume_explicit_values {
        return true;
    }
    if long_flag == "--file" && has_short_flag(options, "-f") {
        return true;
    }
    has_long_flag(options, long_flag)
}

fn has_short_flag(options: &GlobalCommandOptions, short_flag: &str) -> bool {
    options.raw_args.iter().any(|arg| arg == short_flag)
}

fn has_long_flag(options: &GlobalCommandOptions, long_flag: &str) -> bool {
    options.raw_args.iter().any(|arg| {
        arg == long_flag
            || arg
                .to_str()
                .is_some_and(|value| value.starts_with(&format!("{long_flag}=")))
    })
}

struct BinaryOverrideEntry {
    flag: &'static str,
    setter: fn(BinaryOverrides, String) -> BinaryOverrides,
}

const BINARY_OVERRIDE_ENTRIES: &[BinaryOverrideEntry] = &[
    BinaryOverrideEntry {
        flag: "--enroot-bin",
        setter: |mut o, v| {
            o.enroot = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--apptainer-bin",
        setter: |mut o, v| {
            o.apptainer = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--singularity-bin",
        setter: |mut o, v| {
            o.singularity = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--salloc-bin",
        setter: |mut o, v| {
            o.salloc = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sbatch-bin",
        setter: |mut o, v| {
            o.sbatch = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--srun-bin",
        setter: |mut o, v| {
            o.srun = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--scontrol-bin",
        setter: |mut o, v| {
            o.scontrol = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sinfo-bin",
        setter: |mut o, v| {
            o.sinfo = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--squeue-bin",
        setter: |mut o, v| {
            o.squeue = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sacct-bin",
        setter: |mut o, v| {
            o.sacct = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sstat-bin",
        setter: |mut o, v| {
            o.sstat = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--scancel-bin",
        setter: |mut o, v| {
            o.scancel = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sshare-bin",
        setter: |mut o, v| {
            o.sshare = Some(v);
            o
        },
    },
    BinaryOverrideEntry {
        flag: "--sprio-bin",
        setter: |mut o, v| {
            o.sprio = Some(v);
            o
        },
    },
];

fn resolve_binary_overrides(
    options: &GlobalCommandOptions,
    explicit_values: &[(&str, &str)],
) -> BinaryOverrides {
    let mut overrides = BinaryOverrides::default();
    for entry in BINARY_OVERRIDE_ENTRIES {
        if let Some((_, value)) = explicit_values.iter().find(|(flag, _)| *flag == entry.flag)
            && value_is_explicit(options, entry.flag)
        {
            overrides = (entry.setter)(overrides, value.to_string());
        }
    }
    overrides
}

fn print_schema(kind: Option<SchemaKind>, output: Option<String>) -> Result<()> {
    if let Some(command) = output {
        let json = crate::output::contract::output_schema_json(&command).ok_or_else(|| {
            anyhow::anyhow!(
                "unknown output schema '{command}'; known commands: {}",
                crate::output::contract::output_schema_commands().join(", ")
            )
        })?;
        let mut stdout = io::stdout();
        stdout
            .write_all(json.as_bytes())
            .context("failed to write output schema to stdout")?;
        if !json.ends_with('\n') {
            stdout
                .write_all(b"\n")
                .context("failed to write output schema newline to stdout")?;
        }
        return Ok(());
    }
    let json = match kind {
        Some(SchemaKind::Settings) => hpc_compose::schema::settings_schema_json(),
        _ => hpc_compose::schema::schema_json(),
    };
    let mut stdout = io::stdout();
    stdout
        .write_all(json.as_bytes())
        .context("failed to write schema to stdout")?;
    if !json.ends_with('\n') {
        stdout
            .write_all(b"\n")
            .context("failed to write schema newline to stdout")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_helpers_and_strategy_validation_cover_remaining_paths() {
        let options = GlobalCommandOptions {
            raw_args: vec![
                OsString::from("-f"),
                OsString::from("compose.yaml"),
                OsString::from("--srun-bin=/opt/slurm/bin/srun"),
                OsString::from("--sbatch-bin"),
            ],
            ..GlobalCommandOptions::default()
        };
        assert!(value_is_explicit(&options, "--file"));
        assert!(has_short_flag(&options, "-f"));
        assert!(has_long_flag(&options, "--srun-bin"));
        assert!(has_long_flag(&options, "--sbatch-bin"));
        assert!(!value_is_explicit(&options, "--enroot-bin"));

        let assume_explicit = GlobalCommandOptions {
            assume_explicit_values: true,
            ..GlobalCommandOptions::default()
        };
        assert!(value_is_explicit(&assume_explicit, "--any-flag"));

        assert!(!cancel_requires_confirmation(Some("42"), false));
        assert!(cancel_requires_confirmation(None, false));
        assert!(cancel_requires_confirmation(Some("42"), true));
        assert_eq!(
            cancel_confirmation_action("cancel", Some("42"), true),
            "cancel tracked job 42 and purge tracked cache artifacts"
        );

        let missing_cache_strategy = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: None,
                age: None,
                all_unused: false,
                yes: false,
                format: None,
            },
        })
        .expect_err("cache prune should require a strategy");
        assert!(
            missing_cache_strategy
                .to_string()
                .contains("either --age DAYS or --all-unused")
        );

        let conflicting_cache_strategy = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(PathBuf::from("compose.yaml")),
                cache_dir: None,
                age: Some(1),
                all_unused: true,
                yes: false,
                format: None,
            },
        })
        .expect_err("cache prune should reject multiple strategies");
        assert!(
            conflicting_cache_strategy
                .to_string()
                .contains("accepts only one strategy")
        );

        let all_unused_without_file = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: None,
                age: None,
                all_unused: true,
                yes: false,
                format: None,
            },
        })
        .expect_err("all-unused requires a file");
        assert!(
            all_unused_without_file
                .to_string()
                .contains("requires -f/--file")
        );

        let missing_clean_strategy = run_command(Commands::Clean {
            file: None,
            age: None,
            all: false,
            dry_run: false,
            yes: false,
            disk_usage: false,
            format: None,
        })
        .expect_err("clean should require a strategy");
        assert!(
            missing_clean_strategy
                .to_string()
                .contains("clean requires either --age DAYS or --all")
        );
    }

    #[test]
    fn run_cli_dispatches_jobs_list() {
        run_cli(
            Cli {
                color: hpc_compose::cli::ColorPolicy::Auto,
                quiet: false,
                profile: None,
                settings_file: None,
                command: Commands::Jobs {
                    command: JobsCommands::List {
                        disk_usage: false,
                        tag: Vec::new(),
                        format: Some(hpc_compose::cli::OutputFormat::Json),
                    },
                },
            },
            &[
                OsString::from("hpc-compose"),
                OsString::from("jobs"),
                OsString::from("list"),
                OsString::from("--format"),
                OsString::from("json"),
            ],
        )
        .expect("run cli jobs list");
    }

    #[test]
    fn run_cli_dispatches_experiment_show() {
        // Routes to runtime::experiment_show in a throwaway dir with no tracked
        // run; the dispatch is proven by the tracked-job hint error (not a
        // command-not-wired panic), and nothing is submitted or written.
        let dir = tempfile::tempdir().expect("tmpdir");
        let compose = dir.path().join("compose.yaml");
        std::fs::write(
            &compose,
            "name: dispatch-test\nx-slurm:\n  time: \"00:10:00\"\nservices:\n  app:\n    image: docker://python:3.12\n    command: [\"true\"]\n",
        )
        .expect("write compose");
        let err = run_cli(
            Cli {
                color: hpc_compose::cli::ColorPolicy::Auto,
                quiet: false,
                profile: None,
                settings_file: None,
                command: Commands::Experiment {
                    command: ExperimentCommands::Show {
                        job_id: Some("99999".to_string()),
                        file: Some(compose.clone()),
                        format: Some(hpc_compose::cli::OutputFormat::Json),
                        pue: 1.20,
                        gpu_tdp_w: 300.0,
                        cpu_watts_per_core: 8.0,
                        sstat_bin: "sstat".to_string(),
                        squeue_bin: "squeue".to_string(),
                        sacct_bin: "sacct".to_string(),
                    },
                },
            },
            &[
                OsString::from("hpc-compose"),
                OsString::from("experiment"),
                OsString::from("show"),
                OsString::from("99999"),
                OsString::from("-f"),
                OsString::from(compose.as_os_str()),
                OsString::from("--format"),
                OsString::from("json"),
            ],
        )
        .expect_err("unknown tracked job should error");
        assert!(
            err.to_string().contains("was not found"),
            "expected tracked-job hint, got: {err}"
        );
    }
}
