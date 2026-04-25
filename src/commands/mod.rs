use std::env;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{
    CacheCommands, Cli, Commands, DoctorCommands, JobsCommands, OutputFormat, WatchMode,
};
use hpc_compose::context::{
    BinaryOverrides, ResolveRequest, ResolvedContext, resolve, resolve_binaries_only,
};
use hpc_compose::term;

mod cache;
mod doctor;
mod init;
mod runtime;
mod spec;

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
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::validate(context, strict_env, format)
        }
        Commands::Render {
            file,
            output,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::render(context, output, format)
        }
        Commands::Prepare {
            file,
            enroot_bin,
            apptainer_bin,
            singularity_bin,
            keep_failed_prep,
            force,
            format,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            spec::prepare(context, keep_failed_prep, force, format, options.quiet)
        }
        Commands::Preflight {
            file,
            strict,
            verbose,
            format,
            json,
            enroot_bin,
            sbatch_bin,
            srun_bin,
            scontrol_bin,
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
            let context = resolve_command_context(options, file, binary_overrides)?;
            spec::preflight(context, strict, verbose, format, json, options.quiet)
        }
        Commands::Inspect {
            file,
            verbose,
            tree,
            format,
            json,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::inspect(context, verbose, tree, format, json)
        }
        Commands::Config {
            file,
            format,
            variables,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::config(context, format, variables)
        }
        Commands::Schema => print_schema(),
        Commands::Plan {
            file,
            strict_env,
            verbose,
            tree,
            show_script,
            format,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::plan(context, strict_env, verbose, tree, show_script, format)
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
            timeout_seconds,
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
            if mpi_smoke && fabric_smoke {
                bail!("doctor --mpi-smoke cannot be combined with --fabric-smoke");
            }
            if mpi_smoke {
                if cluster_report {
                    bail!("doctor --mpi-smoke cannot be combined with --cluster-report");
                }
                if checks.is_some() {
                    bail!("doctor --checks requires --fabric-smoke");
                }
                let context = resolve_command_context(options, file, binary_overrides)?;
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
                let context = resolve_command_context(options, file, binary_overrides)?;
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
        Commands::Up {
            file,
            script_out,
            sbatch_bin,
            srun_bin,
            enroot_bin,
            apptainer_bin,
            singularity_bin,
            squeue_bin,
            sacct_bin,
            keep_failed_prep,
            skip_prepare,
            force_rebuild,
            no_preflight,
            local,
            allow_resume_changes,
            resume_diff_only,
            dry_run,
            detach,
            watch_mode,
            no_tui,
            format,
        } => {
            if format.is_some() && !detach && !dry_run {
                bail!("up --format requires --detach or --dry-run");
            }
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            let watch_mode = resolve_watch_mode(watch_mode, no_tui)?;
            runtime::up(
                context,
                script_out,
                keep_failed_prep,
                skip_prepare,
                force_rebuild,
                no_preflight,
                local,
                allow_resume_changes,
                resume_diff_only,
                dry_run,
                detach,
                watch_mode,
                format,
                options.quiet,
            )
        }
        Commands::Status {
            file,
            job_id,
            format,
            json,
            squeue_bin,
            sacct_bin,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::status(context, job_id, format, json)
        }
        Commands::Stats {
            file,
            job_id,
            json,
            format,
            sstat_bin,
            squeue_bin,
            sacct_bin,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--sstat-bin", &sstat_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                ],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::stats(context, job_id, json, format)
        }
        Commands::Artifacts {
            file,
            job_id,
            format,
            json,
            bundles,
            tarball,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            runtime::artifacts(context, job_id, format, json, bundles, tarball)
        }
        Commands::Logs {
            file,
            job_id,
            service,
            follow,
            lines,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            runtime::logs(context, job_id, service, follow, lines)
        }
        Commands::Ps {
            file,
            job_id,
            format,
            squeue_bin,
            sacct_bin,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::ps(context, job_id, format)
        }
        Commands::Watch {
            file,
            job_id,
            service,
            lines,
            squeue_bin,
            sacct_bin,
            watch_mode,
            no_tui,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[("--squeue-bin", &squeue_bin), ("--sacct-bin", &sacct_bin)],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            let watch_mode = resolve_watch_mode(watch_mode, no_tui)?;
            runtime::watch(context, job_id, service, lines, watch_mode)
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
            let binary_overrides = resolve_binary_overrides(
                options,
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
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
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
            format,
        } => {
            let binary_overrides =
                resolve_binary_overrides(options, &[("--scancel-bin", &scancel_bin)]);
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::cancel(context, job_id, purge_cache, format)
        }
        Commands::Down {
            file,
            job_id,
            scancel_bin,
            purge_cache,
            format,
        } => {
            let binary_overrides =
                resolve_binary_overrides(options, &[("--scancel-bin", &scancel_bin)]);
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::cancel(context, job_id, purge_cache, format)
        }
        Commands::Run {
            file,
            service,
            cmd,
            script_out,
            sbatch_bin,
            srun_bin,
            enroot_bin,
            apptainer_bin,
            singularity_bin,
            squeue_bin,
            sacct_bin,
            keep_failed_prep,
            skip_prepare,
            force_rebuild,
            no_preflight,
        } => {
            let binary_overrides = resolve_binary_overrides(
                options,
                &[
                    ("--enroot-bin", &enroot_bin),
                    ("--sbatch-bin", &sbatch_bin),
                    ("--srun-bin", &srun_bin),
                    ("--squeue-bin", &squeue_bin),
                    ("--sacct-bin", &sacct_bin),
                    ("--apptainer-bin", &apptainer_bin),
                    ("--singularity-bin", &singularity_bin),
                ],
            );
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::run_service(
                context,
                service,
                cmd,
                script_out,
                keep_failed_prep,
                skip_prepare,
                force_rebuild,
                no_preflight,
                options.quiet,
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
        Commands::Cache { command } => match command {
            CacheCommands::List { cache_dir, format } => cache::list(cache_dir, format),
            CacheCommands::Inspect {
                file,
                service,
                format,
            } => {
                let context = resolve_command_context(options, file, BinaryOverrides::default())?;
                cache::inspect(context, service, format)
            }
            CacheCommands::Prune {
                file,
                cache_dir,
                age,
                all_unused,
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
                    let context =
                        resolve_command_context(options, Some(file), BinaryOverrides::default())?;
                    cache::prune(context, cache_dir, age, all_unused, format)
                } else if cache_dir.is_none() {
                    let context =
                        resolve_command_context(options, file, BinaryOverrides::default())?;
                    cache::prune(context, cache_dir, age, all_unused, format)
                } else {
                    cache::prune_no_context(cache_dir, age, format)
                }
            }
        },
        Commands::Jobs { command } => match command {
            JobsCommands::List { disk_usage, format } => runtime::jobs_list(disk_usage, format),
        },
        Commands::Clean {
            file,
            age,
            all,
            dry_run,
            disk_usage,
            format,
        } => {
            if age.is_none() && !all {
                bail!("clean requires either --age DAYS or --all");
            }
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            runtime::clean(context, age, all, dry_run, disk_usage, format)
        }
        Commands::Context { format } => {
            let context = resolve_command_context(options, None, BinaryOverrides::default())?;
            spec::context(context, format)
        }
        Commands::Setup {
            profile_name,
            compose_file,
            env_files,
            env,
            binaries,
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
            default_profile,
            non_interactive,
            format,
        ),
        Commands::Completions { shell } => init::completions(shell),
    }
}

fn resolve_watch_mode(watch_mode: WatchMode, no_tui: bool) -> Result<WatchMode> {
    if no_tui {
        if watch_mode != WatchMode::Auto {
            bail!("--no-tui cannot be combined with --watch-mode");
        }
        Ok(WatchMode::Line)
    } else {
        Ok(watch_mode)
    }
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
            timeout_seconds,
        } => {
            let context = resolve_command_context(options, file, binary_overrides)?;
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
            timeout_seconds,
        } => {
            let context = resolve_command_context(options, file, binary_overrides)?;
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
    }
}

fn resolve_command_context(
    options: &GlobalCommandOptions,
    compose_file: Option<PathBuf>,
    binary_overrides: BinaryOverrides,
) -> Result<ResolvedContext> {
    let cwd = env::current_dir().context("failed to determine current working directory")?;
    resolve(&ResolveRequest {
        cwd,
        profile: options.profile.clone(),
        settings_file: options.settings_file.clone(),
        compose_file_override: compose_file,
        binary_overrides,
    })
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

fn print_schema() -> Result<()> {
    let mut stdout = io::stdout();
    stdout
        .write_all(hpc_compose::schema::schema_json().as_bytes())
        .context("failed to write schema to stdout")?;
    if !hpc_compose::schema::schema_json().ends_with('\n') {
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

        let missing_cache_strategy = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: None,
                age: None,
                all_unused: false,
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
}
