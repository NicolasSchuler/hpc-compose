use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{CacheCommands, Cli, Commands, JobsCommands};
use hpc_compose::context::{BinaryOverrides, ResolveRequest, ResolvedContext, resolve};

mod cache;
mod init;
mod runtime;
mod spec;

#[derive(Debug, Clone, Default)]
struct GlobalCommandOptions {
    profile: Option<String>,
    settings_file: Option<PathBuf>,
    raw_args: Vec<OsString>,
    assume_explicit_values: bool,
}

/// Dispatches a parsed CLI invocation using the provided raw argument vector.
pub fn run_cli(cli: Cli, raw_args: &[OsString]) -> Result<()> {
    run_command_with_options(
        cli.command,
        &GlobalCommandOptions {
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
            keep_failed_prep,
            force,
            format,
        } => {
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--enroot-bin") {
                binary_overrides.enroot = Some(enroot_bin);
            }
            let context = resolve_command_context(options, file, binary_overrides)?;
            spec::prepare(context, keep_failed_prep, force, format)
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
        } => {
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--enroot-bin") {
                binary_overrides.enroot = Some(enroot_bin);
            }
            if value_is_explicit(options, "--sbatch-bin") {
                binary_overrides.sbatch = Some(sbatch_bin);
            }
            if value_is_explicit(options, "--srun-bin") {
                binary_overrides.srun = Some(srun_bin);
            }
            let context = resolve_command_context(options, file, binary_overrides)?;
            spec::preflight(context, strict, verbose, format, json)
        }
        Commands::Inspect {
            file,
            verbose,
            format,
            json,
        } => {
            let context = resolve_command_context(options, file, BinaryOverrides::default())?;
            spec::inspect(context, verbose, format, json)
        }
        Commands::Submit {
            file,
            script_out,
            sbatch_bin,
            srun_bin,
            enroot_bin,
            squeue_bin,
            sacct_bin,
            keep_failed_prep,
            skip_prepare,
            force_rebuild,
            no_preflight,
            watch,
            local,
            dry_run,
            format,
        } => {
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--enroot-bin") {
                binary_overrides.enroot = Some(enroot_bin);
            }
            if value_is_explicit(options, "--sbatch-bin") {
                binary_overrides.sbatch = Some(sbatch_bin);
            }
            if value_is_explicit(options, "--srun-bin") {
                binary_overrides.srun = Some(srun_bin);
            }
            if value_is_explicit(options, "--squeue-bin") {
                binary_overrides.squeue = Some(squeue_bin);
            }
            if value_is_explicit(options, "--sacct-bin") {
                binary_overrides.sacct = Some(sacct_bin);
            }
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::submit(
                context,
                script_out,
                keep_failed_prep,
                skip_prepare,
                force_rebuild,
                no_preflight,
                watch,
                local,
                dry_run,
                format,
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
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--squeue-bin") {
                binary_overrides.squeue = Some(squeue_bin);
            }
            if value_is_explicit(options, "--sacct-bin") {
                binary_overrides.sacct = Some(sacct_bin);
            }
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
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--sstat-bin") {
                binary_overrides.sstat = Some(sstat_bin);
            }
            if value_is_explicit(options, "--squeue-bin") {
                binary_overrides.squeue = Some(squeue_bin);
            }
            if value_is_explicit(options, "--sacct-bin") {
                binary_overrides.sacct = Some(sacct_bin);
            }
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
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--squeue-bin") {
                binary_overrides.squeue = Some(squeue_bin);
            }
            if value_is_explicit(options, "--sacct-bin") {
                binary_overrides.sacct = Some(sacct_bin);
            }
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
        } => {
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--squeue-bin") {
                binary_overrides.squeue = Some(squeue_bin);
            }
            if value_is_explicit(options, "--sacct-bin") {
                binary_overrides.sacct = Some(sacct_bin);
            }
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::watch(context, job_id, service, lines)
        }
        Commands::Cancel {
            file,
            job_id,
            scancel_bin,
            format,
        } => {
            let mut binary_overrides = BinaryOverrides::default();
            if value_is_explicit(options, "--scancel-bin") {
                binary_overrides.scancel = Some(scancel_bin);
            }
            let context = resolve_command_context(options, file, binary_overrides)?;
            runtime::cancel(context, job_id, format)
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
