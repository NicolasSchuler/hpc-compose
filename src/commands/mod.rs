use anyhow::{Result, bail};
use hpc_compose::cli::{CacheCommands, Commands};

mod cache;
mod init;
mod runtime;
mod spec;

pub(crate) fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::Validate { file, format } => spec::validate(file, format),
        Commands::Render {
            file,
            output,
            format,
        } => spec::render(file, output, format),
        Commands::Prepare {
            file,
            enroot_bin,
            keep_failed_prep,
            force,
            format,
        } => spec::prepare(file, enroot_bin, keep_failed_prep, force, format),
        Commands::Preflight {
            file,
            strict,
            verbose,
            format,
            json,
            enroot_bin,
            sbatch_bin,
            srun_bin,
        } => spec::preflight(
            file, strict, verbose, format, json, enroot_bin, sbatch_bin, srun_bin,
        ),
        Commands::Inspect {
            file,
            verbose,
            format,
            json,
        } => spec::inspect(file, verbose, format, json),
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
            dry_run,
        } => runtime::submit(
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
            dry_run,
        ),
        Commands::Status {
            file,
            job_id,
            format,
            json,
            squeue_bin,
            sacct_bin,
        } => runtime::status(file, job_id, format, json, squeue_bin, sacct_bin),
        Commands::Stats {
            file,
            job_id,
            json,
            format,
            sstat_bin,
            squeue_bin,
            sacct_bin,
        } => runtime::stats(file, job_id, json, format, sstat_bin, squeue_bin, sacct_bin),
        Commands::Artifacts {
            file,
            job_id,
            format,
            json,
            bundles,
            tarball,
        } => runtime::artifacts(file, job_id, format, json, bundles, tarball),
        Commands::Logs {
            file,
            job_id,
            service,
            follow,
            lines,
        } => runtime::logs(file, job_id, service, follow, lines),
        Commands::Cancel {
            file,
            job_id,
            scancel_bin,
        } => runtime::cancel(file, job_id, scancel_bin),
        Commands::Cache { command } => match command {
            CacheCommands::List { cache_dir, format } => cache::list(cache_dir, format),
            CacheCommands::Inspect {
                file,
                service,
                format,
            } => cache::inspect(file, service, format),
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
                cache::prune(file, cache_dir, age, all_unused, format)
            }
        },
        Commands::Clean { file, age, all } => {
            if age.is_none() && !all {
                bail!("clean requires either --age DAYS or --all");
            }
            runtime::clean(file, age, all)
        }
        Commands::Init {
            template,
            list_templates,
            describe_template,
            name,
            cache_dir,
            output,
            force,
        } => init::init(
            template,
            list_templates,
            describe_template,
            name,
            cache_dir,
            output,
            force,
        ),
        Commands::Completions { shell } => init::completions(shell),
    }
}
