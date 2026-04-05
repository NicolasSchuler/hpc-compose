use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use hpc_compose::cache::{
    CacheEntryKind, load_manifest_if_exists, prune_all_unused, prune_by_age, scan_cache,
};
use hpc_compose::init::{
    default_cache_dir as default_init_cache_dir, next_commands, prompt_for_init, render_template,
    resolve_template, write_initialized_template,
};
use hpc_compose::job::{
    ArtifactExportOptions, ArtifactExportReport, SchedulerOptions, StatsOptions, StatsSnapshot,
    StatusSnapshot, WatchOutcome, build_stats_snapshot, build_status_snapshot,
    build_submission_record, clean_all_except_latest, clean_by_age, export_artifacts,
    load_submission_record, print_logs, scheduler_source_label, watch_submission,
    write_submission_record,
};
use hpc_compose::planner::{
    ExecutionSpec, ImageSource, Plan, build_plan, registry_host_for_remote,
};
use hpc_compose::preflight::{Options as PreflightOptions, Report, run as run_preflight};
use hpc_compose::prepare::{
    ArtifactAction, PrepareOptions, PrepareSummary, RuntimePlan, RuntimeService, base_image_path,
    build_runtime_plan, prepare_runtime_plan,
};
use hpc_compose::render::{
    build_srun_command, execution_argv, log_file_name_for_service, render_script,
};
use hpc_compose::spec::{ComposeSpec, DependencyCondition, ServiceDependency};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Compile a compose-like spec into a single Slurm job using Enroot"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum StatsOutputFormat {
    Text,
    Json,
    Csv,
    Jsonl,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Validate {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
    },
    Render {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    Prepare {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long)]
        keep_failed_prep: bool,
        #[arg(long)]
        force: bool,
    },
    Preflight {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        strict: bool,
        #[arg(long)]
        verbose: bool,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long, default_value = "sbatch")]
        sbatch_bin: String,
        #[arg(long, default_value = "srun")]
        srun_bin: String,
    },
    Inspect {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        verbose: bool,
        #[arg(long)]
        json: bool,
    },
    Submit {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        script_out: Option<PathBuf>,
        #[arg(long, default_value = "sbatch")]
        sbatch_bin: String,
        #[arg(long, default_value = "srun")]
        srun_bin: String,
        #[arg(long, default_value = "enroot")]
        enroot_bin: String,
        #[arg(long, default_value = "squeue")]
        squeue_bin: String,
        #[arg(long, default_value = "sacct")]
        sacct_bin: String,
        #[arg(long)]
        keep_failed_prep: bool,
        #[arg(long)]
        skip_prepare: bool,
        #[arg(long)]
        force_rebuild: bool,
        #[arg(long)]
        no_preflight: bool,
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        dry_run: bool,
    },
    Status {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "squeue")]
        squeue_bin: String,
        #[arg(long, default_value = "sacct")]
        sacct_bin: String,
    },
    Stats {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, conflicts_with = "format")]
        json: bool,
        #[arg(long, value_enum)]
        format: Option<StatsOutputFormat>,
        #[arg(long, default_value = "sstat")]
        sstat_bin: String,
        #[arg(long, default_value = "squeue")]
        squeue_bin: String,
        #[arg(long, default_value = "sacct")]
        sacct_bin: String,
    },
    Artifacts {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long = "bundle")]
        bundles: Vec<String>,
        #[arg(long)]
        tarball: bool,
    },
    Logs {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long)]
        service: Option<String>,
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
    Cancel {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, default_value = "scancel")]
        scancel_bin: String,
    },
    Init {
        #[arg(long)]
        template: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long, default_value = "compose.yaml")]
        output: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
    Clean {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long, conflicts_with = "all")]
        age: Option<u64>,
        #[arg(long, conflicts_with = "age")]
        all: bool,
    },
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCommands {
    List {
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    Inspect {
        #[arg(short = 'f', long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        service: Option<String>,
    },
    Prune {
        #[arg(short = 'f', long)]
        file: Option<PathBuf>,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        #[arg(long)]
        age: Option<u64>,
        #[arg(long)]
        all_unused: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    run_command(cli.command)
}

fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::Validate { file } => {
            load_runtime_plan(&file)?;
            println!("spec is valid");
        }
        Commands::Render { file, output } => {
            let script = render_from_path(&file)?;
            if let Some(output_path) = output {
                fs::write(&output_path, script).context(format!(
                    "failed to write rendered script to {}",
                    output_path.display()
                ))?;
                println!("{}", output_path.display());
            } else {
                print!("{script}");
            }
        }
        Commands::Prepare {
            file,
            enroot_bin,
            keep_failed_prep,
            force,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;
            let summary = prepare_runtime_plan(
                &runtime_plan,
                &PrepareOptions {
                    enroot_bin,
                    keep_failed_prep,
                    force_rebuild: force,
                },
            )?;
            print_prepare_summary(&summary);
        }
        Commands::Preflight {
            file,
            strict,
            verbose,
            json,
            enroot_bin,
            sbatch_bin,
            srun_bin,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;
            let report = run_preflight(
                &runtime_plan,
                &PreflightOptions {
                    enroot_bin,
                    sbatch_bin,
                    srun_bin,
                    require_submit_tools: true,
                    skip_prepare: false,
                },
            );
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report.grouped())
                        .context("failed to serialize preflight report")?
                );
            } else {
                print_report(&report, verbose);
            }
            if report.has_errors() {
                bail!("preflight failed");
            }
            if strict && report.has_warnings() {
                bail!("preflight reported warnings");
            }
        }
        Commands::Inspect {
            file,
            verbose,
            json,
        } => {
            let (plan, runtime_plan) = load_plan_and_runtime(&file)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&runtime_plan)
                        .context("failed to serialize inspect output")?
                );
            } else if verbose {
                print_plan_inspect_verbose(&plan, &runtime_plan);
            } else {
                print_plan_inspect(&runtime_plan);
            }
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
            dry_run,
        } => {
            let runtime_plan = load_runtime_plan(&file)?;
            let submit_dir =
                env::current_dir().context("failed to determine submit working directory")?;

            if !no_preflight {
                let report = run_preflight(
                    &runtime_plan,
                    &PreflightOptions {
                        enroot_bin: enroot_bin.clone(),
                        sbatch_bin: sbatch_bin.clone(),
                        srun_bin,
                        require_submit_tools: true,
                        skip_prepare,
                    },
                );
                print_report(&report, false);
                if report.has_errors() {
                    bail!("preflight failed; fix the reported errors before submitting");
                }
            }

            if !skip_prepare {
                let summary = prepare_runtime_plan(
                    &runtime_plan,
                    &PrepareOptions {
                        enroot_bin,
                        keep_failed_prep,
                        force_rebuild,
                    },
                )?;
                print_prepare_summary(&summary);
            }

            let script = render_script(&runtime_plan)?;
            let script_path = script_out.unwrap_or_else(|| default_script_path(&file));
            fs::write(&script_path, script).context(format!(
                "failed to write rendered script to {}",
                script_path.display()
            ))?;

            if dry_run {
                println!("  script: {}", script_path.display());
                println!("  cache:  {}", runtime_plan.cache_dir.display());
                println!("dry run: skipping sbatch submission");
                return Ok(());
            }

            let output = Command::new(&sbatch_bin)
                .arg(&script_path)
                .output()
                .context(format!("failed to execute '{sbatch_bin}'"))?;
            if !output.status.success() {
                bail!(
                    "sbatch failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            print!("{stdout}");
            let tracked_submission = if let Some(job_id) = extract_job_id(stdout.trim()) {
                let record = build_submission_record(
                    &file,
                    &submit_dir,
                    &script_path,
                    &runtime_plan,
                    job_id,
                )?;
                let persisted = match write_submission_record(&record) {
                    Ok(()) => true,
                    Err(err) => {
                        let _ = writeln!(
                            io::stderr(),
                            "warning: job submitted, but failed to write tracking metadata: {err}"
                        );
                        let _ = io::stderr().flush();
                        false
                    }
                };
                Some((record, persisted))
            } else {
                None
            };
            print_submit_details(&runtime_plan, &script_path, stdout.trim())?;
            if let Some((record, persisted)) = tracked_submission.as_ref() {
                if *persisted {
                    println!(
                        "tracked job metadata: {}",
                        hpc_compose::job::latest_record_path_for(&record.compose_file).display()
                    );
                } else {
                    println!(
                        "note: tracking metadata could not be written, so later status/logs commands will not auto-discover this submission"
                    );
                }
            } else {
                println!(
                    "note: submit output did not include a numeric Slurm job id, so status/logs/watch are not trackable for this submission"
                );
            }
            if watch {
                let Some((record, _)) = tracked_submission.as_ref() else {
                    println!("note: skipping watch because the submission is not trackable");
                    return Ok(());
                };
                finish_watch(
                    &record.job_id,
                    watch_submission(
                        record,
                        &SchedulerOptions {
                            squeue_bin,
                            sacct_bin,
                        },
                        100,
                    )?,
                )?;
            }
        }
        Commands::Status {
            file,
            job_id,
            json,
            squeue_bin,
            sacct_bin,
        } => {
            let snapshot = build_status_snapshot(
                &file,
                job_id.as_deref(),
                &SchedulerOptions {
                    squeue_bin,
                    sacct_bin,
                },
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&snapshot)
                        .context("failed to serialize status output")?
                );
            } else {
                print_status_snapshot(&snapshot);
            }
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
            let snapshot = build_stats_snapshot(
                &file,
                job_id.as_deref(),
                &StatsOptions {
                    scheduler: SchedulerOptions {
                        squeue_bin,
                        sacct_bin,
                    },
                    sstat_bin,
                },
            )?;
            let format = if json {
                StatsOutputFormat::Json
            } else {
                format.unwrap_or(StatsOutputFormat::Text)
            };
            match format {
                StatsOutputFormat::Text => print_stats_snapshot(&snapshot),
                StatsOutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&snapshot)
                            .context("failed to serialize stats output")?
                    );
                }
                StatsOutputFormat::Csv => {
                    write_stats_snapshot_csv(&mut io::stdout(), &snapshot)
                        .context("failed to write csv stats output")?;
                }
                StatsOutputFormat::Jsonl => {
                    write_stats_snapshot_jsonl(&mut io::stdout(), &snapshot)
                        .context("failed to write jsonl stats output")?;
                }
            }
        }
        Commands::Artifacts {
            file,
            job_id,
            json,
            bundles,
            tarball,
        } => {
            let report = export_artifacts(
                &file,
                job_id.as_deref(),
                &ArtifactExportOptions {
                    selected_bundles: bundles,
                    tarball,
                },
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .context("failed to serialize artifacts output")?
                );
            } else {
                print_artifact_export_report(&report);
            }
        }
        Commands::Logs {
            file,
            job_id,
            service,
            follow,
            lines,
        } => {
            let record = load_submission_record(&file, job_id.as_deref())?;
            print_logs(&record, service.as_deref(), lines, follow)?;
        }
        Commands::Cancel {
            file,
            job_id,
            scancel_bin,
        } => {
            let resolved_job_id = match job_id {
                Some(job_id) => job_id,
                None => load_submission_record(&file, None)?.job_id,
            };
            cancel_job(&resolved_job_id, &scancel_bin)?;
        }
        Commands::Init {
            template,
            name,
            cache_dir,
            output,
            force,
        } => {
            let answers = resolve_init_answers(template, name, cache_dir, prompt_for_init)?;
            let rendered = render_template(
                &answers.template_name,
                &answers.app_name,
                &answers.cache_dir,
            )?;
            let path = write_initialized_template(&output, &rendered, force)?;
            println!("wrote {}", path.display());
            for command in next_commands(&path) {
                println!("{command}");
            }
        }
        Commands::Cache { command } => match command {
            CacheCommands::List { cache_dir } => {
                let cache_dir = cache_dir.unwrap_or_else(default_cache_dir);
                let manifests = scan_cache(&cache_dir)?;
                if manifests.is_empty() {
                    println!("no cache entries found in {}", cache_dir.display());
                } else {
                    println!("cache dir: {}", cache_dir.display());
                    for manifest in manifests {
                        let kind = match manifest.kind {
                            CacheEntryKind::Base => "base",
                            CacheEntryKind::Prepared => "prepared",
                        };
                        println!(
                            "{kind}\t{}\tservices={}\tsource={}",
                            manifest.artifact_path,
                            manifest.service_names.join(","),
                            manifest.source_image
                        );
                    }
                }
            }
            CacheCommands::Inspect { file, service } => {
                let runtime_plan = load_runtime_plan(&file)?;
                print_cache_inspect(&runtime_plan, service.as_deref())?;
            }
            CacheCommands::Prune {
                file,
                cache_dir,
                age,
                all_unused,
            } => {
                if age.is_none() && !all_unused {
                    bail!("cache prune requires either --age DAYS or --all-unused");
                }
                if age.is_some() && all_unused {
                    bail!("cache prune accepts only one strategy at a time");
                }

                if let Some(days) = age {
                    let target = cache_dir.unwrap_or_else(default_cache_dir);
                    let result = prune_by_age(&target, days)?;
                    print_prune_result(&target, &result.removed);
                } else {
                    let file = file.context("--all-unused requires -f/--file so the current plan can define which artifacts are still referenced")?;
                    let runtime_plan = load_runtime_plan(&file)?;
                    let target = cache_dir.unwrap_or_else(|| runtime_plan.cache_dir.clone());
                    let result = prune_all_unused(&target, &runtime_plan)?;
                    print_prune_result(&target, &result.removed);
                }
            }
        },
        Commands::Clean { file, age, all } => {
            if age.is_none() && !all {
                bail!("clean requires either --age DAYS or --all");
            }
            let result = if let Some(days) = age {
                clean_by_age(&file, days)?
            } else {
                clean_all_except_latest(&file)?
            };
            if result.removed_jobs.is_empty() {
                println!("no job directories to clean");
            } else {
                println!(
                    "removed {} tracked job(s): {}",
                    result.removed_jobs.len(),
                    result.removed_jobs.join(", ")
                );
            }
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "hpc-compose", &mut io::stdout());
        }
    }
    Ok(())
}

fn render_from_path(path: &Path) -> Result<String> {
    let runtime = load_runtime_plan(path)?;
    render_script(&runtime)
}

fn load_plan(path: &Path) -> Result<Plan> {
    let spec = ComposeSpec::load(path)?;
    build_plan(path, spec)
}

fn load_runtime_plan(path: &Path) -> Result<RuntimePlan> {
    let plan = load_plan(path)?;
    Ok(build_runtime_plan(&plan))
}

fn load_plan_and_runtime(path: &Path) -> Result<(Plan, RuntimePlan)> {
    let plan = load_plan(path)?;
    let runtime_plan = build_runtime_plan(&plan);
    Ok((plan, runtime_plan))
}

fn default_script_path(spec_path: &Path) -> PathBuf {
    let parent = spec_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join("hpc-compose.sbatch")
}

fn default_cache_dir() -> PathBuf {
    let home = match env::var_os("HOME") {
        Some(home) => PathBuf::from(home),
        None => PathBuf::from("."),
    };
    home.join(".cache/hpc-compose")
}

fn print_report(report: &Report, verbose: bool) {
    if report.items.is_empty() {
        return;
    }
    let text = if verbose {
        report.render_verbose()
    } else {
        report.render()
    };
    let _ = writeln!(io::stderr(), "{text}");
    let _ = io::stderr().flush();
}

fn print_prepare_summary(summary: &PrepareSummary) {
    for service in &summary.services {
        if let Some(base) = &service.base_image {
            println!(
                "{} service '{}' base image {}: {}",
                action_label(base.action),
                service.service_name,
                artifact_role_label("base"),
                base.path.display()
            );
        }
        println!(
            "{} service '{}' runtime image {}: {}",
            action_label(service.runtime_image.action),
            service.service_name,
            artifact_role_label("runtime"),
            service.runtime_image.path.display()
        );
        if let Some(note) = &service.runtime_image.note {
            println!("note  service '{}': {note}", service.service_name);
        }
    }
}

fn action_label(action: ArtifactAction) -> &'static str {
    match action {
        ArtifactAction::Present => "OK",
        ArtifactAction::Reused => "REUSE",
        ArtifactAction::Built => "BUILD",
    }
}

fn artifact_role_label(name: &str) -> &'static str {
    match name {
        "base" => "cache artifact",
        "runtime" => "artifact",
        _ => "artifact",
    }
}

fn print_status_snapshot(snapshot: &StatusSnapshot) {
    let _ = write_status_snapshot(&mut io::stdout(), snapshot);
}

fn print_stats_snapshot(snapshot: &StatsSnapshot) {
    let _ = write_stats_snapshot(&mut io::stdout(), snapshot);
}

fn print_artifact_export_report(report: &ArtifactExportReport) {
    let _ = write_artifact_export_report(&mut io::stdout(), report);
}

fn print_plan_inspect_verbose(plan: &Plan, runtime_plan: &RuntimePlan) {
    let _ = write_plan_inspect_verbose(&mut io::stdout(), plan, runtime_plan);
}

fn print_plan_inspect(plan: &RuntimePlan) {
    let _ = write_plan_inspect(&mut io::stdout(), plan);
}

fn print_cache_inspect(plan: &RuntimePlan, filter: Option<&str>) -> Result<()> {
    write_cache_inspect(&mut io::stdout(), plan, filter)
}

fn write_status_snapshot(writer: &mut impl Write, snapshot: &StatusSnapshot) -> io::Result<()> {
    writeln!(writer, "job id: {}", snapshot.record.job_id)?;
    writeln!(
        writer,
        "scheduler state: {} ({})",
        snapshot.scheduler.state,
        scheduler_source_label(snapshot.scheduler.source)
    )?;
    if let Some(detail) = &snapshot.scheduler.detail {
        writeln!(writer, "scheduler note: {detail}")?;
    }
    writeln!(
        writer,
        "compose file: {}",
        snapshot.record.compose_file.display()
    )?;
    writeln!(
        writer,
        "script path: {}",
        snapshot.record.script_path.display()
    )?;
    writeln!(writer, "cache dir: {}", snapshot.record.cache_dir.display())?;
    writeln!(writer, "log dir: {}", snapshot.log_dir.display())?;
    if let Some(attempt) = snapshot.attempt {
        writeln!(writer, "attempt: {attempt}")?;
    }
    if let Some(is_resume) = snapshot.is_resume {
        writeln!(writer, "is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &snapshot.resume_dir {
        writeln!(writer, "resume dir: {}", resume_dir.display())?;
    }
    writeln!(
        writer,
        "batch log: {} (present: {}, updated: {})",
        snapshot.batch_log.path.display(),
        yes_no(snapshot.batch_log.present),
        match snapshot.batch_log.updated_age_seconds {
            Some(seconds) => format_age_seconds(seconds),
            None => "unknown".to_string(),
        }
    )?;
    for service in &snapshot.services {
        let age = match service.updated_age_seconds {
            Some(seconds) => format_age_seconds(seconds),
            None => "unknown".to_string(),
        };
        writeln!(
            writer,
            "log  service '{}': {} (present: {}, updated: {})",
            service.service_name,
            service.path.display(),
            yes_no(service.present),
            age
        )?;
        if service.failure_policy_mode.is_some()
            || service.restart_count.is_some()
            || service.max_restarts.is_some()
            || service.last_exit_code.is_some()
        {
            let mode = service.failure_policy_mode.as_deref().unwrap_or("unknown");
            let restart_count = service
                .restart_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let max_restarts = service
                .max_restarts
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let last_exit = service
                .last_exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            writeln!(
                writer,
                "state service '{}': failure_policy={} restarts={}/{} last_exit={}",
                service.service_name, mode, restart_count, max_restarts, last_exit
            )?;
        }
    }
    Ok(())
}

fn write_stats_snapshot(writer: &mut impl Write, snapshot: &StatsSnapshot) -> io::Result<()> {
    writeln!(writer, "job id: {}", snapshot.job_id)?;
    writeln!(
        writer,
        "scheduler state: {} ({})",
        snapshot.scheduler.state,
        scheduler_source_label(snapshot.scheduler.source)
    )?;
    if let Some(detail) = &snapshot.scheduler.detail {
        writeln!(writer, "scheduler note: {detail}")?;
    }
    writeln!(writer, "stats source: {}", snapshot.source)?;
    if let Some(metrics_dir) = &snapshot.metrics_dir {
        writeln!(writer, "metrics dir: {}", metrics_dir.display())?;
    }
    if let Some(attempt) = snapshot.attempt {
        writeln!(writer, "attempt: {attempt}")?;
    }
    if let Some(is_resume) = snapshot.is_resume {
        writeln!(writer, "is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &snapshot.resume_dir {
        writeln!(writer, "resume dir: {}", resume_dir.display())?;
    }
    if let Some(reason) = &snapshot.reason {
        writeln!(writer, "stats reason: {reason}")?;
    }
    for note in &snapshot.notes {
        writeln!(writer, "note: {note}")?;
    }
    if let Some(sampler) = &snapshot.sampler {
        for collector in &sampler.collectors {
            if !collector.enabled {
                continue;
            }
            writeln!(
                writer,
                "collector '{}': {} (last sampled: {})",
                collector.name,
                if collector.available {
                    "available"
                } else {
                    "unavailable"
                },
                collector.last_sampled_at.as_deref().unwrap_or("never")
            )?;
        }
        if let Some(gpu) = &sampler.gpu {
            writeln!(writer)?;
            writeln!(writer, "gpu snapshot: {}", gpu.sampled_at)?;
            for device in &gpu.gpus {
                writeln!(
                    writer,
                    "gpu {}: name={}, util={}, mem util={}, mem={} / {}, temp={}, power={} / {}",
                    display_optional_stats_value(device.index.as_deref()),
                    display_optional_stats_value(device.name.as_deref()),
                    display_optional_stats_value(device.utilization_gpu.as_deref()),
                    display_optional_stats_value(device.utilization_memory.as_deref()),
                    display_optional_stats_value(device.memory_used_mib.as_deref()),
                    display_optional_stats_value(device.memory_total_mib.as_deref()),
                    display_optional_stats_value(device.temperature_c.as_deref()),
                    display_optional_stats_value(device.power_draw_w.as_deref()),
                    display_optional_stats_value(device.power_limit_w.as_deref()),
                )?;
            }
            for process in &gpu.processes {
                writeln!(
                    writer,
                    "gpu process: pid={}, name={}, gpu_uuid={}, mem={}",
                    display_optional_stats_value(process.pid.as_deref()),
                    display_optional_stats_value(process.process_name.as_deref()),
                    display_optional_stats_value(process.gpu_uuid.as_deref()),
                    display_optional_stats_value(process.used_memory_mib.as_deref()),
                )?;
            }
        }
    }
    if !snapshot.available {
        return Ok(());
    }
    for step in &snapshot.steps {
        writeln!(writer)?;
        writeln!(writer, "step: {}", step.step_id)?;
        writeln!(writer, "ntasks: {}", display_stats_value(&step.ntasks))?;
        writeln!(writer, "ave cpu: {}", display_stats_value(&step.ave_cpu))?;
        writeln!(writer, "ave rss: {}", display_stats_value(&step.ave_rss))?;
        writeln!(writer, "max rss: {}", display_stats_value(&step.max_rss))?;
        writeln!(
            writer,
            "alloc tres: {}",
            display_stats_value(&step.alloc_tres)
        )?;
        writeln!(
            writer,
            "tres usage in ave: {}",
            display_stats_value(&step.tres_usage_in_ave)
        )?;
        if let Some(gpu_count) = &step.gpu_count {
            writeln!(writer, "gpu count: {gpu_count}")?;
        }
        if let Some(gpu_util) = &step.gpu_util {
            writeln!(writer, "gpu util: {gpu_util}")?;
        }
        if let Some(gpu_mem) = &step.gpu_mem {
            writeln!(writer, "gpu mem: {gpu_mem}")?;
        }
    }
    Ok(())
}

fn write_stats_snapshot_csv(writer: &mut impl Write, snapshot: &StatsSnapshot) -> io::Result<()> {
    writeln!(
        writer,
        "job_id,scheduler_state,scheduler_source,stats_source,step_id,ntasks,ave_cpu,ave_rss,max_rss,alloc_tres,tres_usage_in_ave,gpu_count,gpu_util,gpu_mem,alloc_tres_map,usage_tres_in_ave_map"
    )?;
    for step in &snapshot.steps {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_field(&snapshot.job_id),
            csv_field(&snapshot.scheduler.state),
            csv_field(scheduler_source_label(snapshot.scheduler.source)),
            csv_field(&snapshot.source),
            csv_field(&step.step_id),
            csv_field(&step.ntasks),
            csv_field(&step.ave_cpu),
            csv_field(&step.ave_rss),
            csv_field(&step.max_rss),
            csv_field(&step.alloc_tres),
            csv_field(&step.tres_usage_in_ave),
            csv_field(step.gpu_count.as_deref().unwrap_or("")),
            csv_field(step.gpu_util.as_deref().unwrap_or("")),
            csv_field(step.gpu_mem.as_deref().unwrap_or("")),
            csv_field(&format_tres_map(&step.alloc_tres_map)),
            csv_field(&format_tres_map(&step.usage_tres_in_ave_map)),
        )?;
    }
    Ok(())
}

fn write_stats_snapshot_jsonl(writer: &mut impl Write, snapshot: &StatsSnapshot) -> io::Result<()> {
    write_jsonl_record(
        writer,
        &serde_json::json!({
            "record_type": "summary",
            "job_id": snapshot.job_id,
            "scheduler_state": snapshot.scheduler.state,
            "scheduler_source": scheduler_source_label(snapshot.scheduler.source),
            "stats_source": snapshot.source,
            "available": snapshot.available,
            "reason": snapshot.reason,
            "metrics_dir": snapshot.metrics_dir,
            "attempt": snapshot.attempt,
            "is_resume": snapshot.is_resume,
            "resume_dir": snapshot.resume_dir,
        }),
    )?;
    for note in &snapshot.notes {
        write_jsonl_record(
            writer,
            &serde_json::json!({
                "record_type": "note",
                "job_id": snapshot.job_id,
                "message": note,
            }),
        )?;
    }
    if let Some(sampler) = &snapshot.sampler {
        for collector in &sampler.collectors {
            write_jsonl_record(
                writer,
                &serde_json::json!({
                    "record_type": "collector",
                    "job_id": snapshot.job_id,
                    "name": collector.name,
                    "enabled": collector.enabled,
                    "available": collector.available,
                    "note": collector.note,
                    "last_sampled_at": collector.last_sampled_at,
                }),
            )?;
        }
        if let Some(gpu) = &sampler.gpu {
            for device in &gpu.gpus {
                write_jsonl_record(
                    writer,
                    &serde_json::json!({
                        "record_type": "gpu_device",
                        "job_id": snapshot.job_id,
                        "sampled_at": gpu.sampled_at,
                        "device": device,
                    }),
                )?;
            }
            for process in &gpu.processes {
                write_jsonl_record(
                    writer,
                    &serde_json::json!({
                        "record_type": "gpu_process",
                        "job_id": snapshot.job_id,
                        "sampled_at": gpu.sampled_at,
                        "process": process,
                    }),
                )?;
            }
        }
    }
    for step in &snapshot.steps {
        write_jsonl_record(
            writer,
            &serde_json::json!({
                "record_type": "step",
                "job_id": snapshot.job_id,
                "scheduler_state": snapshot.scheduler.state,
                "scheduler_source": scheduler_source_label(snapshot.scheduler.source),
                "stats_source": snapshot.source,
                "step": step,
            }),
        )?;
    }
    Ok(())
}

fn write_jsonl_record(writer: &mut impl Write, value: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(io::Error::other)?;
    writeln!(writer)
}

fn csv_field(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn format_tres_map(values: &std::collections::BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn write_artifact_export_report(
    writer: &mut impl Write,
    report: &ArtifactExportReport,
) -> io::Result<()> {
    writeln!(writer, "job id: {}", report.record.job_id)?;
    writeln!(writer, "manifest: {}", report.manifest_path.display())?;
    writeln!(writer, "payload dir: {}", report.payload_dir.display())?;
    writeln!(writer, "export dir: {}", report.export_dir.display())?;
    writeln!(writer, "collect policy: {}", report.manifest.collect_policy)?;
    writeln!(writer, "job outcome: {}", report.manifest.job_outcome)?;
    if let Some(attempt) = report.manifest.attempt {
        writeln!(writer, "attempt: {attempt}")?;
    }
    if let Some(is_resume) = report.manifest.is_resume {
        writeln!(writer, "is resume: {}", yes_no(is_resume))?;
    }
    if let Some(resume_dir) = &report.manifest.resume_dir {
        writeln!(writer, "resume dir: {}", resume_dir.display())?;
    }
    writeln!(
        writer,
        "declared patterns: {}",
        report.manifest.declared_source_patterns.len()
    )?;
    writeln!(
        writer,
        "matched source paths: {}",
        report.manifest.matched_source_paths.len()
    )?;
    writeln!(
        writer,
        "selected bundles: {}",
        report.selected_bundles.join(",")
    )?;
    writeln!(writer, "bundle reports: {}", report.bundles.len())?;
    writeln!(writer, "exported paths: {}", report.exported_paths.len())?;
    for warning in &report.warnings {
        writeln!(writer, "warning: {warning}")?;
    }
    for bundle in &report.bundles {
        writeln!(
            writer,
            "bundle '{}': exported={} provenance={}{}",
            bundle.name,
            bundle.exported_paths.len(),
            bundle.provenance_path.display(),
            match &bundle.tarball_path {
                Some(path) => format!(" tarball={}", path.display()),
                None => String::new(),
            }
        )?;
        for warning in &bundle.warnings {
            writeln!(writer, "bundle warning '{}': {warning}", bundle.name)?;
        }
    }
    for path in &report.exported_paths {
        writeln!(writer, "exported: {}", path.display())?;
    }
    Ok(())
}

fn write_plan_inspect_verbose(
    writer: &mut impl Write,
    plan: &Plan,
    runtime_plan: &RuntimePlan,
) -> io::Result<()> {
    write_plan_inspect(writer, runtime_plan)?;
    writeln!(writer)?;
    writeln!(writer, "compose file: {}", plan.spec_path.display())?;
    writeln!(writer, "project dir: {}", plan.project_dir.display())?;

    for (planned, runtime) in plan
        .ordered_services
        .iter()
        .zip(runtime_plan.ordered_services.iter())
    {
        writeln!(writer)?;
        writeln!(writer, "details for service '{}':", runtime.name)?;
        writeln!(
            writer,
            "execution form: {}",
            execution_form_label(&runtime.execution)
        )?;
        writeln!(
            writer,
            "resolved argv: {}",
            execution_argv(&runtime.execution, runtime.working_dir.as_deref()).join(" ")
        )?;
        writeln!(
            writer,
            "working dir: {}",
            runtime.working_dir.as_deref().unwrap_or("<image default>")
        )?;
        writeln!(writer, "{}", format_mount_block(runtime))?;
        writeln!(writer, "{}", format_environment_block(runtime))?;
        writeln!(
            writer,
            "depends_on: {}",
            if planned.depends_on.is_empty() {
                "0".to_string()
            } else {
                format_dependencies(&planned.depends_on)
            }
        )?;
        writeln!(
            writer,
            "readiness: {}",
            readiness_description(runtime.readiness.as_ref())
        )?;
        writeln!(
            writer,
            "effective srun args: {}",
            build_srun_command(runtime).join(" ")
        )?;
        if let Some(reason) = rebuild_reason(runtime) {
            writeln!(writer, "rebuild reason: {reason}")?;
        }
    }
    Ok(())
}

fn format_mount_block(runtime: &RuntimeService) -> String {
    let mut mounts = Vec::with_capacity(runtime.volumes.len() + 1);
    mounts.push("${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}:/hpc-compose/job".into());
    mounts.extend(runtime.volumes.iter().cloned());
    format_debug_block("mounts", &mounts)
}

fn format_environment_block(runtime: &RuntimeService) -> String {
    let values = runtime
        .environment
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    format_debug_block("environment", &values)
}

fn format_debug_block(label: &str, values: &[String]) -> String {
    if values.is_empty() {
        return format!("{label}: <none>");
    }

    let mut lines = vec![format!("{label}:")];
    for value in values {
        lines.push(format!("  - {value}"));
    }
    lines.join("\n")
}

fn write_plan_inspect(writer: &mut impl Write, plan: &RuntimePlan) -> io::Result<()> {
    writeln!(writer, "name: {}", plan.name)?;
    writeln!(writer, "runtime mode: pyxis")?;
    writeln!(writer, "cache dir: {}", plan.cache_dir.display())?;
    writeln!(
        writer,
        "service order: {}",
        service_names(plan).join(" -> ")
    )?;

    for service in &plan.ordered_services {
        writeln!(writer)?;
        writeln!(writer, "service: {}", service.name)?;
        writeln!(
            writer,
            "source image: {}",
            source_image_display(&service.source)
        )?;
        if let ImageSource::Remote(_) = &service.source {
            let base_path = base_image_path(&plan.cache_dir, service);
            writeln!(writer, "base cache artifact: {}", base_path.display())?;
            writeln!(
                writer,
                "base cache state: {}",
                hit_or_miss(base_path.exists())
            )?;
        }
        writeln!(writer, "runtime image: {}", service.runtime_image.display())?;
        writeln!(
            writer,
            "runtime image state: {}",
            runtime_cache_state(service)
        )?;
        if let Some(prepare) = &service.prepare {
            writeln!(
                writer,
                "prepare commands: {}",
                if prepare.commands.is_empty() {
                    "0".to_string()
                } else {
                    prepare.commands.len().to_string()
                }
            )?;
            if prepare.force_rebuild {
                writeln!(
                    writer,
                    "reuse policy: rebuild on submit because x-enroot.prepare.mounts are present"
                )?;
            } else {
                writeln!(
                    writer,
                    "reuse policy: reuse prepared image when the cached artifact exists"
                )?;
            }
        } else if matches!(service.source, ImageSource::LocalSqsh(_)) {
            writeln!(writer, "reuse policy: uses local .sqsh directly")?;
        } else {
            writeln!(
                writer,
                "reuse policy: reuse imported base image when the cached artifact exists"
            )?;
        }
    }
    Ok(())
}

fn write_cache_inspect(
    writer: &mut impl Write,
    plan: &RuntimePlan,
    filter: Option<&str>,
) -> Result<()> {
    for service in &plan.ordered_services {
        if let Some(filter_name) = filter
            && service.name != filter_name
        {
            continue;
        }

        writeln!(writer, "service: {}", service.name)?;
        writeln!(
            writer,
            "source image: {}",
            source_image_display(&service.source)
        )?;

        if let ImageSource::Remote(remote) = &service.source {
            let base_path = base_image_path(&plan.cache_dir, service);
            writeln!(writer, "base artifact: {}", base_path.display())?;
            writeln!(
                writer,
                "base registry: {}",
                registry_host_for_remote(remote)
            )?;
            write_manifest_block(writer, &base_path)?;
        }

        writeln!(
            writer,
            "runtime artifact: {}",
            service.runtime_image.display()
        )?;
        write_manifest_block(writer, &service.runtime_image)?;
        writeln!(
            writer,
            "current reuse expectation: {}",
            runtime_cache_state(service)
        )?;
        if let Some(prepare) = &service.prepare
            && prepare.force_rebuild
        {
            writeln!(
                writer,
                "note: this service rebuilds on submit because prepare.mounts are present"
            )?;
        }
        writeln!(writer)?;
    }
    Ok(())
}

#[cfg(test)]
fn print_manifest_block(path: &Path) -> Result<()> {
    write_manifest_block(&mut io::stdout(), path)
}

fn write_manifest_block(writer: &mut impl Write, path: &Path) -> Result<()> {
    writeln!(writer, "artifact present: {}", yes_no(path.exists()))?;
    let manifest_path = hpc_compose::cache::manifest_path_for(path);
    writeln!(writer, "manifest path: {}", manifest_path.display())?;
    if let Some(manifest) = load_manifest_if_exists(path)? {
        let kind = match manifest.kind {
            CacheEntryKind::Base => "base",
            CacheEntryKind::Prepared => "prepared",
        };
        writeln!(writer, "manifest kind: {kind}")?;
        writeln!(writer, "manifest cache key: {}", manifest.cache_key)?;
        writeln!(writer, "manifest source: {}", manifest.source_image)?;
        writeln!(
            writer,
            "manifest services: {}",
            manifest.service_names.join(",")
        )?;
        writeln!(writer, "manifest created_at: {}", manifest.created_at)?;
        writeln!(writer, "manifest last_used_at: {}", manifest.last_used_at)?;
        if manifest.kind == CacheEntryKind::Prepared {
            writeln!(
                writer,
                "prepare root: {}",
                manifest.prepare_root.unwrap_or(true)
            )?;
            writeln!(
                writer,
                "prepare commands: {}",
                if manifest.prepare_commands.is_empty() {
                    "0".to_string()
                } else {
                    manifest.prepare_commands.join(" | ")
                }
            )?;
            writeln!(
                writer,
                "force rebuild due to mounts: {}",
                yes_no(manifest.force_rebuild_due_to_mounts)
            )?;
        }
    } else {
        writeln!(writer, "manifest present: no")?;
    }
    Ok(())
}

fn runtime_cache_state(service: &hpc_compose::prepare::RuntimeService) -> &'static str {
    if let Some(prepare) = &service.prepare {
        if prepare.force_rebuild {
            "rebuild on submit"
        } else if service.runtime_image.exists() {
            "cache hit"
        } else {
            "cache miss"
        }
    } else {
        match &service.source {
            ImageSource::LocalSqsh(path) => {
                if path.exists() {
                    "local image present"
                } else {
                    "local image missing"
                }
            }
            ImageSource::Remote(_) => {
                if service.runtime_image.exists() {
                    "cache hit"
                } else {
                    "cache miss"
                }
            }
        }
    }
}

fn source_image_display(source: &ImageSource) -> String {
    match source {
        ImageSource::LocalSqsh(path) => path.display().to_string(),
        ImageSource::Remote(remote) => remote.clone(),
    }
}

fn hit_or_miss(exists: bool) -> &'static str {
    if exists { "cache hit" } else { "cache miss" }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn display_stats_value(value: &str) -> &str {
    if value.is_empty() { "unknown" } else { value }
}

fn display_optional_stats_value(value: Option<&str>) -> &str {
    match value {
        Some(value) if !value.is_empty() => value,
        _ => "unknown",
    }
}

fn service_names(plan: &RuntimePlan) -> Vec<&str> {
    plan.ordered_services
        .iter()
        .map(|service| service.name.as_str())
        .collect()
}

fn resolve_init_answers(
    template: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    prompt_for_answers: impl FnOnce() -> Result<hpc_compose::init::InitAnswers>,
) -> Result<hpc_compose::init::InitAnswers> {
    if let Some(template_name) = template {
        let template = resolve_template(&template_name)?;
        Ok(hpc_compose::init::InitAnswers {
            template_name: template.name.to_string(),
            app_name: match name {
                Some(name) => name,
                None => template.name.to_string(),
            },
            cache_dir: match cache_dir {
                Some(cache_dir) => cache_dir,
                None => default_init_cache_dir().to_string(),
            },
        })
    } else {
        let mut answers = prompt_for_answers()?;
        if let Some(name) = name {
            answers.app_name = name;
        }
        if let Some(cache_dir) = cache_dir {
            answers.cache_dir = cache_dir;
        }
        Ok(answers)
    }
}

fn print_submit_details(plan: &RuntimePlan, script_path: &Path, sbatch_stdout: &str) -> Result<()> {
    println!("rendered script: {}", script_path.display());
    println!("cache dir: {}", plan.cache_dir.display());

    let submit_dir = env::current_dir().context("failed to determine submit working directory")?;
    if let Some(job_id) = extract_job_id(sbatch_stdout) {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}",
                service.name,
                submit_dir
                    .join(".hpc-compose")
                    .join(job_id)
                    .join("logs")
                    .join(log_file_name_for_service(&service.name))
                    .display()
            );
        }
    } else {
        for service in &plan.ordered_services {
            println!(
                "log  service '{}': {}/.hpc-compose/<job-id>/logs/{}.log",
                service.name,
                submit_dir.display(),
                log_file_name_for_service(&service.name)
            );
        }
    }
    Ok(())
}

fn extract_job_id(text: &str) -> Option<&str> {
    text.split_whitespace()
        .rev()
        .find(|token| token.chars().all(|ch| ch.is_ascii_digit()))
}

fn print_prune_result(cache_dir: &Path, removed: &[PathBuf]) {
    println!("cache dir: {}", cache_dir.display());
    if removed.is_empty() {
        println!("removed: 0");
        return;
    }
    println!("removed: {}", removed.len());
    for path in removed {
        println!("pruned: {}", path.display());
    }
}

fn cancel_job(job_id: &str, scancel_bin: &str) -> Result<()> {
    let output = Command::new(scancel_bin)
        .arg(job_id)
        .output()
        .context(format!("failed to execute '{scancel_bin}'"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("scancel failed for job {job_id}");
        }
        bail!("scancel failed for job {job_id}: {detail}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        println!("{stdout}");
    }
    println!("cancelled job: {job_id}");
    Ok(())
}

fn finish_watch(job_id: &str, outcome: WatchOutcome) -> Result<()> {
    match outcome {
        WatchOutcome::Completed(_) => Ok(()),
        WatchOutcome::Unknown(status) => {
            if let Some(detail) = status.detail {
                bail!(
                    "job {} could not be tracked to a terminal scheduler state ({}): {}",
                    job_id,
                    status.state,
                    detail
                );
            }
            bail!(
                "job {} could not be tracked to a terminal scheduler state ({})",
                job_id,
                status.state
            );
        }
        WatchOutcome::Failed(status) => {
            bail!(
                "job {} finished in scheduler state {}",
                job_id,
                status.state
            )
        }
    }
}

fn execution_form_label(execution: &ExecutionSpec) -> &'static str {
    match execution {
        ExecutionSpec::ImageDefault => "image-default",
        ExecutionSpec::Shell(_) => "shell",
        ExecutionSpec::Exec(_) => "exec",
    }
}

fn readiness_description(readiness: Option<&hpc_compose::spec::ReadinessSpec>) -> String {
    match readiness {
        None => "none".to_string(),
        Some(hpc_compose::spec::ReadinessSpec::Sleep { seconds }) => {
            format!("sleep {}s", seconds)
        }
        Some(hpc_compose::spec::ReadinessSpec::Tcp {
            host,
            port,
            timeout_seconds,
        }) => format!(
            "tcp {}:{} (timeout {}s)",
            host.as_deref().unwrap_or("127.0.0.1"),
            port,
            timeout_seconds.unwrap_or(60)
        ),
        Some(hpc_compose::spec::ReadinessSpec::Log {
            pattern,
            timeout_seconds,
        }) => format!(
            "log '{}' (timeout {}s)",
            pattern,
            timeout_seconds.unwrap_or(60)
        ),
        Some(hpc_compose::spec::ReadinessSpec::Http {
            url,
            status_code,
            timeout_seconds,
        }) => format!(
            "http {} (status {} timeout {}s)",
            url,
            status_code,
            timeout_seconds.unwrap_or(60)
        ),
    }
}

fn rebuild_reason(service: &hpc_compose::prepare::RuntimeService) -> Option<&'static str> {
    let prepare = service.prepare.as_ref()?;
    if prepare.force_rebuild {
        Some("x-enroot.prepare.mounts are present")
    } else if !service.runtime_image.exists() {
        Some("runtime cache artifact is missing")
    } else {
        None
    }
}

fn format_dependencies(dependencies: &[ServiceDependency]) -> String {
    let mut formatted = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let condition = match dependency.condition {
            DependencyCondition::ServiceStarted => "service_started",
            DependencyCondition::ServiceHealthy => "service_healthy",
        };
        formatted.push(format!("{}({condition})", dependency.name));
    }
    formatted.join(",")
}

fn format_age_seconds(seconds: u64) -> String {
    match seconds {
        0..=59 => format!("{seconds}s ago"),
        60..=3599 => format!("{}m ago", seconds / 60),
        3600..=86_399 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use super::*;
    use hpc_compose::cache::{CacheEntryKind, CacheEntryManifest};
    use hpc_compose::job::{
        ArtifactExportReport, ArtifactManifest, BatchLogStatus, CollectorStatus, GpuDeviceSample,
        GpuProcessSample, GpuSnapshot, SamplerSnapshot, SchedulerSource, SchedulerStatus,
        ServiceLogStatus, StatsSnapshot, StatusSnapshot, StepStats, SubmissionRecord,
    };
    use hpc_compose::planner::{ExecutionSpec, ImageSource, PreparedImageSpec};
    use hpc_compose::spec::{
        DependencyCondition, ReadinessSpec, ServiceDependency, ServiceFailurePolicy,
        ServiceSlurmConfig, SlurmConfig,
    };

    fn runtime_service(
        source: ImageSource,
        runtime_image: PathBuf,
        prepare: Option<PreparedImageSpec>,
    ) -> hpc_compose::prepare::RuntimeService {
        hpc_compose::prepare::RuntimeService {
            name: "svc/name".into(),
            runtime_image,
            execution: ExecutionSpec::Shell("echo hi".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            failure_policy: ServiceFailurePolicy::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare,
            source,
        }
    }

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).expect("write script");
        let mut perms = fs::metadata(path).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("fake-enroot.sh");
        write_script(
            &path,
            r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name) name="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start) exit 0 ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output) output="$2"; shift 2 ;;
        -f|--force) shift ;;
        *) shift ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove) exit 0 ;;
esac
"#,
        );
        path
    }

    fn write_fake_sbatch(tmpdir: &Path, success: bool) -> PathBuf {
        let path = tmpdir.join(if success { "sbatch-ok" } else { "sbatch-fail" });
        let body = if success {
            "#!/bin/bash\nset -euo pipefail\necho 'Submitted batch job 54321'\n"
        } else {
            "#!/bin/bash\nset -euo pipefail\necho 'boom' >&2\nexit 2\n"
        };
        write_script(&path, body);
        path
    }

    fn write_fake_srun(tmpdir: &Path) -> PathBuf {
        let path = tmpdir.join("srun");
        write_script(
            &path,
            "#!/bin/bash\nset -euo pipefail\nif [[ \"${1:-}\" == \"--help\" ]]; then echo 'usage --container-image'; fi\n",
        );
        path
    }

    fn write_compose(tmpdir: &Path, body: &str) -> PathBuf {
        let path = tmpdir.join("compose.yaml");
        fs::write(&path, body).expect("compose");
        path
    }

    fn safe_cache_dir() -> tempfile::TempDir {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
        fs::create_dir_all(&root).expect("cache root");
        tempfile::Builder::new()
            .prefix("case-")
            .tempdir_in(root)
            .expect("cache tempdir")
    }

    fn write_valid_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
        fs::create_dir_all(tmpdir.join("app")).expect("app");
        fs::write(tmpdir.join("app/main.py"), "print('hi')\n").expect("main.py");
        write_compose(
            tmpdir,
            &format!(
                r#"
name: demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install click
"#,
                cache_dir.display()
            ),
        )
    }

    fn submission_record(tmpdir: &Path, plan: &RuntimePlan, job_id: &str) -> SubmissionRecord {
        hpc_compose::job::build_submission_record(
            &tmpdir.join("compose.yaml"),
            tmpdir,
            &tmpdir.join("job.sbatch"),
            plan,
            job_id,
        )
        .expect("record")
    }

    fn sample_step() -> StepStats {
        let mut alloc_tres_map = BTreeMap::new();
        alloc_tres_map.insert("gres/gpu".into(), "1".into());
        let mut usage_tres_map = BTreeMap::new();
        usage_tres_map.insert("gres/gpuutil".into(), "87".into());
        usage_tres_map.insert("gres/gpumem".into(), "4096M".into());
        StepStats {
            step_id: "12345.0".into(),
            ntasks: "1".into(),
            ave_cpu: "00:00:03".into(),
            ave_rss: "128M".into(),
            max_rss: "256M".into(),
            alloc_tres: "cpu=1,gres/gpu=1".into(),
            tres_usage_in_ave: "cpu=00:00:03,gres/gpuutil=87,gres/gpumem=4096M".into(),
            alloc_tres_map,
            usage_tres_in_ave_map: usage_tres_map,
            gpu_count: Some("1".into()),
            gpu_util: Some("87".into()),
            gpu_mem: Some("4096M".into()),
        }
    }

    #[test]
    fn action_and_label_helpers_cover_all_variants() {
        assert_eq!(action_label(ArtifactAction::Present), "OK");
        assert_eq!(action_label(ArtifactAction::Reused), "REUSE");
        assert_eq!(action_label(ArtifactAction::Built), "BUILD");
        assert_eq!(artifact_role_label("base"), "cache artifact");
        assert_eq!(artifact_role_label("runtime"), "artifact");
        assert_eq!(artifact_role_label("other"), "artifact");
        assert_eq!(hit_or_miss(true), "cache hit");
        assert_eq!(hit_or_miss(false), "cache miss");
        assert_eq!(yes_no(true), "yes");
        assert_eq!(yes_no(false), "no");
    }

    #[test]
    fn sanitize_and_extract_job_id_work() {
        assert_eq!(
            log_file_name_for_service("svc/name.with spaces"),
            "svc_x2f_name_x2e_with_x20_spaces.log"
        );
        assert_eq!(extract_job_id("Submitted batch job 12345"), Some("12345"));
        assert_eq!(extract_job_id("no job id here"), None);
    }

    #[test]
    fn finish_watch_requires_a_terminal_scheduler_result() {
        finish_watch(
            "12345",
            WatchOutcome::Completed(hpc_compose::job::SchedulerStatus {
                state: "COMPLETED".into(),
                source: hpc_compose::job::SchedulerSource::Sacct,
                terminal: true,
                failed: false,
                detail: None,
            }),
        )
        .expect("completed watch");

        let err = finish_watch(
            "12345",
            WatchOutcome::Unknown(hpc_compose::job::SchedulerStatus {
                state: "unknown".into(),
                source: hpc_compose::job::SchedulerSource::LocalOnly,
                terminal: false,
                failed: false,
                detail: Some("scheduler tools were unavailable".into()),
            }),
        )
        .expect_err("unknown watch should fail");
        assert!(err.to_string().contains("could not be tracked"));
        assert!(err.to_string().contains("scheduler tools were unavailable"));
    }

    #[test]
    fn runtime_cache_state_covers_prepare_and_local_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        let remote_sqsh = tmpdir.path().join("remote.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        std::fs::write(&remote_sqsh, "x").expect("remote");

        let with_forced_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        assert_eq!(
            runtime_cache_state(&with_forced_prepare),
            "rebuild on submit"
        );

        let with_cached_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            remote_sqsh.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&with_cached_prepare), "cache hit");

        let missing_prepare = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("prepared-missing.sqsh"),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(runtime_cache_state(&missing_prepare), "cache miss");

        let local_present = runtime_service(
            ImageSource::LocalSqsh(local_sqsh.clone()),
            local_sqsh.clone(),
            None,
        );
        assert_eq!(runtime_cache_state(&local_present), "local image present");

        let local_missing = runtime_service(
            ImageSource::LocalSqsh(tmpdir.path().join("missing.sqsh")),
            tmpdir.path().join("missing.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&local_missing), "local image missing");

        let remote_missing = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            tmpdir.path().join("missing-remote.sqsh"),
            None,
        );
        assert_eq!(runtime_cache_state(&remote_missing), "cache miss");
    }

    #[test]
    fn service_names_collect_in_order() {
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: PathBuf::from("/cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![
                runtime_service(
                    ImageSource::Remote("docker://redis:7".into()),
                    PathBuf::from("/cache/a.sqsh"),
                    None,
                ),
                hpc_compose::prepare::RuntimeService {
                    name: "worker".into(),
                    ..runtime_service(
                        ImageSource::Remote("docker://python:3.11-slim".into()),
                        PathBuf::from("/cache/b.sqsh"),
                        None,
                    )
                },
            ],
        };
        assert_eq!(service_names(&plan), vec!["svc/name", "worker"]);
    }

    #[test]
    fn path_helpers_return_expected_locations() {
        let path = PathBuf::from("/tmp/project/compose.yaml");
        assert_eq!(
            default_script_path(&path),
            PathBuf::from("/tmp/project/hpc-compose.sbatch")
        );
        assert_eq!(
            default_script_path(Path::new("compose.yaml")),
            PathBuf::from("hpc-compose.sbatch")
        );
        assert!(default_cache_dir().ends_with(".cache/hpc-compose"));
        assert_eq!(
            render_from_path(Path::new("/definitely/missing/compose.yaml"))
                .expect_err("missing")
                .to_string(),
            "failed to read spec at /definitely/missing/compose.yaml"
        );
    }

    #[test]
    fn print_helpers_cover_manifest_and_summary_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("prepared.sqsh");
        std::fs::write(&runtime_image, "x").expect("runtime");
        let local_sqsh = tmpdir.path().join("local.sqsh");
        std::fs::write(&local_sqsh, "x").expect("local");
        let manifest = CacheEntryManifest {
            kind: CacheEntryKind::Prepared,
            artifact_path: runtime_image.display().to_string(),
            service_names: vec!["svc/name".into()],
            cache_key: "key".into(),
            source_image: "docker://redis:7".into(),
            registry: Some("registry-1.docker.io".into()),
            prepare_commands: Vec::new(),
            prepare_env: Vec::new(),
            prepare_root: Some(true),
            prepare_mounts: Vec::new(),
            force_rebuild_due_to_mounts: false,
            created_at: 1,
            last_used_at: 1,
            tool_version: "0.1.0".into(),
        };
        let manifest_path = hpc_compose::cache::manifest_path_for(&runtime_image);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");

        let service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service.clone()],
        };
        let local_plan = RuntimePlan {
            name: "local-demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![runtime_service(
                ImageSource::LocalSqsh(local_sqsh.clone()),
                local_sqsh,
                None,
            )],
        };

        print_report(&Report { items: Vec::new() }, false);
        print_report(
            &Report {
                items: vec![hpc_compose::preflight::Item {
                    level: hpc_compose::preflight::Level::Warn,
                    message: "warn".into(),
                    remediation: None,
                }],
            },
            false,
        );
        print_prepare_summary(&PrepareSummary {
            services: vec![hpc_compose::prepare::ServicePrepareResult {
                service_name: service.name.clone(),
                base_image: Some(hpc_compose::prepare::ArtifactStatus {
                    path: tmpdir.path().join("base.sqsh"),
                    action: ArtifactAction::Built,
                    note: None,
                }),
                runtime_image: hpc_compose::prepare::ArtifactStatus {
                    path: runtime_image.clone(),
                    action: ArtifactAction::Reused,
                    note: Some("cached".into()),
                },
            }],
        });
        print_plan_inspect(&plan);
        print_plan_inspect(&local_plan);
        print_cache_inspect(&plan, None).expect("inspect");
        print_cache_inspect(&plan, Some("other")).expect("inspect filtered");
        print_manifest_block(&runtime_image).expect("manifest block");
        print_manifest_block(&tmpdir.path().join("missing.sqsh")).expect("missing manifest block");
        print_prune_result(tmpdir.path(), &[]);
        print_prune_result(tmpdir.path(), std::slice::from_ref(&runtime_image));
        print_submit_details(&plan, Path::new("/tmp/job.sbatch"), "no job id")
            .expect("submit details");
        print_submit_details(
            &plan,
            Path::new("/tmp/job.sbatch"),
            "Submitted batch job 99999",
        )
        .expect("submit details with job id");
        assert_eq!(source_image_display(&service.source), "docker://redis:7");
        assert_eq!(
            source_image_display(&ImageSource::LocalSqsh(PathBuf::from("/tmp/local.sqsh"))),
            "/tmp/local.sqsh"
        );
    }

    #[test]
    fn writer_helpers_cover_status_stats_artifacts_and_verbose_inspect() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("prepared.sqsh");
        fs::write(&runtime_image, "x").expect("runtime");
        let mut service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image,
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: vec!["/host:/mnt".into()],
                env: Vec::new(),
                root: true,
                force_rebuild: true,
            }),
        );
        service.environment = vec![("TOKEN".into(), "secret".into())];
        service.volumes = vec!["./app:/workspace".into()];
        service.working_dir = Some("/workspace".into());
        service.readiness = Some(ReadinessSpec::Http {
            url: "http://127.0.0.1:8000/health".into(),
            status_code: 200,
            timeout_seconds: Some(30),
        });
        service.depends_on = vec![ServiceDependency {
            name: "db".into(),
            condition: DependencyCondition::ServiceHealthy,
        }];

        let plan = RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![service.clone()],
        };
        let record = submission_record(tmpdir.path(), &plan, "12345");
        let status = StatusSnapshot {
            record: record.clone(),
            scheduler: SchedulerStatus {
                state: "COMPLETED".into(),
                source: SchedulerSource::Sacct,
                terminal: true,
                failed: false,
                detail: Some("finished".into()),
            },
            log_dir: tmpdir.path().join(".hpc-compose/12345/logs"),
            batch_log: BatchLogStatus {
                path: tmpdir.path().join("slurm-12345.out"),
                present: true,
                updated_at: Some(1),
                updated_age_seconds: Some(70),
            },
            services: vec![ServiceLogStatus {
                service_name: "svc/name".into(),
                path: tmpdir.path().join(".hpc-compose/12345/logs/svc.log"),
                present: false,
                updated_at: None,
                updated_age_seconds: None,
                failure_policy_mode: Some("restart_on_failure".into()),
                restart_count: Some(1),
                max_restarts: Some(3),
                last_exit_code: Some(0),
            }],
            attempt: Some(1),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/runs/demo")),
        };
        let mut status_out = Vec::new();
        write_status_snapshot(&mut status_out, &status).expect("status");
        let status_text = String::from_utf8(status_out).expect("utf8");
        assert!(status_text.contains("scheduler state: COMPLETED (sacct)"));
        assert!(status_text.contains("scheduler note: finished"));
        assert!(status_text.contains("attempt: 1"));
        assert!(status_text.contains("is resume: yes"));
        assert!(status_text.contains("resume dir: /shared/runs/demo"));
        assert!(status_text.contains("updated: 1m ago"));
        assert!(status_text.contains("updated: unknown"));
        assert!(status_text.contains(
            "state service 'svc/name': failure_policy=restart_on_failure restarts=1/3 last_exit=0"
        ));

        let stats = StatsSnapshot {
            job_id: "12345".into(),
            record: Some(record.clone()),
            metrics_dir: Some(tmpdir.path().join(".hpc-compose/12345/metrics")),
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: Some("visible".into()),
            },
            available: true,
            reason: Some("ignored once available".into()),
            source: "sampler+sstat".into(),
            notes: vec!["note one".into()],
            sampler: Some(SamplerSnapshot {
                interval_seconds: 5,
                collectors: vec![
                    CollectorStatus {
                        name: "gpu".into(),
                        enabled: true,
                        available: true,
                        note: None,
                        last_sampled_at: Some("2026-04-05T10:00:10Z".into()),
                    },
                    CollectorStatus {
                        name: "slurm".into(),
                        enabled: false,
                        available: false,
                        note: None,
                        last_sampled_at: None,
                    },
                ],
                gpu: Some(GpuSnapshot {
                    sampled_at: "2026-04-05T10:00:10Z".into(),
                    gpus: vec![GpuDeviceSample {
                        index: Some("0".into()),
                        uuid: Some("GPU-0".into()),
                        name: Some("A100".into()),
                        utilization_gpu: Some("87".into()),
                        utilization_memory: Some("73".into()),
                        memory_used_mib: Some("4096".into()),
                        memory_total_mib: Some("8192".into()),
                        temperature_c: Some("55".into()),
                        power_draw_w: Some("220".into()),
                        power_limit_w: Some("300".into()),
                    }],
                    processes: vec![GpuProcessSample {
                        gpu_uuid: Some("GPU-0".into()),
                        pid: Some("4242".into()),
                        process_name: Some("python".into()),
                        used_memory_mib: Some("2048".into()),
                    }],
                }),
                slurm: None,
            }),
            steps: vec![sample_step()],
            attempt: Some(1),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/runs/demo")),
        };
        let mut stats_out = Vec::new();
        write_stats_snapshot(&mut stats_out, &stats).expect("stats");
        let stats_text = String::from_utf8(stats_out).expect("utf8");
        assert!(stats_text.contains("collector 'gpu': available"));
        assert!(stats_text.contains("attempt: 1"));
        assert!(stats_text.contains("is resume: yes"));
        assert!(stats_text.contains("resume dir: /shared/runs/demo"));
        assert!(!stats_text.contains("collector 'slurm'"));
        assert!(stats_text.contains("gpu snapshot: 2026-04-05T10:00:10Z"));
        assert!(stats_text.contains("gpu process: pid=4242"));
        assert!(stats_text.contains("gpu count: 1"));

        let mut csv_out = Vec::new();
        write_stats_snapshot_csv(&mut csv_out, &stats).expect("csv");
        let csv_text = String::from_utf8(csv_out).expect("utf8");
        assert!(csv_text.contains("job_id,scheduler_state,scheduler_source,stats_source"));
        assert!(csv_text.contains("\"12345\",\"RUNNING\",\"squeue\",\"sampler+sstat\""));
        assert!(csv_text.contains("\"12345.0\""));

        let mut jsonl_out = Vec::new();
        write_stats_snapshot_jsonl(&mut jsonl_out, &stats).expect("jsonl");
        let jsonl_text = String::from_utf8(jsonl_out).expect("utf8");
        assert!(jsonl_text.contains("\"record_type\":\"summary\""));
        assert!(jsonl_text.contains("\"record_type\":\"collector\""));
        assert!(jsonl_text.contains("\"record_type\":\"gpu_device\""));
        assert!(jsonl_text.contains("\"record_type\":\"gpu_process\""));
        assert!(jsonl_text.contains("\"record_type\":\"step\""));
        assert!(jsonl_text.contains("\"attempt\":1"));
        assert!(jsonl_text.contains("\"is_resume\":true"));

        let unavailable_stats = StatsSnapshot {
            available: false,
            sampler: None,
            steps: Vec::new(),
            source: "sstat".into(),
            notes: Vec::new(),
            reason: Some("job is pending".into()),
            metrics_dir: None,
            record: None,
            job_id: "12345".into(),
            scheduler: SchedulerStatus {
                state: "PENDING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: None,
            },
            attempt: None,
            is_resume: None,
            resume_dir: None,
        };
        let mut unavailable_out = Vec::new();
        write_stats_snapshot(&mut unavailable_out, &unavailable_stats).expect("stats");
        let unavailable_text = String::from_utf8(unavailable_out).expect("utf8");
        assert!(unavailable_text.contains("stats reason: job is pending"));
        assert!(!unavailable_text.contains("step: "));

        let mut unavailable_csv = Vec::new();
        write_stats_snapshot_csv(&mut unavailable_csv, &unavailable_stats).expect("csv");
        assert_eq!(
            String::from_utf8(unavailable_csv).expect("utf8"),
            "job_id,scheduler_state,scheduler_source,stats_source,step_id,ntasks,ave_cpu,ave_rss,max_rss,alloc_tres,tres_usage_in_ave,gpu_count,gpu_util,gpu_mem,alloc_tres_map,usage_tres_in_ave_map\n"
        );

        let report = ArtifactExportReport {
            record: record.clone(),
            manifest_path: tmpdir.path().join("manifest.json"),
            payload_dir: tmpdir.path().join("payload"),
            export_dir: tmpdir.path().join("results"),
            manifest: ArtifactManifest {
                schema_version: 2,
                job_id: "12345".into(),
                collect_policy: "always".into(),
                collected_at: "2026-04-05T10:00:00Z".into(),
                job_outcome: "success".into(),
                attempt: Some(1),
                is_resume: Some(true),
                resume_dir: Some(PathBuf::from("/shared/runs/demo")),
                declared_source_patterns: vec!["/x/**".into()],
                matched_source_paths: vec!["/x/a".into()],
                copied_relative_paths: vec!["a".into()],
                warnings: Vec::new(),
                bundles: BTreeMap::from([(
                    "default".into(),
                    hpc_compose::job::ArtifactBundleManifest {
                        declared_source_patterns: vec!["/x/**".into()],
                        matched_source_paths: vec!["/x/a".into()],
                        copied_relative_paths: vec!["a".into()],
                        warnings: Vec::new(),
                    },
                )]),
            },
            selected_bundles: vec!["default".into()],
            bundles: Vec::new(),
            exported_paths: vec![tmpdir.path().join("results/a")],
            tarball_paths: Vec::new(),
            warnings: vec!["missing optional path".into()],
        };
        let mut report_out = Vec::new();
        write_artifact_export_report(&mut report_out, &report).expect("artifacts");
        let report_text = String::from_utf8(report_out).expect("utf8");
        assert!(report_text.contains("collect policy: always"));
        assert!(report_text.contains("attempt: 1"));
        assert!(report_text.contains("is resume: yes"));
        assert!(report_text.contains("resume dir: /shared/runs/demo"));
        assert!(report_text.contains("warning: missing optional path"));
        assert!(report_text.contains("exported: "));

        let plan_model = hpc_compose::planner::Plan {
            spec_path: tmpdir.path().join("compose.yaml"),
            project_dir: tmpdir.path().to_path_buf(),
            name: "demo".into(),
            cache_dir: tmpdir.path().join("cache"),
            slurm: SlurmConfig::default(),
            ordered_services: vec![hpc_compose::planner::PlannedService {
                name: service.name.clone(),
                image: service.source.clone(),
                execution: service.execution.clone(),
                environment: service.environment.clone(),
                volumes: service.volumes.clone(),
                working_dir: service.working_dir.clone(),
                depends_on: service.depends_on.clone(),
                readiness: service.readiness.clone(),
                failure_policy: service.failure_policy.clone(),
                slurm: service.slurm.clone(),
                prepare: service.prepare.clone(),
            }],
        };
        let mut inspect_out = Vec::new();
        write_plan_inspect_verbose(&mut inspect_out, &plan_model, &plan).expect("inspect");
        let inspect_text = String::from_utf8(inspect_out).expect("utf8");
        assert!(inspect_text.contains("execution form: shell"));
        assert!(inspect_text.contains("depends_on: db(service_healthy)"));
        assert!(
            inspect_text
                .contains("readiness: http http://127.0.0.1:8000/health (status 200 timeout 30s)")
        );
        assert!(inspect_text.contains("rebuild reason: x-enroot.prepare.mounts are present"));
    }

    #[test]
    fn helper_functions_cover_remaining_formatting_paths() {
        assert_eq!(display_stats_value(""), "unknown");
        assert_eq!(display_stats_value("5"), "5");
        assert_eq!(display_optional_stats_value(None), "unknown");
        assert_eq!(display_optional_stats_value(Some("")), "unknown");
        assert_eq!(display_optional_stats_value(Some("x")), "x");
        assert_eq!(
            execution_form_label(&ExecutionSpec::ImageDefault),
            "image-default"
        );
        assert_eq!(
            execution_form_label(&ExecutionSpec::Shell("echo".into())),
            "shell"
        );
        assert_eq!(
            execution_form_label(&ExecutionSpec::Exec(vec!["echo".into()])),
            "exec"
        );
        assert_eq!(readiness_description(None), "none");
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Sleep { seconds: 5 })),
            "sleep 5s"
        );
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Tcp {
                host: None,
                port: 5432,
                timeout_seconds: None,
            })),
            "tcp 127.0.0.1:5432 (timeout 60s)"
        );
        assert_eq!(
            readiness_description(Some(&ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: Some(9),
            })),
            "log 'ready' (timeout 9s)"
        );
        assert_eq!(format_age_seconds(59), "59s ago");
        assert_eq!(format_age_seconds(61), "1m ago");
        assert_eq!(format_age_seconds(7_200), "2h ago");
        assert_eq!(format_age_seconds(172_800), "2d ago");
        assert_eq!(
            format_dependencies(&[
                ServiceDependency {
                    name: "db".into(),
                    condition: DependencyCondition::ServiceStarted,
                },
                ServiceDependency {
                    name: "cache".into(),
                    condition: DependencyCondition::ServiceHealthy,
                },
            ]),
            "db(service_started),cache(service_healthy)"
        );

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let runtime_image = tmpdir.path().join("runtime.sqsh");
        let service = runtime_service(
            ImageSource::Remote("docker://redis:7".into()),
            runtime_image.clone(),
            Some(PreparedImageSpec {
                commands: vec!["echo hi".into()],
                mounts: Vec::new(),
                env: Vec::new(),
                root: true,
                force_rebuild: false,
            }),
        );
        assert_eq!(
            rebuild_reason(&service),
            Some("runtime cache artifact is missing")
        );
        fs::write(&runtime_image, "x").expect("runtime");
        assert_eq!(rebuild_reason(&service), None);
    }

    #[test]
    fn resolve_init_answers_and_cancel_job_cover_remaining_paths() {
        let answers = resolve_init_answers(Some("dev-python-app".into()), None, None, || {
            unreachable!("template path should not prompt")
        })
        .expect("template answers");
        assert_eq!(answers.app_name, "dev-python-app");
        assert_eq!(answers.cache_dir, default_init_cache_dir());

        let prompted =
            resolve_init_answers(None, Some("override".into()), Some("/cache".into()), || {
                Ok(hpc_compose::init::InitAnswers {
                    template_name: "app-redis-worker".into(),
                    app_name: "prompted".into(),
                    cache_dir: "/default".into(),
                })
            })
            .expect("prompted");
        assert_eq!(prompted.app_name, "override");
        assert_eq!(prompted.cache_dir, "/cache");

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let empty_fail = tmpdir.path().join("scancel-empty");
        write_script(&empty_fail, "#!/bin/bash\nset -euo pipefail\nexit 1\n");
        let err = cancel_job("42", empty_fail.to_str().expect("path")).expect_err("empty fail");
        assert_eq!(err.to_string(), "scancel failed for job 42");

        let stderr_fail = tmpdir.path().join("scancel-stderr");
        write_script(
            &stderr_fail,
            "#!/bin/bash\nset -euo pipefail\necho boom >&2\nexit 1\n",
        );
        let err = cancel_job("42", stderr_fail.to_str().expect("path")).expect_err("stderr fail");
        assert!(err.to_string().contains("scancel failed for job 42: boom"));

        let err = cancel_job(
            "42",
            tmpdir.path().join("missing-bin").to_str().expect("path"),
        )
        .expect_err("missing binary");
        assert!(err.to_string().contains("failed to execute"));
    }

    #[test]
    fn run_command_covers_success_and_error_arms() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let cache_root = safe_cache_dir();
        let cache_dir = cache_root.path().to_path_buf();
        let compose = write_valid_compose(tmpdir.path(), &cache_dir);
        let enroot = write_fake_enroot(tmpdir.path());
        let srun = write_fake_srun(tmpdir.path());
        let sbatch_ok = write_fake_sbatch(tmpdir.path(), true);
        let sbatch_fail = write_fake_sbatch(tmpdir.path(), false);
        let empty_cache = tmpdir.path().join("empty-cache");
        fs::create_dir_all(&empty_cache).expect("empty cache");
        let no_id_sbatch = tmpdir.path().join("sbatch-no-id");
        write_script(
            &no_id_sbatch,
            "#!/bin/bash\nset -euo pipefail\necho 'submitted without id'\n",
        );
        let scancel_ok = tmpdir.path().join("scancel-ok");
        write_script(
            &scancel_ok,
            "#!/bin/bash\nset -euo pipefail\necho 'cancel ok'\n",
        );
        let scancel_fail = tmpdir.path().join("scancel-fail");
        write_script(
            &scancel_fail,
            "#!/bin/bash\nset -euo pipefail\necho 'denied' >&2\nexit 1\n",
        );

        run_command(Commands::Validate {
            file: compose.clone(),
        })
        .expect("validate");
        run_command(Commands::Render {
            file: compose.clone(),
            output: None,
        })
        .expect("render stdout");
        let rendered = tmpdir.path().join("rendered.sbatch");
        run_command(Commands::Render {
            file: compose.clone(),
            output: Some(rendered.clone()),
        })
        .expect("render file");
        assert!(rendered.exists());
        let render_err = run_command(Commands::Render {
            file: compose.clone(),
            output: Some(tmpdir.path().join("missing-parent/rendered.sbatch")),
        })
        .expect_err("render write failure");
        assert!(
            render_err
                .to_string()
                .contains("failed to write rendered script")
        );

        run_command(Commands::Prepare {
            file: compose.clone(),
            enroot_bin: enroot.display().to_string(),
            keep_failed_prep: false,
            force: true,
        })
        .expect("prepare");

        let err = run_command(Commands::Preflight {
            file: compose.clone(),
            strict: true,
            verbose: false,
            json: false,
            enroot_bin: enroot.display().to_string(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
        })
        .expect_err("strict warnings");
        assert!(err.to_string().contains("preflight reported warnings"));
        run_command(Commands::Preflight {
            file: compose.clone(),
            strict: false,
            verbose: false,
            json: false,
            enroot_bin: enroot.display().to_string(),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
        })
        .expect("non-strict preflight");

        run_command(Commands::Inspect {
            file: compose.clone(),
            verbose: false,
            json: false,
        })
        .expect("inspect");

        let err = run_command(Commands::Submit {
            file: compose.clone(),
            script_out: None,
            sbatch_bin: sbatch_fail.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
            watch: false,
            dry_run: false,
        })
        .expect_err("sbatch fail");
        assert!(err.to_string().contains("sbatch failed"));

        run_command(Commands::Submit {
            file: compose.clone(),
            script_out: Some(tmpdir.path().join("submit.sbatch")),
            sbatch_bin: sbatch_ok.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: false,
            watch: false,
            dry_run: false,
        })
        .expect("submit");
        run_command(Commands::Submit {
            file: compose.clone(),
            script_out: Some(tmpdir.path().join("submit-no-id.sbatch")),
            sbatch_bin: no_id_sbatch.display().to_string(),
            srun_bin: srun.display().to_string(),
            enroot_bin: enroot.display().to_string(),
            squeue_bin: "squeue".into(),
            sacct_bin: "sacct".into(),
            keep_failed_prep: false,
            skip_prepare: true,
            force_rebuild: false,
            no_preflight: true,
            watch: false,
            dry_run: false,
        })
        .expect("submit without id");

        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(cache_dir.clone()),
            },
        })
        .expect("cache list");
        run_command(Commands::Cache {
            command: CacheCommands::List {
                cache_dir: Some(empty_cache),
            },
        })
        .expect("cache list empty");
        run_command(Commands::Cache {
            command: CacheCommands::Inspect {
                file: compose.clone(),
                service: Some("app".into()),
            },
        })
        .expect("cache inspect");
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir.clone()),
                age: None,
                all_unused: true,
            },
        })
        .expect_err("missing file");
        assert!(err.to_string().contains("--all-unused requires -f/--file"));
        let err = run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(compose.clone()),
                cache_dir: Some(cache_dir.clone()),
                age: Some(7),
                all_unused: true,
            },
        })
        .expect_err("conflicting strategies");
        assert!(
            err.to_string()
                .contains("cache prune accepts only one strategy at a time")
        );
        run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: None,
                cache_dir: Some(cache_dir),
                age: Some(999),
                all_unused: false,
            },
        })
        .expect("prune age");
        run_command(Commands::Cache {
            command: CacheCommands::Prune {
                file: Some(compose.clone()),
                cache_dir: None,
                age: None,
                all_unused: true,
            },
        })
        .expect("prune all unused");

        run_command(Commands::Cancel {
            file: compose.clone(),
            job_id: Some("12345".into()),
            scancel_bin: scancel_ok.display().to_string(),
        })
        .expect("cancel ok");
        let cancel_err = run_command(Commands::Cancel {
            file: compose.clone(),
            job_id: Some("12345".into()),
            scancel_bin: scancel_fail.display().to_string(),
        })
        .expect_err("cancel fail");
        assert!(
            cancel_err
                .to_string()
                .contains("scancel failed for job 12345")
        );

        let init_output = tmpdir.path().join("init-compose.yaml");
        run_command(Commands::Init {
            template: Some("dev-python-app".into()),
            name: Some("custom-init".into()),
            cache_dir: Some("/tmp/custom-cache".into()),
            output: init_output.clone(),
            force: true,
        })
        .expect("init");
        assert!(init_output.exists());
    }
}
