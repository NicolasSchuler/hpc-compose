use std::path::PathBuf;

use clap::{Parser, Subcommand};
use clap_complete::Shell;

use super::help::*;
use super::{OutputFormat, StatsOutputFormat};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Compile a compose-like spec into a single Slurm job using Enroot",
    long_about = "Compile a compose-like specification into one Slurm batch job that launches one or more services through Enroot and Pyxis inside a single allocation. Use up for the normal run, and use config, validate, inspect, preflight, and prepare when adapting or debugging a spec.",
    after_help = TOP_LEVEL_HELP
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "NAME",
        help = "Profile name to load from .hpc-compose/settings.toml"
    )]
    pub profile: Option<String>,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Explicit settings file path; defaults to upward search for .hpc-compose/settings.toml"
    )]
    pub settings_file: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        about = "Validate a compose spec without submitting a job",
        long_about = "Parse, normalize, and validate the compose specification without touching cluster state or submitting a Slurm job.",
        after_help = VALIDATE_HELP
    )]
    Validate {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            help = "Fail when ${VAR:-default} or ${VAR-default} fallbacks are used because VAR is missing"
        )]
        strict_env: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Render the generated sbatch script",
        long_about = "Render the sbatch script produced from the normalized plan. Use this to inspect generated SBATCH directives, srun invocations, mounts, and environment forwarding without submitting the job.",
        after_help = RENDER_HELP
    )]
    Render {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            short,
            long,
            value_name = "OUTPUT",
            help = "Write the rendered batch script to this path instead of stdout"
        )]
        output: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Prepare imported and customized runtime images",
        long_about = "Import base images and build prepared runtime artifacts on the submission host. This is the login-node image preparation phase reused later by submit.",
        after_help = PREPARE_HELP
    )]
    Prepare {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            help = "Keep failed preparation state on disk for later inspection"
        )]
        keep_failed_prep: bool,
        #[arg(
            long,
            help = "Refresh imported and prepared artifacts even when cache entries exist"
        )]
        force: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Check cluster prerequisites on the submission host",
        long_about = "Check whether the submission host and compose specification satisfy the prerequisites for a later submit. This validates required binaries, cache path safety, local mounts, and Pyxis or Slurm availability.",
        after_help = PREFLIGHT_HELP
    )]
    Preflight {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(long, help = "Treat warnings as failures")]
        strict: bool,
        #[arg(long, help = "Show detailed preflight findings")]
        verbose: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(long, hide = true, conflicts_with = "format")]
        json: bool,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
    },
    #[command(
        about = "Inspect the normalized runtime plan",
        long_about = "Show the normalized runtime plan derived from the compose specification. Use verbose mode when you need cache, mount, or resolved environment details.",
        after_help = INSPECT_HELP
    )]
    Inspect {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            help = "Include resolved environment values and final mount mappings"
        )]
        verbose: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(long, hide = true, conflicts_with = "format")]
        json: bool,
    },
    #[command(
        about = "Render the fully interpolated effective config",
        long_about = "Print the normalized effective compose config after interpolation, healthcheck normalization, and default application. Use this to inspect what submit and inspect actually receive.",
        after_help = CONFIG_HELP
    )]
    Config {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Print the hpc-compose JSON Schema",
        long_about = "Print the checked-in JSON Schema for compose authoring tools. Rust validation remains the semantic source of truth.",
        after_help = SCHEMA_HELP
    )]
    Schema,
    #[command(
        about = "Submit, watch, and stream logs in one command",
        long_about = "Run the normal end-to-end workflow: optional preflight, image preparation, script rendering, sbatch submission or local launch, and immediate live watching with log streaming and exit-code propagation.",
        after_help = UP_HELP
    )]
    Up {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path before submission or local launch"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            help = "Keep failed preparation state on disk for later inspection"
        )]
        keep_failed_prep: bool,
        #[arg(long, help = "Skip image preparation and reuse existing artifacts")]
        skip_prepare: bool,
        #[arg(
            long,
            help = "Refresh imported and prepared artifacts before submission"
        )]
        force_rebuild: bool,
        #[arg(long, help = "Skip the preflight phase before submission")]
        no_preflight: bool,
        #[arg(
            long,
            help = "Launch the plan locally with Enroot instead of submitting it to Slurm",
            conflicts_with_all = ["sbatch_bin", "srun_bin", "squeue_bin", "sacct_bin"]
        )]
        local: bool,
        #[arg(
            long,
            help = "Allow submission even when resume config drift is detected"
        )]
        allow_resume_changes: bool,
        #[arg(
            long,
            help = "Print the resume config diff and exit without preparing or submitting"
        )]
        resume_diff_only: bool,
        #[arg(
            long,
            help = "Run preflight, prepare, and render without calling sbatch"
        )]
        dry_run: bool,
    },
    #[command(
        about = "Submit a job and optionally watch it",
        long_about = "Run the end-to-end submission flow: optional preflight, image preparation, script rendering, sbatch submission, and optional live watching of tracked state and logs.",
        after_help = SUBMIT_HELP
    )]
    Submit {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path before submission or local launch"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            help = "Keep failed preparation state on disk for later inspection"
        )]
        keep_failed_prep: bool,
        #[arg(long, help = "Skip image preparation and reuse existing artifacts")]
        skip_prepare: bool,
        #[arg(
            long,
            help = "Refresh imported and prepared artifacts before submission"
        )]
        force_rebuild: bool,
        #[arg(long, help = "Skip the preflight phase before submission")]
        no_preflight: bool,
        #[arg(
            long,
            help = "Poll tracked state and stream logs after submission or local launch"
        )]
        watch: bool,
        #[arg(
            long,
            help = "Launch the plan locally with Enroot instead of submitting it to Slurm",
            conflicts_with_all = ["sbatch_bin", "srun_bin", "squeue_bin", "sacct_bin"]
        )]
        local: bool,
        #[arg(
            long,
            help = "Allow submission even when resume config drift is detected"
        )]
        allow_resume_changes: bool,
        #[arg(
            long,
            help = "Print the resume config diff and exit without preparing or submitting"
        )]
        resume_diff_only: bool,
        #[arg(
            long,
            help = "Run preflight, prepare, and render without calling sbatch"
        )]
        dry_run: bool,
        #[arg(
            long,
            value_enum,
            value_name = "FORMAT",
            help = "Output format for non-watch submits",
            conflicts_with = "watch"
        )]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Show tracked scheduler state and log locations",
        long_about = "Read tracked submission metadata and query the scheduler to show current batch state, runtime paths, log locations, and placement details.",
        after_help = STATUS_HELP
    )]
    Status {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to inspect instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(long, hide = true, conflicts_with = "format")]
        json: bool,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        about = "Show tracked runtime metrics and step stats",
        long_about = "Read tracked metrics and Slurm step statistics for a submitted job. Prefer machine-readable formats when integrating with dashboards or experiment tooling.",
        after_help = STATS_HELP
    )]
    Stats {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to inspect instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(long, hide = true, conflicts_with = "format")]
        json: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<StatsOutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        about = "Export tracked artifact bundles after a run",
        long_about = "Export tracked artifact bundles collected under the tracked job directory into the configured export directory.",
        after_help = ARTIFACTS_HELP
    )]
    Artifacts {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to export instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(long, hide = true, conflicts_with = "format")]
        json: bool,
        #[arg(
            long = "bundle",
            value_name = "NAME",
            help = "Artifact bundle name to export; may be passed multiple times"
        )]
        bundles: Vec<String>,
        #[arg(long, help = "Also create tar.gz archives for exported bundles")]
        tarball: bool,
    },
    #[command(
        about = "Print tracked service logs",
        long_about = "Print tracked service logs from a previous submit. Follow mode tails appended log data as it appears.",
        after_help = LOGS_HELP
    )]
    Logs {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to read instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "Service name whose log should be printed"
        )]
        service: Option<String>,
        #[arg(long, help = "Follow appended log output until interrupted")]
        follow: bool,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of trailing log lines to show before follow mode begins"
        )]
        lines: usize,
    },
    #[command(
        about = "Show tracked per-service runtime state",
        long_about = "Read tracked runtime state, log metadata, and scheduler state for each service in a submitted job. This is the compose-style per-service process view.",
        after_help = PS_HELP
    )]
    Ps {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to inspect instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        about = "Watch a tracked job in a live terminal UI",
        long_about = "Open a live watch view for a tracked job. On TTYs this uses the alternate-screen watch UI; otherwise it falls back to line-oriented scheduler and log streaming.",
        after_help = WATCH_HELP
    )]
    Watch {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to watch instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "Service to focus initially in the watch UI"
        )]
        service: Option<String>,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of trailing log lines to seed into the watch view"
        )]
        lines: usize,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        about = "Cancel a tracked Slurm job",
        long_about = "Cancel a tracked Slurm job by explicit job id or by the latest submission recorded for the compose file.",
        after_help = CANCEL_HELP
    )]
    Cancel {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to cancel instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scancel",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
        #[arg(long, help = "Also purge tracked cached image artifacts for this job")]
        purge_cache: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Cancel a tracked job and clean tracked state",
        long_about = "Cancel a tracked Slurm job by compose context or job id, remove tracked metadata and runtime state for that job, and optionally purge its tracked cached image artifacts.",
        after_help = DOWN_HELP
    )]
    Down {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to cancel instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scancel",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
        #[arg(long, help = "Also purge tracked cached image artifacts for this job")]
        purge_cache: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Run a one-off command in one service environment",
        long_about = "Submit a fresh one-off job using one service's image, environment, mounts, working directory, and prepare rules, then stream logs and propagate the final exit state.",
        after_help = RUN_HELP
    )]
    Run {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(value_name = "SERVICE", help = "Service to run")]
        service: String,
        #[arg(
            value_name = "CMD",
            required = true,
            num_args = 1..,
            trailing_var_arg = true,
            allow_hyphen_values = true,
            help = "Command argv to execute inside the service environment"
        )]
        cmd: Vec<String>,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path before submission"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            help = "Keep failed preparation state on disk for later inspection"
        )]
        keep_failed_prep: bool,
        #[arg(long, help = "Skip image preparation and reuse existing artifacts")]
        skip_prepare: bool,
        #[arg(
            long,
            help = "Refresh imported and prepared artifacts before submission"
        )]
        force_rebuild: bool,
        #[arg(long, help = "Skip the preflight phase before submission")]
        no_preflight: bool,
    },
    #[command(
        about = "Write a starter compose file from a built-in template",
        long_about = "Write a starter compose specification from a built-in template, or list and describe the available templates without writing a file. Writing a template requires an explicit shared cache directory.",
        after_help = NEW_HELP,
        name = "new",
        alias = "init"
    )]
    New {
        #[arg(
            long,
            value_name = "TEMPLATE",
            help = "Built-in template name to render"
        )]
        template: Option<String>,
        #[arg(
            long,
            help = "List the available built-in templates and exit",
            conflicts_with_all = ["describe_template", "template", "name", "cache_dir", "output", "force"]
        )]
        list_templates: bool,
        #[arg(
            long = "describe-template",
            value_name = "TEMPLATE",
            help = "Describe one built-in template and exit",
            conflicts_with_all = ["list_templates", "template", "name", "cache_dir", "output", "force"]
        )]
        describe_template: Option<String>,
        #[arg(
            long,
            value_name = "NAME",
            help = "Application name written into the generated spec"
        )]
        name: Option<String>,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Shared cache directory written into the generated spec; required when writing a template"
        )]
        cache_dir: Option<String>,
        #[arg(
            long,
            value_name = "FILE",
            default_value = "compose.yaml",
            help = "Path to the compose file to create"
        )]
        output: PathBuf,
        #[arg(long, help = "Overwrite the output file if it already exists")]
        force: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Inspect and prune cached image artifacts",
        long_about = "Inspect reusable imported and prepared image artifacts stored under the cache directory, or prune entries that are no longer needed.",
        after_help = CACHE_HELP
    )]
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
    #[command(
        about = "List tracked jobs under the current repo tree",
        long_about = "Scan the current repository tree for tracked hpc-compose submissions and list the recorded jobs without querying the scheduler.",
        after_help = JOBS_HELP
    )]
    Jobs {
        #[command(subcommand)]
        command: JobsCommands,
    },
    #[command(
        about = "Remove old tracked job directories",
        long_about = "Preview or remove tracked job metadata and runtime directories for the active compose context while keeping recent tracking data available.",
        after_help = CLEAN_HELP
    )]
    Clean {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DAYS",
            help = "Remove tracked job directories older than this many days",
            conflicts_with = "all"
        )]
        age: Option<u64>,
        #[arg(
            long,
            help = "Remove all tracked job directories except the latest one",
            conflicts_with = "age"
        )]
        all: bool,
        #[arg(
            long,
            help = "Preview the tracked job cleanup plan without deleting files"
        )]
        dry_run: bool,
        #[arg(
            long,
            help = "Include recursive disk-usage totals for tracked job paths"
        )]
        disk_usage: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Print resolved project-local settings context",
        long_about = "Print the effective project-local settings, selected profile, binaries, interpolation variables, and derived runtime paths for the active invocation context.",
        after_help = CONTEXT_HELP
    )]
    Context {
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Create or update the project-local settings file",
        long_about = "Create or update .hpc-compose/settings.toml with profile defaults, environment files, explicit environment variables, and binary overrides.",
        after_help = SETUP_HELP
    )]
    Setup {
        #[arg(long, value_name = "PROFILE", help = "Profile to create or update")]
        profile_name: Option<String>,
        #[arg(
            long,
            value_name = "FILE",
            help = "Compose file path recorded under the selected profile"
        )]
        compose_file: Option<String>,
        #[arg(
            long = "env-file",
            value_name = "PATH",
            help = "Environment file path to append under profile.env_files"
        )]
        env_files: Vec<String>,
        #[arg(
            long = "env",
            value_name = "KEY=VALUE",
            help = "Environment variable entry written into profile.env"
        )]
        env: Vec<String>,
        #[arg(
            long = "binary",
            value_name = "NAME=PATH",
            help = "Binary override such as srun=/opt/slurm/bin/srun"
        )]
        binaries: Vec<String>,
        #[arg(
            long,
            value_name = "PROFILE",
            help = "Set settings.default_profile to this profile name"
        )]
        default_profile: Option<String>,
        #[arg(
            long,
            help = "Do not prompt; use provided flags and existing settings as defaults"
        )]
        non_interactive: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Generate shell completions",
        long_about = "Generate shell completion scripts for the supported shells.",
        after_help = COMPLETIONS_HELP
    )]
    Completions {
        #[arg(
            value_enum,
            value_name = "SHELL",
            help = "Shell to generate completions for"
        )]
        shell: Shell,
    },
}

#[derive(Debug, Subcommand)]
pub enum CacheCommands {
    #[command(
        about = "List cached image artifacts",
        long_about = "List imported and prepared image artifacts already present in a cache directory.",
        after_help = CACHE_LIST_HELP
    )]
    List {
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to inspect instead of the default cache path"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Inspect cache reuse for the current plan",
        long_about = "Show which cached artifacts the current compose plan expects to reuse or rebuild.",
        after_help = CACHE_INSPECT_HELP
    )]
    Inspect {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(long, value_name = "SERVICE", help = "Limit the report to one service")]
        service: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Prune cached image artifacts",
        long_about = "Delete cached artifacts by age or delete artifacts that the current compose plan no longer references. In age mode, the active context resolves the cache directory unless --cache-dir is passed.",
        after_help = CACHE_PRUNE_HELP
    )]
    Prune {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = "Compose specification file used to resolve the active context for --age or to define live cache references for --all-unused"
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to prune directly; in --age mode this overrides active context resolution and skips compose loading"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DAYS",
            help = "Remove cached artifacts older than this many days; without --cache-dir, use the active context cache dir or the default cache path"
        )]
        age: Option<u64>,
        #[arg(
            long,
            help = "Remove cached artifacts that the current compose plan no longer references; requires -f/--file"
        )]
        all_unused: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
}

#[derive(Debug, Subcommand)]
pub enum JobsCommands {
    #[command(
        about = "List tracked jobs discovered under the repo tree",
        long_about = "Scan the nearest git repository root, or the current directory when no git root exists, for tracked hpc-compose submissions and list the recorded jobs.",
        after_help = JOBS_HELP
    )]
    List {
        #[arg(
            long,
            help = "Include recursive disk-usage totals for tracked job paths"
        )]
        disk_usage: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
}
