//! Shared CLI definitions for the runtime binary, completion generation, and
//! generated manpages.
#![allow(missing_docs)]

use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

const TOP_LEVEL_HELP: &str = "\
Normal run:
  hpc-compose submit --watch -f compose.yaml

Debugging flow:
  hpc-compose validate -f compose.yaml
  hpc-compose inspect --verbose -f compose.yaml
  hpc-compose preflight -f compose.yaml
  hpc-compose prepare -f compose.yaml";

const VALIDATE_HELP: &str = "\
Examples:
  hpc-compose validate -f compose.yaml
  hpc-compose validate -f compose.yaml --format json";

const RENDER_HELP: &str = "\
Examples:
  hpc-compose render -f compose.yaml
  hpc-compose render -f compose.yaml --output job.sbatch
  hpc-compose render -f compose.yaml --format json";

const PREPARE_HELP: &str = "\
Examples:
  hpc-compose prepare -f compose.yaml
  hpc-compose prepare -f compose.yaml --force
  hpc-compose prepare -f compose.yaml --format json";

const PREFLIGHT_HELP: &str = "\
Examples:
  hpc-compose preflight -f compose.yaml
  hpc-compose preflight -f compose.yaml --strict
  hpc-compose preflight -f compose.yaml --format json";

const INSPECT_HELP: &str = "\
Examples:
  hpc-compose inspect -f compose.yaml
  hpc-compose inspect --verbose -f compose.yaml
  hpc-compose inspect -f compose.yaml --format json";

const SUBMIT_HELP: &str = "\
Examples:
  hpc-compose submit --watch -f compose.yaml
  hpc-compose submit --dry-run -f compose.yaml
  hpc-compose submit --skip-prepare -f compose.yaml";

const STATUS_HELP: &str = "\
Examples:
  hpc-compose status -f compose.yaml
  hpc-compose status -f compose.yaml --format json";

const STATS_HELP: &str = "\
Examples:
  hpc-compose stats -f compose.yaml
  hpc-compose stats -f compose.yaml --format json
  hpc-compose stats -f compose.yaml --format csv";

const ARTIFACTS_HELP: &str = "\
Examples:
  hpc-compose artifacts -f compose.yaml
  hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
  hpc-compose artifacts -f compose.yaml --format json";

const LOGS_HELP: &str = "\
Examples:
  hpc-compose logs -f compose.yaml
  hpc-compose logs -f compose.yaml --service app --follow
  hpc-compose logs -f compose.yaml --job-id 12345 --lines 200";

const CANCEL_HELP: &str = "\
Examples:
  hpc-compose cancel -f compose.yaml
  hpc-compose cancel -f compose.yaml --job-id 12345";

const INIT_HELP: &str = "\
Examples:
  hpc-compose init --list-templates
  hpc-compose init --describe-template minimal-batch
  hpc-compose init --template minimal-batch --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml";

const CACHE_HELP: &str = "\
Examples:
  hpc-compose cache list
  hpc-compose cache inspect -f compose.yaml
  hpc-compose cache prune --age 7";

const CACHE_LIST_HELP: &str = "\
Examples:
  hpc-compose cache list
  hpc-compose cache list --cache-dir /shared/$USER/hpc-compose-cache
  hpc-compose cache list --format json";

const CACHE_INSPECT_HELP: &str = "\
Examples:
  hpc-compose cache inspect -f compose.yaml
  hpc-compose cache inspect -f compose.yaml --service app
  hpc-compose cache inspect -f compose.yaml --format json";

const CACHE_PRUNE_HELP: &str = "\
Examples:
  hpc-compose cache prune --age 14
  hpc-compose cache prune --all-unused -f compose.yaml
  hpc-compose cache prune --age 7 --format json";

const CLEAN_HELP: &str = "\
Examples:
  hpc-compose clean -f compose.yaml --age 7
  hpc-compose clean -f compose.yaml --all";

const COMPLETIONS_HELP: &str = "\
Examples:
  hpc-compose completions bash
  hpc-compose completions zsh > ~/.zfunc/_hpc-compose
  hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish";

pub const TOP_LEVEL_EXAMPLES: &[&str] = &[
    "hpc-compose submit --watch -f compose.yaml",
    "hpc-compose validate -f compose.yaml",
    "hpc-compose inspect --verbose -f compose.yaml",
    "hpc-compose preflight -f compose.yaml",
];

pub const VALIDATE_EXAMPLES: &[&str] = &[
    "hpc-compose validate -f compose.yaml",
    "hpc-compose validate -f compose.yaml --format json",
];

pub const RENDER_EXAMPLES: &[&str] = &[
    "hpc-compose render -f compose.yaml",
    "hpc-compose render -f compose.yaml --output job.sbatch",
    "hpc-compose render -f compose.yaml --format json",
];

pub const PREPARE_EXAMPLES: &[&str] = &[
    "hpc-compose prepare -f compose.yaml",
    "hpc-compose prepare -f compose.yaml --force",
    "hpc-compose prepare -f compose.yaml --format json",
];

pub const PREFLIGHT_EXAMPLES: &[&str] = &[
    "hpc-compose preflight -f compose.yaml",
    "hpc-compose preflight -f compose.yaml --strict",
    "hpc-compose preflight -f compose.yaml --format json",
];

pub const INSPECT_EXAMPLES: &[&str] = &[
    "hpc-compose inspect -f compose.yaml",
    "hpc-compose inspect --verbose -f compose.yaml",
    "hpc-compose inspect -f compose.yaml --format json",
];

pub const SUBMIT_EXAMPLES: &[&str] = &[
    "hpc-compose submit --watch -f compose.yaml",
    "hpc-compose submit --dry-run -f compose.yaml",
    "hpc-compose submit --skip-prepare -f compose.yaml",
];

pub const STATUS_EXAMPLES: &[&str] = &[
    "hpc-compose status -f compose.yaml",
    "hpc-compose status -f compose.yaml --format json",
];

pub const STATS_EXAMPLES: &[&str] = &[
    "hpc-compose stats -f compose.yaml",
    "hpc-compose stats -f compose.yaml --format json",
    "hpc-compose stats -f compose.yaml --format csv",
];

pub const ARTIFACTS_EXAMPLES: &[&str] = &[
    "hpc-compose artifacts -f compose.yaml",
    "hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball",
    "hpc-compose artifacts -f compose.yaml --format json",
];

pub const LOGS_EXAMPLES: &[&str] = &[
    "hpc-compose logs -f compose.yaml",
    "hpc-compose logs -f compose.yaml --service app --follow",
    "hpc-compose logs -f compose.yaml --job-id 12345 --lines 200",
];

pub const CANCEL_EXAMPLES: &[&str] = &[
    "hpc-compose cancel -f compose.yaml",
    "hpc-compose cancel -f compose.yaml --job-id 12345",
];

pub const INIT_EXAMPLES: &[&str] = &[
    "hpc-compose init --list-templates",
    "hpc-compose init --describe-template minimal-batch",
    "hpc-compose init --template minimal-batch --name my-app --cache-dir /shared/$USER/hpc-compose-cache --output compose.yaml",
];

pub const CACHE_EXAMPLES: &[&str] = &[
    "hpc-compose cache list",
    "hpc-compose cache inspect -f compose.yaml",
    "hpc-compose cache prune --age 7",
];

pub const CACHE_LIST_EXAMPLES: &[&str] = &[
    "hpc-compose cache list",
    "hpc-compose cache list --cache-dir /shared/$USER/hpc-compose-cache",
    "hpc-compose cache list --format json",
];

pub const CACHE_INSPECT_EXAMPLES: &[&str] = &[
    "hpc-compose cache inspect -f compose.yaml",
    "hpc-compose cache inspect -f compose.yaml --service app",
    "hpc-compose cache inspect -f compose.yaml --format json",
];

pub const CACHE_PRUNE_EXAMPLES: &[&str] = &[
    "hpc-compose cache prune --age 14",
    "hpc-compose cache prune --all-unused -f compose.yaml",
    "hpc-compose cache prune --age 7 --format json",
];

pub const CLEAN_EXAMPLES: &[&str] = &[
    "hpc-compose clean -f compose.yaml --age 7",
    "hpc-compose clean -f compose.yaml --all",
];

pub const COMPLETIONS_EXAMPLES: &[&str] = &[
    "hpc-compose completions bash",
    "hpc-compose completions zsh > ~/.zfunc/_hpc-compose",
    "hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish",
];

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Compile a compose-like spec into a single Slurm job using Enroot",
    long_about = "Compile a compose-like specification into one Slurm batch job that launches one or more services through Enroot and Pyxis inside a single allocation. Use submit --watch for the normal run, and use validate, inspect, preflight, and prepare when adapting or debugging a spec.",
    after_help = TOP_LEVEL_HELP
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum StatsOutputFormat {
    Text,
    Json,
    Csv,
    Jsonl,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
        about = "Submit a job and optionally watch it",
        long_about = "Run the end-to-end submission flow: optional preflight, image preparation, script rendering, sbatch submission, and optional live watching of tracked state and logs.",
        after_help = SUBMIT_HELP
    )]
    Submit {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered batch script to this path before submission"
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
            help = "Poll scheduler state and stream tracked logs after submission"
        )]
        watch: bool,
        #[arg(
            long,
            help = "Run preflight, prepare, and render without calling sbatch"
        )]
        dry_run: bool,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
        about = "Cancel a tracked Slurm job",
        long_about = "Cancel a tracked Slurm job by explicit job id or by the latest submission recorded for the compose file.",
        after_help = CANCEL_HELP
    )]
    Cancel {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
    },
    #[command(
        about = "Write a starter compose file from a built-in template",
        long_about = "Write a starter compose specification from a built-in template, or list and describe the available templates without writing a file.",
        after_help = INIT_HELP
    )]
    Init {
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
            help = "Shared cache directory written into the generated spec"
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
        about = "Remove old tracked job directories",
        long_about = "Remove old tracked job directories under .hpc-compose for the compose file while keeping recent tracking data available.",
        after_help = CLEAN_HELP
    )]
    Clean {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
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
            default_value = "compose.yaml",
            help = "Compose specification file to read"
        )]
        file: PathBuf,
        #[arg(long, value_name = "SERVICE", help = "Limit the report to one service")]
        service: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Prune cached image artifacts",
        long_about = "Delete cached artifacts by age or delete artifacts that the current compose plan no longer references.",
        after_help = CACHE_PRUNE_HELP
    )]
    Prune {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = "Compose specification file whose plan defines the live cache references"
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to prune instead of the default or plan cache path"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DAYS",
            help = "Remove cached artifacts older than this many days"
        )]
        age: Option<u64>,
        #[arg(
            long,
            help = "Remove cached artifacts that the current compose plan no longer references"
        )]
        all_unused: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
}

pub fn parse_cli() -> Cli {
    Cli::parse()
}

pub fn build_cli_command() -> clap::Command {
    Cli::command()
}

pub fn examples_for_path(path: &[&str]) -> &'static [&'static str] {
    match path {
        [] => TOP_LEVEL_EXAMPLES,
        ["validate"] => VALIDATE_EXAMPLES,
        ["render"] => RENDER_EXAMPLES,
        ["prepare"] => PREPARE_EXAMPLES,
        ["preflight"] => PREFLIGHT_EXAMPLES,
        ["inspect"] => INSPECT_EXAMPLES,
        ["submit"] => SUBMIT_EXAMPLES,
        ["status"] => STATUS_EXAMPLES,
        ["stats"] => STATS_EXAMPLES,
        ["artifacts"] => ARTIFACTS_EXAMPLES,
        ["logs"] => LOGS_EXAMPLES,
        ["cancel"] => CANCEL_EXAMPLES,
        ["init"] => INIT_EXAMPLES,
        ["cache"] => CACHE_EXAMPLES,
        ["cache", "list"] => CACHE_LIST_EXAMPLES,
        ["cache", "inspect"] => CACHE_INSPECT_EXAMPLES,
        ["cache", "prune"] => CACHE_PRUNE_EXAMPLES,
        ["clean"] => CLEAN_EXAMPLES,
        ["completions"] => COMPLETIONS_EXAMPLES,
        _ => &[],
    }
}
