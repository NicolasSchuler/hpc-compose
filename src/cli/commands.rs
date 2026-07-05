use std::path::PathBuf;

use clap::{Parser, Subcommand};
use clap_complete::Shell;

use super::help::*;
use super::{
    ColorPolicy, CsvOutputFormat, DependencyOutputFormat, ExamplesOutputFormat, HoldOnExit,
    OutputFormat, RemoteInstallMode, SchemaKind, StatsOutputFormat, WatchMode,
};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Compile a compose-like spec into a single Slurm job",
    long_about = "Compile a compose-like specification into one Slurm batch job that launches one or more services through Pyxis/Enroot, Apptainer, Singularity, or host runtime software inside a single allocation. Use plan for static authoring, up for the normal run, and debug for one-command triage.",
    help_template = "{about-with-newline}\n{usage-heading} {usage}\n\n{options}\n\n{after-help}",
    after_help = top_level_help()
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "WHEN",
        help = "Controls colored output",
        default_value = "auto"
    )]
    pub color: ColorPolicy,
    #[arg(
        long,
        global = true,
        help = "Suppress progress indicators and non-essential labels"
    )]
    pub quiet: bool,
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

/// Common runtime launch flags shared across `up`, `germinate`, `test`, `dev`,
/// `tmux`, `when`, `alloc`, and `run`.
#[derive(Debug, clap::Args)]
pub struct RuntimeLaunchArgs {
    #[arg(
        short = 'f',
        long,
        value_name = "FILE",
        help = FILE_ARG_HELP
    )]
    pub file: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "enroot",
        help_heading = "Tool overrides",
        help = "Path to the enroot executable"
    )]
    pub enroot_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "apptainer",
        help_heading = "Tool overrides",
        help = "Path to the apptainer executable"
    )]
    pub apptainer_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "singularity",
        help_heading = "Tool overrides",
        help = "Path to the singularity executable"
    )]
    pub singularity_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "huggingface-cli",
        help_heading = "Tool overrides",
        help = "Path to the huggingface-cli used by hf:// stage_in inside the job (default: huggingface-cli)"
    )]
    pub huggingface_cli_bin: String,
    #[arg(
        long,
        help = "Keep failed preparation state on disk for later inspection"
    )]
    pub keep_failed_prep: bool,
    #[arg(long, help = "Skip image import and prepare reuse checks")]
    pub skip_prepare: bool,
    #[arg(long, help = "Refresh imported and prepared artifacts before running")]
    pub force_rebuild: bool,
    #[arg(long, help = "Skip the preflight phase before running")]
    pub no_preflight: bool,
}

/// Slurm submission tool overrides shared across `up`, `germinate`, `test`,
/// `when`, and `run`.
#[derive(Debug, clap::Args)]
pub struct SlurmSubmitArgs {
    #[arg(
        long,
        value_name = "PATH",
        default_value = "sbatch",
        help_heading = "Tool overrides",
        help = "Path to the sbatch executable"
    )]
    pub sbatch_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "srun",
        help_heading = "Tool overrides",
        help = "Path to the srun executable"
    )]
    pub srun_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "squeue",
        help_heading = "Tool overrides",
        help = "Path to the squeue executable"
    )]
    pub squeue_bin: String,
    #[arg(
        long,
        value_name = "PATH",
        default_value = "sacct",
        help_heading = "Tool overrides",
        help = "Path to the sacct executable"
    )]
    pub sacct_bin: String,
}

/// The `--remote[=HOST]` follow-up delegation flag shared by the read-only
/// inspection commands (`status`, `stats`, `score`, `pull`, `logs`, `ps`). Runs
/// the command on the login node's staged checkout from a prior `up --remote`.
/// (`up`'s own `--remote` has different help and delegation semantics, so it
/// keeps its own field rather than flattening this.)
#[derive(Debug, clap::Args)]
pub struct RemoteArgs {
    #[arg(
        long,
        value_name = "HOST",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "",
        help = "Run this command on the login node's staged checkout from a prior `up --remote`, over SSH, streaming output back. With no value, uses the configured login_host; accepts user@host (otherwise HPC_COMPOSE_REMOTE_USER / login_user / ~/.ssh/config)"
    )]
    pub remote: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        display_order = 400,
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
        display_order = 405,
        about = "Run opinionated static lint checks on a compose spec",
        long_about = "Run stricter static checks on a validated compose specification. Lint catches suspicious authoring choices that are structurally valid but often risky on shared HPC systems.",
        after_help = LINT_HELP
    )]
    Lint {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            help = "Fail when ${VAR:-default} or ${VAR-default} fallbacks are used because VAR is missing"
        )]
        strict_env: bool,
        #[arg(
            long,
            help = "Exit successfully when only warning-level findings are present"
        )]
        allow_warnings: bool,
        #[arg(
            long,
            help = "Apply every auto-fixable finding to the compose file in place"
        )]
        fix: bool,
        #[arg(
            long,
            requires = "fix",
            help = "With --fix, print a unified diff of the proposed changes without writing"
        )]
        dry_run: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 430,
        about = "Render the generated sbatch script",
        long_about = "Render the sbatch script produced from the normalized plan. Use this to inspect generated SBATCH directives, srun invocations, mounts, and environment forwarding without submitting the job. With --annotate, provenance comments (`# <- x-slurm.mem` markers and `# --- section ---` banners) map script lines back to the spec fields that produced them; annotations are preview-only and never appear in submitted scripts.",
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
        #[arg(
            long,
            help = "Interleave provenance comments mapping script lines back to spec fields (preview-only)"
        )]
        annotate: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 435,
        about = "Explain which spec fields produced which script lines",
        long_about = "Map the rendered preview script back to the compose spec fields that produced it, in both directions: `--field` lists the script lines a spec field generated, `--line` names the field behind one script line, and the bare command prints the full provenance map. Static-safe: renders the same preview script as `render` and `plan --show-script` without contacting Slurm, so line numbers match those previews exactly (submitted scripts can differ once submission paths bake absolute runtime paths). Coverage is best-effort: SBATCH directives, feature-block sections, readiness gates, and dependency waits are mapped. Echoed script lines are secret-redacted like other diagnostics; the full map lists line ranges without echoing contents.",
        after_help = EXPLAIN_HELP
    )]
    Explain {
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "SPEC_PATH",
            help = "Show the script lines produced by this spec field (prefix match, e.g. x-slurm.mem or services.app.readiness)"
        )]
        field: Option<String>,
        #[arg(
            long,
            value_name = "N",
            conflicts_with = "field",
            help = "Show the spec field(s) that produced script line N (1-based, matching render / plan --show-script line numbers)"
        )]
        line: Option<usize>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 440,
        about = "Prepare imported and customized runtime images",
        long_about = "Import base images and build prepared runtime artifacts on the submission host with the selected runtime backend. This is the login-node image preparation phase reused later by up and run.",
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
            help_heading = "Tool overrides",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "apptainer",
            help_heading = "Tool overrides",
            help = "Path to the apptainer executable"
        )]
        apptainer_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "singularity",
            help_heading = "Tool overrides",
            help = "Path to the singularity executable"
        )]
        singularity_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "huggingface-cli",
            help_heading = "Tool overrides",
            help = "Path to the huggingface-cli used by hf:// stage_in inside the job (default: huggingface-cli)"
        )]
        huggingface_cli_bin: String,
        #[arg(
            long,
            help = "Keep failed preparation state on disk for later inspection"
        )]
        keep_failed_prep: bool,
        #[arg(
            long = "force-rebuild",
            help = "Refresh imported and prepared artifacts before running"
        )]
        force_rebuild: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 450,
        about = "Check cluster prerequisites on the submission host",
        long_about = "Check whether the submission host and compose specification satisfy the prerequisites for a later run. This validates required binaries, cache path safety, local mounts, selected runtime backend availability, Slurm availability, and any discovered cluster profile.",
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
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help_heading = "Tool overrides",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scontrol",
            help_heading = "Tool overrides",
            help = "Path to the scontrol executable"
        )]
        scontrol_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "apptainer",
            help_heading = "Tool overrides",
            help = "Path to the apptainer executable"
        )]
        apptainer_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "singularity",
            help_heading = "Tool overrides",
            help = "Path to the singularity executable"
        )]
        singularity_bin: String,
    },
    #[command(
        display_order = 410,
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
            conflicts_with_all = ["rightsize", "dependencies"],
            help = "Include resolved environment values and final mount mappings"
        )]
        verbose: bool,
        #[arg(
            long,
            conflicts_with_all = ["rightsize", "dependencies"],
            help = "Show services as a dependency tree"
        )]
        tree: bool,
        #[arg(
            long,
            conflicts_with = "dependencies",
            help = "Compare requested resources against tracked post-run usage and suggest conservative replacements"
        )]
        rightsize: bool,
        #[arg(
            long,
            conflicts_with_all = ["verbose", "tree", "rightsize"],
            help = "Show the normalized service dependency graph"
        )]
        dependencies: bool,
        #[arg(
            long = "dependencies-format",
            requires = "dependencies",
            value_enum,
            value_name = "FORMAT",
            default_value = "text",
            help = "Dependency graph output format"
        )]
        dependencies_format: DependencyOutputFormat,
        #[arg(
            long,
            requires = "rightsize",
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to right-size instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable for --rightsize"
        )]
        sstat_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable for --rightsize"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable for --rightsize"
        )]
        sacct_bin: String,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 420,
        about = "Render the fully interpolated effective config",
        long_about = "Print the normalized effective compose config after interpolation, healthcheck normalization, and default application. Use this to inspect what plan, up, and inspect receive.",
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
        #[arg(long, help = "Show resolved interpolation variables and their sources")]
        variables: bool,
        #[arg(
            long,
            help = "Show sensitive values (secret-sourced or sensitive-named) instead of redacting them, in both the effective config and --variables output"
        )]
        show_values: bool,
    },
    #[command(
        display_order = 470,
        about = "Print the hpc-compose JSON Schema",
        long_about = "Print the checked-in JSON Schema for compose authoring tools (default) or settings.toml authoring tools (--kind settings). Rust validation remains the semantic source of truth.",
        after_help = SCHEMA_HELP
    )]
    Schema {
        #[arg(
            long,
            value_name = "KIND",
            help = "Which schema to print: 'compose' (default) or 'settings'"
        )]
        kind: Option<SchemaKind>,
        #[arg(
            long,
            value_name = "COMMAND",
            help = "Print the JSON Schema for a command's --format json output (e.g. score, jobs-list)"
        )]
        output: Option<String>,
    },
    #[command(
        display_order = 100,
        about = "Validate and preview a static execution plan",
        long_about = "Run the safe static authoring path: validate the compose file, build the normalized runtime plan, and optionally print the generated launcher script without touching Slurm, preparing images, or writing script files.",
        after_help = PLAN_HELP
    )]
    Plan {
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
        #[arg(
            long,
            help = "Include resolved environment values and final mount mappings"
        )]
        verbose: bool,
        #[arg(long, help = "Show services as a dependency tree")]
        tree: bool,
        #[arg(
            long,
            help = "Print the rendered launcher script to stdout after the plan"
        )]
        show_script: bool,
        #[arg(
            long,
            requires = "show_script",
            help = "With --show-script, interleave provenance comments mapping script lines back to spec fields"
        )]
        annotate: bool,
        #[arg(long, help = "Show cache, runtime, and next-step planning hints")]
        explain: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 460,
        about = "Check cluster readiness and tool availability",
        long_about = "Run environment diagnostics without requiring a compose file. Checks Slurm, runtime backend tools, GPU, and cache directory availability. Use the cluster-report, mpi-smoke, and fabric-smoke subcommands for targeted probes."
    )]
    Doctor {
        #[command(subcommand)]
        command: Option<DoctorCommands>,
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP,
            hide = true
        )]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            help = "Generate a best-effort cluster capability profile",
            hide = true
        )]
        cluster_report: bool,
        #[arg(
            long = "cluster-report-out",
            value_name = "PATH",
            help = "Write the cluster profile to this path; use '-' to print TOML",
            hide = true
        )]
        cluster_report_out: Option<PathBuf>,
        #[arg(
            long,
            help = "Render or run an MPI smoke probe for a compose service with x-slurm.mpi",
            hide = true
        )]
        mpi_smoke: bool,
        #[arg(
            long,
            help = "Render or run MPI and fabric smoke probes for a compose service with x-slurm.mpi",
            hide = true
        )]
        fabric_smoke: bool,
        #[arg(
            long,
            value_name = "CHECKS",
            help = "Fabric smoke checks: auto, mpi, nccl, ucx, ofi, or a comma-separated list",
            hide = true
        )]
        checks: Option<String>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "MPI service to smoke-test; inferred when exactly one MPI service exists",
            hide = true
        )]
        service: Option<String>,
        #[arg(
            long,
            help = "Submit the MPI smoke probe to Slurm; without this, only render/explain it",
            hide = true
        )]
        submit: bool,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered MPI smoke batch script to this path",
            hide = true
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "5m",
            help = "Timeout for a submitted MPI smoke job",
            hide = true
        )]
        timeout: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scontrol",
            help_heading = "Tool overrides",
            help = "Path to the scontrol executable"
        )]
        scontrol_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help_heading = "Tool overrides",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "apptainer",
            help_heading = "Tool overrides",
            help = "Path to the apptainer executable"
        )]
        apptainer_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "singularity",
            help_heading = "Tool overrides",
            help = "Path to the singularity executable"
        )]
        singularity_bin: String,
    },
    #[command(
        display_order = 465,
        about = "Show advisory live cluster conditions",
        long_about = "Show a compact one-shot dashboard of live Slurm node, queue, fairshare, and priority signals. This is advisory cluster weather, not a reservation, scheduler mutation, or full Slurm inspection frontend.",
        after_help = WEATHER_HELP
    )]
    Weather {
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sinfo",
            help_heading = "Tool overrides",
            help = "Path to the sinfo executable"
        )]
        sinfo_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sshare",
            help_heading = "Tool overrides",
            help = "Path to the sshare executable"
        )]
        sshare_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sprio",
            help_heading = "Tool overrides",
            help = "Path to the sprio executable"
        )]
        sprio_bin: String,
    },
    #[command(
        display_order = 110,
        about = "Submit, watch, and stream logs in one command",
        long_about = "Run the normal end-to-end workflow: optional preflight, image preparation, script rendering, sbatch submission or local launch, and immediate live watching with log streaming and exit-code propagation.",
        after_help = UP_HELP
    )]
    Up {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long,
            help = "Stream the raw image-prepare tool output (enroot/apptainer) live instead of summarized phase lines. With --remote this enables verbose prepare on the login node, where a local HPC_COMPOSE_PREPARE_VERBOSE would not reach"
        )]
        prepare_verbose: bool,
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
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
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
            help = "Run preflight, prepare, and render without calling sbatch (prepare still imports images; add --skip-prepare for a no-side-effect render, or use `plan`)"
        )]
        dry_run: bool,
        #[arg(long, help = "Submit or launch and return without watching logs")]
        detach: bool,
        #[arg(
            long,
            help = "After Slurm submission, poll queue state until the job reaches RUNNING before opening the watch view"
        )]
        watch_queue: bool,
        #[arg(
            long,
            value_name = "DURATION",
            help = "Warn when --watch-queue stays PENDING longer than this duration; default is 10m, and 0 disables the warning"
        )]
        queue_warn_after: Option<String>,
        #[arg(
            long,
            value_enum,
            value_name = "MODE",
            default_value = "auto",
            help = "Watch output mode"
        )]
        watch_mode: WatchMode,
        #[arg(
            long,
            value_enum,
            value_name = "WHEN",
            default_value = "failure",
            help = "Keep the watch UI open after terminal states"
        )]
        hold_on_exit: HoldOnExit,
        #[arg(
            long,
            value_enum,
            value_name = "FORMAT",
            help = "Output format for --detach or --dry-run"
        )]
        format: Option<OutputFormat>,
        #[arg(
            long,
            help = "Include readiness-derived service endpoints (host/port/url) and suggested next commands in the JSON output (with --detach or --dry-run)"
        )]
        print_endpoints: bool,
        #[arg(
            long,
            value_name = "SECONDS",
            conflicts_with = "no_metrics",
            help = "Enable runtime metrics sampling and override x-slurm.metrics.interval_seconds for this run"
        )]
        metrics_interval: Option<u64>,
        #[arg(long, help = "Disable runtime metrics sampling for this run")]
        no_metrics: bool,
        #[arg(
            long,
            value_name = "HOST",
            num_args = 0..=1,
            require_equals = true,
            default_missing_value = "",
            help = "Delegate this submission to a login node over SSH: rsync the project there and run `hpc-compose up` remotely, streaming output back. With no value, uses the configured login_host. Accepts user@host; otherwise the user comes from HPC_COMPOSE_REMOTE_USER / login_user / your ~/.ssh/config. Port and identity come from ~/.ssh/config (or set HPC_COMPOSE_REMOTE_SSH_OPTS for ad-hoc ssh flags)"
        )]
        remote: Option<String>,
        #[arg(
            long,
            value_enum,
            value_name = "MODE",
            default_value = "auto",
            help = "With --remote, bootstrap/upgrade hpc-compose on the login node before delegating: auto installs the newest release when it is missing or older than the local version, force always reinstalls, never only probes and errors (override with HPC_COMPOSE_REMOTE_INSTALL)"
        )]
        remote_install: RemoteInstallMode,
    },
    #[command(
        display_order = 112,
        about = "Submit and inspect hyperparameter sweeps",
        long_about = "Expand an embedded top-level sweep block into independent tracked Slurm submissions, then aggregate their tracked scheduler and runtime state.",
        after_help = SWEEP_HELP
    )]
    Sweep {
        #[command(subcommand)]
        command: SweepCommands,
    },
    #[command(
        display_order = 111,
        about = "Submit a short canary run and recommend resource settings",
        long_about = "Submit a minimized Slurm canary allocation, force metrics sampling on, wait for it to finish, then compare observed usage against the original compose request and print right-sizing recommendations."
    )]
    Germinate {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered canary script to this path before submission"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "TIME",
            default_value = "00:01:00",
            help = "Walltime for the canary allocation"
        )]
        canary_time: String,
        #[arg(
            long,
            value_name = "SECONDS",
            default_value_t = 5,
            help = "Metrics sampler interval forced into the canary plan"
        )]
        metrics_interval: u64,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "30m",
            help = "Give up if the canary remains non-terminal for this long"
        )]
        timeout: String,
        #[arg(
            long,
            value_name = "CPUS",
            default_value_t = 1,
            help = "Minimum canary CPUs per task"
        )]
        min_cpus: u32,
        #[arg(
            long,
            value_name = "MEM",
            default_value = "1G",
            help = "Minimum canary memory"
        )]
        min_mem: String,
        #[arg(
            long,
            value_name = "GPUS",
            default_value_t = 1,
            help = "Minimum canary GPU count when the original plan requests GPUs"
        )]
        min_gpus: u32,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
        #[arg(long, help = "Render the canary script without submitting it")]
        dry_run: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        name = "test",
        display_order = 113,
        about = "Smoke-test a compose spec end to end",
        long_about = "Validate, prepare, render, launch, and evaluate a finite compose smoke test. Choose --local for the local Pyxis/Enroot supervisor or --submit for a short Slurm submission.",
        after_help = TEST_HELP
    )]
    Test {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long,
            help = "Run the smoke test through the local Pyxis/Enroot supervisor"
        )]
        local: bool,
        #[arg(long, help = "Submit the smoke test to Slurm")]
        submit: bool,
        #[arg(
            long,
            value_name = "TIME",
            default_value = "00:01:00",
            help = "Walltime override for --submit smoke tests"
        )]
        time: String,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "180s",
            help = "Maximum time to wait for the smoke test to reach a terminal result"
        )]
        timeout: String,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scancel",
            help_heading = "Tool overrides",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 114,
        about = "Run a local hot-reload development loop",
        long_about = "Launch the compose spec with the local Pyxis/Enroot supervisor, watch bind-mounted source directories, and request targeted service restarts when files change.",
        after_help = DEV_HELP
    )]
    Dev {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long = "watch-paths",
            alias = "watch-path",
            value_name = "PATH",
            help = "Additional source directory to watch; restarts all services when it changes"
        )]
        watch_paths: Vec<PathBuf>,
        #[arg(
            long,
            value_name = "MILLISECONDS",
            default_value_t = 300,
            help = "Debounce file changes before requesting a restart"
        )]
        debounce_ms: u64,
        #[arg(long, help = "Leave the local supervisor running when dev exits")]
        keep_running: bool,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered local launcher script to this path"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            help = "Open the live watch TUI while file-watching restarts services in the background"
        )]
        tui: bool,
    },
    #[command(
        display_order = 116,
        about = "Open a tmux pane dashboard for local service logs",
        long_about = "Launch or attach to a tracked local run and create one tmux pane per service, each tailing that service's log. tmux is a log dashboard; the existing local supervisor still owns service processes.",
        after_help = TMUX_HELP
    )]
    Tmux {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked local job id to attach to instead of launching a new local run"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "NAME",
            help = "tmux session name; defaults to hpc-compose-<job-id>"
        )]
        session: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "tmux",
            help_heading = "Tool overrides",
            help = "Path to the tmux executable"
        )]
        tmux_bin: String,
        #[arg(long, help = "Create or update the tmux session without attaching")]
        no_attach: bool,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of existing log lines each pane should show before following"
        )]
        lines: usize,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered local launcher script to this path when launching"
        )]
        script_out: Option<PathBuf>,
    },
    #[command(
        display_order = 117,
        about = "Submit once cluster conditions are met",
        long_about = "Prepare and render the compose job now, then monitor Slurm or local wall-clock conditions in the foreground and submit automatically when every condition is satisfied.",
        after_help = WHEN_HELP
    )]
    When {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            long,
            value_name = "PARTITION",
            requires = "free_nodes",
            help = "Partition to monitor for --free-nodes; must match x-slurm.partition"
        )]
        partition: Option<String>,
        #[arg(
            long,
            value_name = "NODES",
            requires = "partition",
            help = "Submit when the monitored partition has at least this many idle nodes"
        )]
        free_nodes: Option<u32>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Submit after this Slurm job reaches a terminal state matching --after-job-condition"
        )]
        after_job: Option<String>,
        #[arg(
            long,
            value_name = "CONDITION",
            default_value = "afterany",
            help = "Dependency condition for --after-job: afterany, afterok, or afternotok"
        )]
        after_job_condition: String,
        #[arg(
            long,
            value_name = "HH:MM-HH:MM",
            help = "Submit only inside this local wall-clock window, e.g. 22:00-06:00"
        )]
        between: Option<String>,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "60s",
            help = "Polling interval for active monitoring; minimum 5s"
        )]
        poll_interval: String,
        #[arg(
            long,
            value_name = "DURATION",
            help = "Give up if conditions are not met within this duration; 0s performs one check"
        )]
        timeout: Option<String>,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path before monitoring"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sinfo",
            help_heading = "Tool overrides",
            help = "Path to the sinfo executable"
        )]
        sinfo_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            help = "Allow submission even when resume config drift is detected"
        )]
        allow_resume_changes: bool,
        #[arg(long, help = "Submit and return after tracking metadata is written")]
        detach: bool,
        #[arg(
            long,
            value_enum,
            value_name = "MODE",
            default_value = "auto",
            help = "Watch output mode after submission"
        )]
        watch_mode: WatchMode,
        #[arg(
            long,
            value_enum,
            value_name = "WHEN",
            default_value = "failure",
            help = "Keep the watch UI open after terminal states"
        )]
        hold_on_exit: HoldOnExit,
        #[arg(
            long,
            value_enum,
            value_name = "FORMAT",
            help = "Output format for --detach"
        )]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 115,
        about = "Open an interactive Slurm allocation for iterative service runs",
        long_about = "Request one Slurm allocation using the compose file's top-level x-slurm settings, prepare images, export HPC_COMPOSE_* allocation metadata, and open a login shell or run the command after --.",
        after_help = ALLOC_HELP
    )]
    Alloc {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            value_name = "COMMAND",
            num_args = 0..,
            trailing_var_arg = true,
            allow_hyphen_values = true,
            help = "Optional command to run inside the allocation after --; defaults to $SHELL -l"
        )]
        command: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "salloc",
            help_heading = "Tool overrides",
            help = "Path to the salloc executable"
        )]
        salloc_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scontrol",
            help_heading = "Tool overrides",
            help = "Path to the scontrol executable"
        )]
        scontrol_bin: String,
    },
    #[command(
        display_order = 220,
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
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            help = "Include Slurm array task rows from squeue --array and sacct --array"
        )]
        array: bool,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 250,
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
        #[arg(
            long,
            value_name = "ID",
            conflicts_with = "job_id",
            help = "Aggregate per-trial stats over all trials of this sweep id instead of a single job"
        )]
        sweep: Option<String>,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<StatsOutputFormat>,
        #[arg(
            long,
            help = "Include on-demand sacct accounting rollups for completed or visible Slurm jobs"
        )]
        accounting: bool,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(name = "metrics-probe", hide = true)]
    MetricsProbe {
        #[arg(
            long,
            value_name = "SECONDS",
            default_value_t = 5,
            help = "Duration of the internal CPU workload used for perf counter probing"
        )]
        duration_seconds: u64,
        #[arg(
            long,
            value_enum,
            value_name = "FORMAT",
            default_value = "json",
            help = "Output format"
        )]
        format: OutputFormat,
        #[arg(
            long,
            help = "Also time one nvidia-smi query for rough overhead comparison"
        )]
        compare_nvidia_smi: bool,
    },
    #[command(
        display_order = 252,
        about = "Score tracked job resource efficiency",
        long_about = "Compute a post-run 0-100 efficiency score for a tracked Slurm job from GPU sampler history, memory usage, active compute time, accounting, and best-effort energy estimates.",
        after_help = SCORE_HELP
    )]
    Score {
        #[arg(value_name = "JOB_ID", help = "Tracked Slurm job id to score")]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "ID",
            conflicts_with = "job_id",
            help = "Score every trial of this sweep id instead of a single job"
        )]
        sweep: Option<String>,
        #[arg(
            short = 'f',
            long,
            value_name = "FILE",
            help = FILE_ARG_HELP
        )]
        file: Option<PathBuf>,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "FLOAT",
            default_value_t = 1.20,
            help = "Power usage effectiveness multiplier for kWh estimates"
        )]
        pue: f64,
        #[arg(
            long,
            value_name = "WATTS",
            default_value_t = 300.0,
            help = "Fallback GPU TDP in watts when sampler power is unavailable"
        )]
        gpu_tdp_w: f64,
        #[arg(
            long,
            value_name = "WATTS",
            default_value_t = 8.0,
            help = "Fallback CPU watts per allocated core for energy estimates"
        )]
        cpu_watts_per_core: f64,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 255,
        about = "Compare two tracked job submissions",
        long_about = "Compare tracked submission metadata, effective config snapshots, selected resource settings, and observed outcomes between two jobs. With --across <SWEEP_ID> or --jobs <a,b,c> the comparison becomes an N-way matrix (one column per run, one row per field that differs in at least one run).",
        after_help = DIFF_HELP
    )]
    Diff {
        #[arg(value_name = "JOB_ID_1", help = "Earlier or left-hand tracked job id")]
        job_id_1: Option<String>,
        #[arg(value_name = "JOB_ID_2", help = "Later or right-hand tracked job id")]
        job_id_2: Option<String>,
        #[arg(
            long,
            value_name = "SWEEP_ID",
            conflicts_with_all = ["job_id_1", "job_id_2", "jobs"],
            help = "Compare every submitted trial of this sweep id as an N-way matrix"
        )]
        across: Option<String>,
        #[arg(
            long,
            value_name = "JOB_IDS",
            value_delimiter = ',',
            conflicts_with_all = ["job_id_1", "job_id_2", "across"],
            help = "Compare these tracked job ids as an N-way matrix (comma-separated)"
        )]
        jobs: Vec<String>,
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_enum,
            value_name = "FORMAT",
            help = "Pairwise output format (text, json)"
        )]
        format: Option<OutputFormat>,
        #[arg(
            long = "matrix-format",
            value_enum,
            value_name = "FORMAT",
            help = "N-way matrix output format (text, csv, json)"
        )]
        matrix_format: Option<CsvOutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 260,
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
        display_order = 261,
        about = "Resolve a tracked artifact bundle and print an rsync line",
        long_about = "Resolve a tracked job's artifact payload directory and print the rsync command to copy it to a laptop, with SSH connection multiplexing so an OTP login node prompts only once. Read-only: it never copies anything, opens a connection, or contacts the scheduler.",
        after_help = PULL_HELP
    )]
    Pull {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to pull instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "DIR",
            help = "Local destination directory shown in the rsync command (default: .)"
        )]
        into: Option<PathBuf>,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 230,
        about = "Print tracked service logs",
        long_about = "Print tracked service logs from a previous run. Follow mode tails appended log data as it appears.",
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
            long = "grep",
            value_name = "PATTERN",
            help = "Only print log lines matching this Rust regex pattern"
        )]
        grep: Option<String>,
        #[arg(
            long,
            value_name = "DURATION",
            help = "Only print initial log tails for files updated within this coarse duration, e.g. 30s, 15m, 2h, or 1d"
        )]
        since: Option<String>,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of trailing log lines to show before follow mode begins"
        )]
        lines: usize,
        #[command(flatten)]
        remote: RemoteArgs,
    },
    #[command(
        display_order = 240,
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
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 210,
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
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_enum,
            value_name = "MODE",
            default_value = "auto",
            help = "Watch output mode"
        )]
        watch_mode: WatchMode,
        #[arg(
            long,
            value_enum,
            value_name = "WHEN",
            default_value = "failure",
            help = "Keep the watch UI open after terminal states"
        )]
        hold_on_exit: HoldOnExit,
    },
    #[command(
        display_order = 215,
        about = "Replay a tracked job timeline from runtime artifacts",
        long_about = "Reconstruct a best-effort timeline from tracked state, service-exit markers, metrics JSONL, and logs, then replay it in the watch-style terminal UI or print a static summary.",
        after_help = REPLAY_HELP
    )]
    Replay {
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
            help = "Tracked Slurm job id to replay instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "Service to focus initially and include in the replay"
        )]
        service: Option<String>,
        #[arg(
            long,
            value_name = "MULTIPLIER",
            default_value_t = 1.0,
            allow_hyphen_values = true,
            help = "Replay speed multiplier, e.g. 1, 10, or 100"
        )]
        speed: f64,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of trailing log lines to seed into the replay view"
        )]
        lines: usize,
        #[arg(
            long,
            value_enum,
            value_name = "MODE",
            default_value = "auto",
            help = "Watch output mode"
        )]
        watch_mode: WatchMode,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 245,
        about = "Show attempt and requeue history from tracked state",
        long_about = "Reconstruct the attempt and requeue history of a tracked job from LOCAL tracked state only: the per-attempt state.json files written under .hpc-compose/<job>/attempts/<n>/ when x-slurm.resume is configured, or the single latest state.json otherwise. Contacts no scheduler and reads nothing from the cluster filesystem; missing or unreadable state degrades gracefully into notes instead of failing.",
        after_help = CHECKPOINTS_HELP
    )]
    Checkpoints {
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
    },
    #[command(
        display_order = 200,
        about = "Diagnose the latest tracked run",
        long_about = "Collect tracked scheduler state, service state, batch and service log tails, and a recommended next command. Add --preflight to rerun cluster prerequisite checks.",
        after_help = DEBUG_HELP
    )]
    Debug {
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
            help = "Tracked Slurm job id to diagnose instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "Service whose log tail should be emphasized"
        )]
        service: Option<String>,
        #[arg(
            long,
            value_name = "LINES",
            default_value_t = 100,
            help = "Number of trailing log lines to include"
        )]
        lines: usize,
        #[arg(long, help = "Rerun preflight and include its findings")]
        preflight: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help_heading = "Tool overrides",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scontrol",
            help_heading = "Tool overrides",
            help = "Path to the scontrol executable"
        )]
        scontrol_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "apptainer",
            help_heading = "Tool overrides",
            help = "Path to the apptainer executable"
        )]
        apptainer_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "singularity",
            help_heading = "Tool overrides",
            help = "Path to the singularity executable"
        )]
        singularity_bin: String,
    },
    #[command(
        display_order = 340,
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
            help_heading = "Tool overrides",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
        #[arg(long, help = "Also purge tracked cached image artifacts for this job")]
        purge_cache: bool,
        #[arg(
            long,
            help = "Skip auto-exporting tracked artifacts to x-slurm.artifacts.export_dir before reaping runtime state"
        )]
        no_export: bool,
        #[arg(
            long,
            help = "Confirm this destructive action without prompting. Passing an explicit --job-id already skips the prompt (explicit intent), unless --purge-cache is also set; the prompt otherwise appears only when targeting the latest tracked job"
        )]
        yes: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 330,
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
            help_heading = "Tool overrides",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
        #[arg(long, help = "Also purge tracked cached image artifacts for this job")]
        purge_cache: bool,
        #[arg(
            long,
            help = "Skip auto-exporting tracked artifacts to x-slurm.artifacts.export_dir before reaping runtime state"
        )]
        no_export: bool,
        #[arg(
            long,
            help = "Confirm this destructive action without prompting. Passing an explicit --job-id already skips the prompt (explicit intent), unless --purge-cache is also set; the prompt otherwise appears only when targeting the latest tracked job"
        )]
        yes: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 120,
        about = "Run a one-off command in one service environment",
        long_about = "Submit a fresh one-off job using either one service's image, environment, mounts, working directory, and prepare rules, or an ephemeral image supplied with --image. Service mode uses: hpc-compose run [-f compose.yaml] SERVICE -- CMD. Image mode uses: hpc-compose run --image IMAGE [resources] -- CMD.",
        after_help = RUN_HELP
    )]
    Run {
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(
            value_name = "ARGS",
            required = true,
            num_args = 1..,
            trailing_var_arg = true,
            allow_hyphen_values = true,
            help = "Service plus command in service mode, or command argv in --image mode"
        )]
        args: Vec<String>,
        #[arg(
            long,
            value_name = "IMAGE",
            help = "Container image for ephemeral image mode"
        )]
        image: Option<String>,
        #[arg(
            long,
            value_name = "NAME",
            help = "Settings resource profile to apply in ephemeral image mode"
        )]
        resources: Option<String>,
        #[arg(
            long,
            value_name = "TIME",
            help = "Slurm time limit for ephemeral image mode"
        )]
        time: Option<String>,
        #[arg(
            long,
            value_name = "MEM",
            help = "Slurm memory request for ephemeral image mode"
        )]
        mem: Option<String>,
        #[arg(
            long,
            value_name = "N",
            help = "Slurm CPUs per task for ephemeral image mode"
        )]
        cpus_per_task: Option<u32>,
        #[arg(
            long,
            value_name = "N",
            help = "Slurm GPU count for ephemeral image mode"
        )]
        gpus: Option<u32>,
        #[arg(
            long,
            value_name = "PARTITION",
            help = "Slurm partition for ephemeral image mode"
        )]
        partition: Option<String>,
        #[arg(
            long = "env",
            value_name = "KEY=VALUE",
            help = "Environment variable to pass into the ephemeral container"
        )]
        env: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Shared-FS dataset path to bind read-only and expose as HPC_COMPOSE_DATASET_DIR (image mode only)"
        )]
        dataset: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DIR",
            help = "Directory to export job artifacts into; exposed in-job as HPC_COMPOSE_OUTPUT_DIR (image mode only)"
        )]
        output: Option<PathBuf>,
        #[arg(
            long,
            help = "Run the ephemeral image locally through the local Pyxis-compatible launcher"
        )]
        local: bool,
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
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 125,
        about = "Open an interactive shell in a Slurm container",
        long_about = "Launch a thin direct srun --pty wrapper around a Pyxis container image. Defaults to bash -l inside the container.",
        after_help = SHELL_HELP
    )]
    Shell {
        #[arg(
            long,
            value_name = "IMAGE",
            required = true,
            help = "Container image to run interactively"
        )]
        image: String,
        #[arg(long, value_name = "NAME", help = "Settings resource profile to apply")]
        resources: Option<String>,
        #[arg(long, value_name = "TIME", help = "Slurm time limit")]
        time: Option<String>,
        #[arg(long, value_name = "MEM", help = "Slurm memory request")]
        mem: Option<String>,
        #[arg(long, value_name = "N", help = "Slurm CPUs per task")]
        cpus_per_task: Option<u32>,
        #[arg(long, value_name = "N", help = "Slurm GPU count")]
        gpus: Option<u32>,
        #[arg(long, value_name = "PARTITION", help = "Slurm partition")]
        partition: Option<String>,
        #[arg(
            long = "env",
            value_name = "KEY=VALUE",
            help = "Environment variable to pass into the interactive container"
        )]
        env: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
    },
    #[command(
        display_order = 126,
        about = "Launch a tracked JupyterLab or VS Code notebook server",
        long_about = "Submit a long-running, tracked interactive server (JupyterLab or VS Code `code tunnel`) as a single-service Slurm job, wait for it to become ready, and print the connection URL. Use --local to run on the current host through the local supervisor. Manage the session with `hpc-compose status` and stop it with `hpc-compose cancel`.",
        after_help = NOTEBOOK_HELP
    )]
    Notebook {
        #[arg(
            long,
            value_enum,
            default_value_t = super::NotebookKindArg::Jupyter,
            value_name = "KIND",
            help = "Interactive server preset"
        )]
        kind: super::NotebookKindArg,
        #[arg(
            long,
            value_name = "IMAGE",
            help = "Container image override (required for --kind vscode)"
        )]
        image: Option<String>,
        #[arg(
            long,
            value_name = "PORT",
            default_value_t = 8888,
            help = "Jupyter port (ignored for --kind vscode)"
        )]
        port: u16,
        #[arg(
            long,
            value_name = "TOKEN",
            help = "Jupyter auth token; a random token is generated when omitted"
        )]
        token: Option<String>,
        #[arg(
            long = "volume",
            value_name = "HOST:CONTAINER",
            help = "Additional host:container mount; may be passed multiple times"
        )]
        volumes: Vec<String>,
        #[arg(long, value_name = "PATH", help = "Container working directory")]
        working_dir: Option<String>,
        #[arg(
            long,
            value_name = "NAME",
            default_value = "hpc-compose",
            help = "VS Code tunnel name"
        )]
        tunnel_name: String,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "10m",
            help = "Give up waiting for the notebook to become ready after this duration"
        )]
        timeout: String,
        #[arg(
            long,
            help = "Stream service logs after readiness instead of detaching"
        )]
        follow: bool,
        #[arg(long, help = "Render the launcher script without submitting")]
        dry_run: bool,
        #[arg(
            value_name = "ARGS",
            num_args = 0..,
            trailing_var_arg = true,
            allow_hyphen_values = true,
            help = "Extra argv forwarded to the server command after --"
        )]
        args: Vec<String>,
        #[command(flatten)]
        launch: RuntimeLaunchArgs,
        #[arg(long, value_name = "NAME", help = "Settings resource profile to apply")]
        resources: Option<String>,
        #[arg(long, value_name = "TIME", help = "Slurm time limit")]
        time: Option<String>,
        #[arg(long, value_name = "MEM", help = "Slurm memory request")]
        mem: Option<String>,
        #[arg(long, value_name = "N", help = "Slurm CPUs per task")]
        cpus_per_task: Option<u32>,
        #[arg(long, value_name = "N", help = "Slurm GPU count")]
        gpus: Option<u32>,
        #[arg(long, value_name = "PARTITION", help = "Slurm partition")]
        partition: Option<String>,
        #[arg(
            long = "env",
            value_name = "KEY=VALUE",
            help = "Environment variable to pass into the notebook container"
        )]
        env: Vec<String>,
        #[arg(
            long,
            help = "Run on the current host through the local Pyxis-compatible launcher"
        )]
        local: bool,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered launcher script to this path"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 10,
        about = "Write a starter compose file from a built-in template",
        long_about = "Write a starter compose specification from a built-in template, or list and describe the available templates without writing a file. Use --cache-dir when the generated spec should pin an explicit shared cache directory.",
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
            help = "Optional shared cache directory written into the generated spec"
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
        display_order = 15,
        about = "Learn specs by progressively evolving a valid compose file",
        long_about = "Run an authoring-only tutorial that starts from a minimal valid spec and progressively adds services, readiness, failure policy, and multi-node placement. Each accepted step validates and writes the current spec without submitting a job, preparing images, or running preflight.",
        after_help = EVOLVE_HELP
    )]
    Evolve {
        #[arg(
            long,
            value_name = "LESSON",
            help = "Lesson id to run; defaults to progressive-complexity"
        )]
        lesson: Option<String>,
        #[arg(
            long,
            help = "List shipped evolve lessons and exit",
            conflicts_with_all = ["describe_lesson", "lesson", "name", "cache_dir", "output", "force", "yes", "until"]
        )]
        list_lessons: bool,
        #[arg(
            long = "describe-lesson",
            value_name = "LESSON",
            help = "Describe one evolve lesson and exit",
            conflicts_with_all = ["list_lessons", "lesson", "name", "cache_dir", "output", "force", "yes", "until"]
        )]
        describe_lesson: Option<String>,
        #[arg(
            long,
            value_name = "NAME",
            help = "Application name written into each accepted spec"
        )]
        name: Option<String>,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Optional shared cache directory written into each accepted spec"
        )]
        cache_dir: Option<String>,
        #[arg(
            long,
            value_name = "FILE",
            default_value = "compose.yaml",
            help = "Path to the compose file to evolve"
        )]
        output: PathBuf,
        #[arg(long, help = "Overwrite the output file if it already exists")]
        force: bool,
        #[arg(
            long,
            help = "Accept each step noninteractively; required with --format json"
        )]
        yes: bool,
        #[arg(
            long,
            value_name = "STEP",
            help = "Stop after this step id, for example readiness"
        )]
        until: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 300,
        about = "Inspect and prune cached image artifacts",
        long_about = "Inspect reusable imported and prepared image artifacts stored under the cache directory, or prune entries that are no longer needed.",
        after_help = CACHE_HELP
    )]
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
    #[command(
        display_order = 305,
        about = "Inspect and manage shared-cache rendezvous records",
        long_about = "Register, resolve, list, or prune cross-job service discovery records under the active cache directory."
    )]
    Rendezvous {
        #[command(subcommand)]
        command: RendezvousCommands,
    },
    #[command(
        display_order = 310,
        about = "List tracked jobs under the current repo tree",
        long_about = "Scan the current repository tree for tracked hpc-compose submissions and list the recorded jobs without querying the scheduler.",
        after_help = JOBS_HELP
    )]
    Jobs {
        #[command(subcommand)]
        command: JobsCommands,
    },
    #[command(
        display_order = 320,
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
        #[arg(long, help = "Confirm this destructive action without prompting")]
        yes: bool,
        #[arg(
            long,
            help = "Include recursive disk-usage totals for tracked job paths"
        )]
        disk_usage: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        display_order = 30,
        about = "Print resolved project-local settings context",
        long_about = "Print the effective project-local settings, selected profile, binaries, interpolation variables, and derived runtime paths for the active invocation context.",
        after_help = CONTEXT_HELP
    )]
    Context {
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            help = "Show sensitive-looking interpolation values instead of redacting them"
        )]
        show_values: bool,
    },
    #[command(
        display_order = 20,
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
            long = "cache-dir",
            value_name = "PATH",
            help = "Cache directory default written under the selected profile"
        )]
        cache_dir: Option<String>,
        #[arg(
            long,
            value_name = "HOST",
            help = "SSH login host used as the `up --remote` destination (bare host, ~/.ssh/config alias, or user@host)"
        )]
        login_host: Option<String>,
        #[arg(
            long,
            value_name = "USER",
            help = "SSH username applied to a bare login host for `up --remote` (destination becomes user@host)"
        )]
        login_user: Option<String>,
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
        display_order = 466,
        about = "Search shipped examples and starter templates",
        long_about = "List, search, and render a coverage table for shipped hpc-compose examples and built-in starter templates.",
        after_help = EXAMPLES_HELP
    )]
    Examples {
        #[command(subcommand)]
        command: ExamplesCommands,
    },
    #[command(
        display_order = 225,
        about = "Print the SSH tunnel to reach a tracked service from a laptop",
        long_about = "Resolve the SSH port-forward needed to reach a tracked service's TCP/HTTP readiness port from a laptop: the compute node comes from tracked status and the port from the service readiness. Prints the `ssh -L` command (with connection multiplexing so an OTP login node prompts only once), or runs it in the foreground with --open. Read-only; never daemonizes a tunnel.",
        after_help = REACH_HELP
    )]
    Reach {
        #[arg(
            value_name = "SERVICE",
            help = "Service whose readiness port should be forwarded"
        )]
        service: String,
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to reach instead of the latest recorded submission"
        )]
        job_id: Option<String>,
        #[arg(
            long,
            value_name = "PORT",
            help = "Override the forwarded port; required for services without TCP/HTTP readiness"
        )]
        port: Option<u16>,
        #[arg(
            long,
            help = "Run the port-forward in the foreground (Ctrl-C to stop) instead of printing the command"
        )]
        open: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        display_order = 256,
        about = "Aggregate one tracked run into a single read-only object",
        long_about = "Read-only aggregation over a single tracked run: combines scheduler status, the post-run efficiency score, the artifact manifest, and submit-time provenance into one object. Static-safe — it contacts the scheduler only as much as `status`/`score` already do (squeue, terminal-only sacct/sstat), never submits, cancels, exports, writes a file, or opens a connection.",
        after_help = EXPERIMENT_HELP
    )]
    Experiment {
        #[command(subcommand)]
        command: ExperimentCommands,
    },
    #[command(
        display_order = 480,
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
pub enum DoctorCommands {
    #[command(about = "Generate a best-effort cluster capability profile")]
    ClusterReport {
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long = "out",
            value_name = "PATH",
            help = "Write the cluster profile to this path; use '-' to print TOML"
        )]
        out: Option<PathBuf>,
    },
    #[command(about = "Render or run an MPI smoke probe for one service")]
    MpiSmoke {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "MPI service to smoke-test; inferred when exactly one MPI service exists"
        )]
        service: Option<String>,
        #[arg(
            long,
            help = "Submit the MPI smoke probe to Slurm; without this, only render/explain it"
        )]
        submit: bool,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered MPI smoke batch script to this path"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "5m",
            help = "Timeout for a submitted MPI smoke job"
        )]
        timeout: String,
    },
    #[command(about = "Render or run MPI and fabric smoke probes for one service")]
    FabricSmoke {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "MPI service to smoke-test; inferred when exactly one MPI service exists"
        )]
        service: Option<String>,
        #[arg(
            long,
            value_name = "CHECKS",
            help = "Fabric smoke checks: auto, mpi, nccl, ucx, ofi, or a comma-separated list"
        )]
        checks: Option<String>,
        #[arg(
            long,
            help = "Submit the fabric smoke probe to Slurm; without this, only render/explain it"
        )]
        submit: bool,
        #[arg(
            long,
            value_name = "OUTPUT",
            help = "Write the rendered fabric smoke batch script to this path"
        )]
        script_out: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "5m",
            help = "Timeout for a submitted fabric smoke job"
        )]
        timeout: String,
    },
    #[command(about = "Explain or run one service readiness probe from the current host")]
    Readiness {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "SERVICE",
            help = "Service readiness probe to inspect; inferred when exactly one service defines readiness"
        )]
        service: Option<String>,
        #[arg(
            long,
            help = "Run the readiness probe from the current host instead of only explaining it"
        )]
        run: bool,
        #[arg(
            long,
            value_name = "PATH",
            help = "Log file to inspect for readiness.type=log when --run is used"
        )]
        log_file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DURATION",
            help = "Override the probe timeout for this doctor run only"
        )]
        timeout: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum ExamplesCommands {
    #[command(about = "List shipped examples and starter templates")]
    List {
        #[arg(long, value_name = "TAG", help = "Only show examples with this tag")]
        tag: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<ExamplesOutputFormat>,
    },
    #[command(about = "Search shipped examples and starter templates")]
    Search {
        #[arg(value_name = "QUERY", help = "Free-text search query")]
        query: String,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<ExamplesOutputFormat>,
    },
    #[command(
        about = "Recommend starting examples for a workflow",
        long_about = "Recommend shipped examples or starter templates from static registry metadata. This command does not inspect the cluster, contact Slurm, or submit jobs."
    )]
    Recommend {
        #[arg(value_name = "QUERY", help = "Optional workflow description to match")]
        query: Option<String>,
        #[arg(
            long = "tag",
            value_name = "TAG",
            help = "Require a matching example tag; repeat to require multiple tags"
        )]
        tags: Vec<String>,
        #[arg(
            long,
            value_name = "N",
            default_value_t = 5,
            value_parser = parse_recommend_limit,
            help = "Maximum recommendations to print (1-20)"
        )]
        limit: usize,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(about = "Print the examples coverage table used by the docs")]
    Coverage {
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<ExamplesOutputFormat>,
    },
}

fn parse_recommend_limit(value: &str) -> Result<usize, String> {
    let limit = value
        .parse::<usize>()
        .map_err(|error| format!("expected a positive integer: {error}"))?;
    if (1..=20).contains(&limit) {
        Ok(limit)
    } else {
        Err("limit must be between 1 and 20".to_string())
    }
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
        #[arg(long, help = "Confirm this destructive action without prompting")]
        yes: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
}

#[derive(Debug, Subcommand)]
pub enum RendezvousCommands {
    #[command(about = "Register a provider record in the shared cache")]
    Register {
        #[arg(value_name = "NAME", help = "Rendezvous name")]
        name: String,
        #[arg(long, value_name = "HOST", help = "Reachable provider host")]
        host: String,
        #[arg(long, value_name = "PORT", help = "Reachable provider port")]
        port: u16,
        #[arg(
            long,
            value_name = "JOB_ID",
            help = "Owning Slurm job id; defaults to $SLURM_JOB_ID when set"
        )]
        job_id: Option<String>,
        #[arg(long, value_name = "SERVICE", help = "Optional owning service name")]
        service: Option<String>,
        #[arg(
            long,
            value_name = "PROTOCOL",
            default_value = "http",
            help = "URL protocol written into the record"
        )]
        protocol: String,
        #[arg(long, value_name = "PATH", help = "Optional URL path, such as /v1")]
        path: Option<String>,
        #[arg(
            long,
            value_name = "SECONDS",
            default_value_t = 3600,
            help = "Registration TTL"
        )]
        ttl_seconds: u64,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to use instead of the active context cache"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(about = "Resolve one provider record from the shared cache")]
    Resolve {
        #[arg(value_name = "NAME", help = "Rendezvous name")]
        name: String,
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to use instead of the active context cache"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(about = "List live provider records in the shared cache")]
    List {
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to use instead of the active context cache"
        )]
        cache_dir: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(about = "Remove expired provider records from the shared cache")]
    Prune {
        #[arg(
            long,
            value_name = "CACHE_DIR",
            help = "Cache directory to use instead of the active context cache"
        )]
        cache_dir: Option<PathBuf>,
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

/// Subcommands for the read-only `experiment` aggregator.
#[derive(Debug, Subcommand)]
pub enum ExperimentCommands {
    #[command(
        about = "Show one tracked run aggregated into a single object",
        long_about = "Aggregate a single tracked run's scheduler status, post-run efficiency score, artifact manifest, and submit-time provenance into one read-only object. Defaults to the latest tracked run when no job id is given. Static-safe: contacts the scheduler only as much as `status`/`score` already do and writes nothing.",
        after_help = EXPERIMENT_SHOW_HELP
    )]
    Show {
        #[arg(
            value_name = "JOB_ID",
            help = "Tracked Slurm job id to aggregate; defaults to the latest tracked run"
        )]
        job_id: Option<String>,
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "FLOAT",
            default_value_t = 1.20,
            help = "Power usage effectiveness multiplier for the embedded efficiency report"
        )]
        pue: f64,
        #[arg(
            long,
            value_name = "WATTS",
            default_value_t = 300.0,
            help = "Fallback GPU TDP in watts when sampler power is unavailable"
        )]
        gpu_tdp_w: f64,
        #[arg(
            long,
            value_name = "WATTS",
            default_value_t = 8.0,
            help = "Fallback CPU watts per allocated core for energy estimates"
        )]
        cpu_watts_per_core: f64,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum SweepCommands {
    #[command(
        about = "Submit all trials in an embedded sweep",
        long_about = "Expand the top-level sweep block, render one batch script per trial, submit each trial as an independent Slurm job, and persist a sweep manifest.",
        after_help = SWEEP_SUBMIT_HELP
    )]
    Submit {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            help = "Validate and print the expanded trials without writing scripts or submitting jobs"
        )]
        dry_run: bool,
        #[arg(
            long,
            value_name = "N",
            help = "Maximum number of trials allowed for this submission; defaults to 100"
        )]
        max_trials: Option<usize>,
        #[arg(long, help = "Skip image preparation and reuse existing artifacts")]
        skip_prepare: bool,
        #[arg(
            long,
            help = "Refresh imported and prepared artifacts before submission"
        )]
        force_rebuild: bool,
        #[arg(long, help = "Skip the preflight phase before submission")]
        no_preflight: bool,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sbatch",
            help_heading = "Tool overrides",
            help = "Path to the sbatch executable"
        )]
        sbatch_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "srun",
            help_heading = "Tool overrides",
            help = "Path to the srun executable"
        )]
        srun_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scontrol",
            help_heading = "Tool overrides",
            help = "Path to the scontrol executable"
        )]
        scontrol_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "enroot",
            help_heading = "Tool overrides",
            help = "Path to the enroot executable"
        )]
        enroot_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "apptainer",
            help_heading = "Tool overrides",
            help = "Path to the apptainer executable"
        )]
        apptainer_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "singularity",
            help_heading = "Tool overrides",
            help = "Path to the singularity executable"
        )]
        singularity_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "huggingface-cli",
            help_heading = "Tool overrides",
            help = "Path to the huggingface-cli executable used by hf:// stage-in trials"
        )]
        huggingface_cli_bin: String,
    },
    #[command(
        about = "Show aggregated tracked state for one sweep",
        long_about = "Load a persisted sweep manifest and query tracked scheduler/runtime state for each submitted trial.",
        after_help = SWEEP_STATUS_HELP
    )]
    Status {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "ID",
            help = "Sweep id to inspect; defaults to the latest sweep"
        )]
        sweep_id: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
    },
    #[command(
        about = "List persisted sweeps for a compose file",
        long_about = "List sweep manifests stored under the compose file's .hpc-compose/sweeps directory without querying the scheduler.",
        after_help = SWEEP_LIST_HELP
    )]
    List {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
    },
    #[command(
        about = "Parse and rank trial objectives for one sweep",
        long_about = "Read each terminal trial's objective (from a log regex or JSON artifact), write it back to the sweep manifest, and print a ranked table. With --watch --stop-when, poll until the objective condition is met and then stop the sweep.",
        after_help = SWEEP_OBSERVE_HELP
    )]
    Observe {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "ID",
            help = "Sweep id to observe; defaults to the latest sweep"
        )]
        sweep_id: Option<String>,
        #[arg(
            long,
            help = "Poll until a terminal trial satisfies --stop-when, then stop the sweep"
        )]
        watch: bool,
        #[arg(
            long,
            value_name = "EXPR",
            requires = "watch",
            help = "Stop condition, e.g. `objective < 0.05` or `objective > 0.9` (matches the sweep.objective.direction)"
        )]
        stop_when: Option<String>,
        #[arg(
            long,
            value_name = "DURATION",
            default_value = "30s",
            requires = "watch",
            help = "Polling interval for --watch"
        )]
        poll_interval: String,
        #[arg(
            long,
            value_name = "DURATION",
            requires = "watch",
            help = "Give up watching after this duration; 0s watches without a deadline"
        )]
        timeout: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            help = "Print a post-hoc scaling report (objective vs sweep.objective.scaling_axis: log-log slope + efficiency table) over terminal trials"
        )]
        scaling: bool,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scancel",
            help_heading = "Tool overrides",
            help = "Path to the scancel executable (used by --watch --stop-when)"
        )]
        scancel_bin: String,
    },
    #[command(
        about = "Cancel all non-terminal trials of one sweep",
        long_about = "Cancel every still-running or pending trial in a sweep via scancel and record the stop on the manifest. Use after `sweep observe` to realize early termination once an objective threshold is met.",
        after_help = SWEEP_STOP_HELP
    )]
    Stop {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "ID",
            help = "Sweep id to stop; defaults to the latest sweep"
        )]
        sweep_id: Option<String>,
        #[arg(long, help = "Skip the interactive confirmation prompt")]
        yes: bool,
        #[arg(
            long,
            value_name = "REASON",
            help = "Free-form stop reason recorded on the manifest"
        )]
        reason: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<OutputFormat>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "scancel",
            help_heading = "Tool overrides",
            help = "Path to the scancel executable"
        )]
        scancel_bin: String,
    },
    #[command(
        about = "Tabulate per-trial results for one sweep",
        long_about = "Read a persisted sweep manifest and print one tidy row per trial (each sweep variable as its own column, status, and parsed objective) as text, JSON, or CSV. Read-only: unlike `sweep observe`, it never writes objective state back to the manifest.",
        after_help = SWEEP_RESULTS_HELP
    )]
    Results {
        #[arg(short = 'f', long, value_name = "FILE", help = FILE_ARG_HELP)]
        file: Option<PathBuf>,
        #[arg(
            long,
            value_name = "ID",
            help = "Sweep id to tabulate; defaults to the latest sweep"
        )]
        sweep_id: Option<String>,
        #[arg(long, value_enum, value_name = "FORMAT", help = "Output format")]
        format: Option<CsvOutputFormat>,
        #[arg(
            long,
            value_name = "METRIC",
            value_delimiter = ',',
            help = "Extra per-trial columns: score, energy (comma-separated)"
        )]
        include: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "squeue",
            help_heading = "Tool overrides",
            help = "Path to the squeue executable"
        )]
        squeue_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sacct",
            help_heading = "Tool overrides",
            help = "Path to the sacct executable"
        )]
        sacct_bin: String,
        #[arg(
            long,
            value_name = "PATH",
            default_value = "sstat",
            help_heading = "Tool overrides",
            help = "Path to the sstat executable"
        )]
        sstat_bin: String,
    },
}
