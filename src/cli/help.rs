pub(super) const FILE_ARG_HELP: &str = "Compose specification file to read; if omitted, use the active context compose file or fall back to compose.yaml";

pub(super) const TOP_LEVEL_HELP: &str = "\
Normal run:
  hpc-compose up -f compose.yaml

Debugging flow:
  hpc-compose validate -f compose.yaml
  hpc-compose inspect --verbose -f compose.yaml
  hpc-compose preflight -f compose.yaml
  hpc-compose prepare -f compose.yaml
  hpc-compose config -f compose.yaml

Start a new spec:
  hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml";

pub(super) const VALIDATE_HELP: &str = "\
Examples:
  hpc-compose validate -f compose.yaml
  hpc-compose validate -f compose.yaml --strict-env
  hpc-compose validate -f compose.yaml --format json";

pub(super) const RENDER_HELP: &str = "\
Examples:
  hpc-compose render -f compose.yaml
  hpc-compose render -f compose.yaml --output job.sbatch
  hpc-compose render -f compose.yaml --format json";

pub(super) const PREPARE_HELP: &str = "\
Examples:
  hpc-compose prepare -f compose.yaml
  hpc-compose prepare -f compose.yaml --force
  hpc-compose prepare -f compose.yaml --format json";

pub(super) const PREFLIGHT_HELP: &str = "\
Examples:
  hpc-compose preflight -f compose.yaml
  hpc-compose preflight -f compose.yaml --strict
  hpc-compose preflight -f compose.yaml --format json";

pub(super) const INSPECT_HELP: &str = "\
Examples:
  hpc-compose inspect -f compose.yaml
  hpc-compose inspect --verbose -f compose.yaml
  hpc-compose inspect -f compose.yaml --format json";

pub(super) const CONFIG_HELP: &str = "\
Examples:
  hpc-compose config -f compose.yaml
  hpc-compose config -f compose.yaml --format json";

pub(super) const SCHEMA_HELP: &str = "\
Examples:
  hpc-compose schema
  hpc-compose schema > hpc-compose.schema.json";

pub(super) const UP_HELP: &str = "\
Examples:
  hpc-compose up -f compose.yaml
  hpc-compose up --dry-run -f compose.yaml
  hpc-compose up --skip-prepare -f compose.yaml";

pub(super) const SUBMIT_HELP: &str = "\
Examples:
  hpc-compose submit --watch -f compose.yaml
  hpc-compose submit --dry-run -f compose.yaml
  hpc-compose submit --local --dry-run -f compose.yaml
  hpc-compose submit --skip-prepare -f compose.yaml
  hpc-compose submit --resume-diff-only -f compose.yaml";

pub(super) const STATUS_HELP: &str = "\
Examples:
  hpc-compose status -f compose.yaml
  hpc-compose status -f compose.yaml --format json";

pub(super) const STATS_HELP: &str = "\
Examples:
  hpc-compose stats -f compose.yaml
  hpc-compose stats -f compose.yaml --format json
  hpc-compose stats -f compose.yaml --format csv";

pub(super) const ARTIFACTS_HELP: &str = "\
Examples:
  hpc-compose artifacts -f compose.yaml
  hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
  hpc-compose artifacts -f compose.yaml --format json";

pub(super) const LOGS_HELP: &str = "\
Examples:
  hpc-compose logs -f compose.yaml
  hpc-compose logs -f compose.yaml --service app --follow
  hpc-compose logs -f compose.yaml --job-id 12345 --lines 200";

pub(super) const PS_HELP: &str = "\
Examples:
  hpc-compose ps -f compose.yaml
  hpc-compose ps -f compose.yaml --job-id 12345
  hpc-compose ps -f compose.yaml --format json";

pub(super) const WATCH_HELP: &str = "\
Examples:
  hpc-compose watch -f compose.yaml
  hpc-compose watch -f compose.yaml --service app
  hpc-compose watch -f compose.yaml --job-id 12345 --lines 200";

pub(super) const CANCEL_HELP: &str = "\
Examples:
  hpc-compose cancel -f compose.yaml
  hpc-compose cancel -f compose.yaml --job-id 12345";

pub(super) const DOWN_HELP: &str = "\
Examples:
  hpc-compose down -f compose.yaml
  hpc-compose down --job-id 12345
  hpc-compose down --job-id 12345 --purge-cache";

pub(super) const RUN_HELP: &str = "\
Examples:
  hpc-compose run -f compose.yaml app -- python -m pytest
  hpc-compose run -f compose.yaml app -- bash
  hpc-compose run -f compose.yaml worker -- python worker.py --once";

pub(super) const NEW_HELP: &str = "\
Examples:
  hpc-compose new --list-templates
  hpc-compose new --describe-template minimal-batch
  hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml";

pub(super) const CACHE_HELP: &str = "\
Examples:
  hpc-compose cache list
  hpc-compose cache inspect -f compose.yaml
  hpc-compose cache prune --age 7";

pub(super) const CACHE_LIST_HELP: &str = "\
Examples:
  hpc-compose cache list
  hpc-compose cache list --cache-dir '<shared-cache-dir>'
  hpc-compose cache list --format json";

pub(super) const CACHE_INSPECT_HELP: &str = "\
Examples:
  hpc-compose cache inspect -f compose.yaml
  hpc-compose cache inspect -f compose.yaml --service app
  hpc-compose cache inspect -f compose.yaml --format json";

pub(super) const CACHE_PRUNE_HELP: &str = "\
Examples:
  hpc-compose --profile dev cache prune --age 14
  hpc-compose cache prune --all-unused -f compose.yaml
  hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'
  hpc-compose cache prune --age 7 --format json";

pub(super) const JOBS_HELP: &str = "\
Examples:
  hpc-compose jobs list
  hpc-compose jobs list --disk-usage
  hpc-compose jobs list --format json";

pub(super) const CLEAN_HELP: &str = "\
Examples:
  hpc-compose clean --age 7
  hpc-compose clean --all --dry-run
  hpc-compose clean --all --format json";

pub(super) const CONTEXT_HELP: &str = "\
Examples:
  hpc-compose context
  hpc-compose context --format json
  hpc-compose --profile dev context";

pub(super) const SETUP_HELP: &str = "\
Examples:
  hpc-compose setup
  hpc-compose setup --profile-name dev --compose-file compose.yaml --default-profile dev --non-interactive
  hpc-compose setup --env 'CACHE_DIR=<shared-cache-dir>' --binary srun=/opt/slurm/bin/srun --non-interactive";

pub(super) const COMPLETIONS_HELP: &str = "\
Examples:
  hpc-compose completions bash
  hpc-compose completions zsh > ~/.zfunc/_hpc-compose
  hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish";

const TOP_LEVEL_EXAMPLES: &[&str] = &[
    "hpc-compose up -f compose.yaml",
    "hpc-compose validate -f compose.yaml",
    "hpc-compose inspect --verbose -f compose.yaml",
    "hpc-compose config -f compose.yaml",
    "hpc-compose preflight -f compose.yaml",
];

const VALIDATE_EXAMPLES: &[&str] = &[
    "hpc-compose validate -f compose.yaml",
    "hpc-compose validate -f compose.yaml --strict-env",
    "hpc-compose validate -f compose.yaml --format json",
];

const RENDER_EXAMPLES: &[&str] = &[
    "hpc-compose render -f compose.yaml",
    "hpc-compose render -f compose.yaml --output job.sbatch",
    "hpc-compose render -f compose.yaml --format json",
];

const PREPARE_EXAMPLES: &[&str] = &[
    "hpc-compose prepare -f compose.yaml",
    "hpc-compose prepare -f compose.yaml --force",
    "hpc-compose prepare -f compose.yaml --format json",
];

const PREFLIGHT_EXAMPLES: &[&str] = &[
    "hpc-compose preflight -f compose.yaml",
    "hpc-compose preflight -f compose.yaml --strict",
    "hpc-compose preflight -f compose.yaml --format json",
];

const INSPECT_EXAMPLES: &[&str] = &[
    "hpc-compose inspect -f compose.yaml",
    "hpc-compose inspect --verbose -f compose.yaml",
    "hpc-compose inspect -f compose.yaml --format json",
];

const CONFIG_EXAMPLES: &[&str] = &[
    "hpc-compose config -f compose.yaml",
    "hpc-compose config -f compose.yaml --format json",
];

const SCHEMA_EXAMPLES: &[&str] = &[
    "hpc-compose schema",
    "hpc-compose schema > hpc-compose.schema.json",
];

const UP_EXAMPLES: &[&str] = &[
    "hpc-compose up -f compose.yaml",
    "hpc-compose up --dry-run -f compose.yaml",
    "hpc-compose up --skip-prepare -f compose.yaml",
];

const SUBMIT_EXAMPLES: &[&str] = &[
    "hpc-compose submit --watch -f compose.yaml",
    "hpc-compose submit --dry-run -f compose.yaml",
    "hpc-compose submit --local --dry-run -f compose.yaml",
    "hpc-compose submit --skip-prepare -f compose.yaml",
    "hpc-compose submit --resume-diff-only -f compose.yaml",
];

const STATUS_EXAMPLES: &[&str] = &[
    "hpc-compose status -f compose.yaml",
    "hpc-compose status -f compose.yaml --format json",
];

const STATS_EXAMPLES: &[&str] = &[
    "hpc-compose stats -f compose.yaml",
    "hpc-compose stats -f compose.yaml --format json",
    "hpc-compose stats -f compose.yaml --format csv",
];

const ARTIFACTS_EXAMPLES: &[&str] = &[
    "hpc-compose artifacts -f compose.yaml",
    "hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball",
    "hpc-compose artifacts -f compose.yaml --format json",
];

const LOGS_EXAMPLES: &[&str] = &[
    "hpc-compose logs -f compose.yaml",
    "hpc-compose logs -f compose.yaml --service app --follow",
    "hpc-compose logs -f compose.yaml --job-id 12345 --lines 200",
];

const PS_EXAMPLES: &[&str] = &[
    "hpc-compose ps -f compose.yaml",
    "hpc-compose ps -f compose.yaml --job-id 12345",
    "hpc-compose ps -f compose.yaml --format json",
];

const WATCH_EXAMPLES: &[&str] = &[
    "hpc-compose watch -f compose.yaml",
    "hpc-compose watch -f compose.yaml --service app",
    "hpc-compose watch -f compose.yaml --job-id 12345 --lines 200",
];

const CANCEL_EXAMPLES: &[&str] = &[
    "hpc-compose cancel -f compose.yaml",
    "hpc-compose cancel -f compose.yaml --job-id 12345",
];

const DOWN_EXAMPLES: &[&str] = &[
    "hpc-compose down -f compose.yaml",
    "hpc-compose down --job-id 12345",
    "hpc-compose down --job-id 12345 --purge-cache",
];

const RUN_EXAMPLES: &[&str] = &[
    "hpc-compose run -f compose.yaml app -- python -m pytest",
    "hpc-compose run -f compose.yaml app -- bash",
    "hpc-compose run -f compose.yaml worker -- python worker.py --once",
];

const NEW_EXAMPLES: &[&str] = &[
    "hpc-compose new --list-templates",
    "hpc-compose new --describe-template minimal-batch",
    "hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml",
];

const CACHE_EXAMPLES: &[&str] = &[
    "hpc-compose cache list",
    "hpc-compose cache inspect -f compose.yaml",
    "hpc-compose cache prune --age 7",
];

const CACHE_LIST_EXAMPLES: &[&str] = &[
    "hpc-compose cache list",
    "hpc-compose cache list --cache-dir '<shared-cache-dir>'",
    "hpc-compose cache list --format json",
];

const CACHE_INSPECT_EXAMPLES: &[&str] = &[
    "hpc-compose cache inspect -f compose.yaml",
    "hpc-compose cache inspect -f compose.yaml --service app",
    "hpc-compose cache inspect -f compose.yaml --format json",
];

const CACHE_PRUNE_EXAMPLES: &[&str] = &[
    "hpc-compose --profile dev cache prune --age 14",
    "hpc-compose cache prune --all-unused -f compose.yaml",
    "hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'",
    "hpc-compose cache prune --age 7 --format json",
];

const JOBS_EXAMPLES: &[&str] = &[
    "hpc-compose jobs list",
    "hpc-compose jobs list --disk-usage",
    "hpc-compose jobs list --format json",
];

const CLEAN_EXAMPLES: &[&str] = &[
    "hpc-compose clean --age 7",
    "hpc-compose clean --all --dry-run",
    "hpc-compose clean --all --format json",
];

const CONTEXT_EXAMPLES: &[&str] = &[
    "hpc-compose context",
    "hpc-compose context --format json",
    "hpc-compose --profile dev context",
];

const SETUP_EXAMPLES: &[&str] = &[
    "hpc-compose setup",
    "hpc-compose setup --profile-name dev --compose-file compose.yaml --default-profile dev --non-interactive",
    "hpc-compose setup --env 'CACHE_DIR=<shared-cache-dir>' --binary srun=/opt/slurm/bin/srun --non-interactive",
];

const COMPLETIONS_EXAMPLES: &[&str] = &[
    "hpc-compose completions bash",
    "hpc-compose completions zsh > ~/.zfunc/_hpc-compose",
    "hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish",
];

#[must_use]
pub fn examples_for_path(path: &[&str]) -> &'static [&'static str] {
    match path {
        [] => TOP_LEVEL_EXAMPLES,
        ["validate"] => VALIDATE_EXAMPLES,
        ["render"] => RENDER_EXAMPLES,
        ["prepare"] => PREPARE_EXAMPLES,
        ["preflight"] => PREFLIGHT_EXAMPLES,
        ["inspect"] => INSPECT_EXAMPLES,
        ["config"] => CONFIG_EXAMPLES,
        ["schema"] => SCHEMA_EXAMPLES,
        ["up"] => UP_EXAMPLES,
        ["submit"] => SUBMIT_EXAMPLES,
        ["status"] => STATUS_EXAMPLES,
        ["stats"] => STATS_EXAMPLES,
        ["artifacts"] => ARTIFACTS_EXAMPLES,
        ["logs"] => LOGS_EXAMPLES,
        ["ps"] => PS_EXAMPLES,
        ["watch"] => WATCH_EXAMPLES,
        ["cancel"] => CANCEL_EXAMPLES,
        ["down"] => DOWN_EXAMPLES,
        ["run"] => RUN_EXAMPLES,
        ["new"] => NEW_EXAMPLES,
        ["cache"] => CACHE_EXAMPLES,
        ["cache", "list"] => CACHE_LIST_EXAMPLES,
        ["cache", "inspect"] => CACHE_INSPECT_EXAMPLES,
        ["cache", "prune"] => CACHE_PRUNE_EXAMPLES,
        ["jobs"] => JOBS_EXAMPLES,
        ["jobs", "list"] => JOBS_EXAMPLES,
        ["clean"] => CLEAN_EXAMPLES,
        ["context"] => CONTEXT_EXAMPLES,
        ["setup"] => SETUP_EXAMPLES,
        ["completions"] => COMPLETIONS_EXAMPLES,
        _ => &[],
    }
}
