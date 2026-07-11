pub(super) const FILE_ARG_HELP: &str = "Compose specification file to read; if omitted, use the active context compose file or fall back to compose.yaml";

/// Workflow groups shown under `hpc-compose --help`, in display order. This is
/// the single source of truth for that listing: [`top_level_help`] renders the
/// help block from it, and `tests` cross-checks it against the real clap
/// subcommand set, so the list can never silently drift from the actual command
/// surface again (it previously dropped `pull`, `checkpoints`, `experiment`, and
/// `reach`). Every non-hidden top-level command must appear in exactly one group.
pub(super) const WORKFLOW_GROUPS: &[(&str, &str, &[&str])] = &[
    (
        "Start",
        "scaffold and configure a spec",
        &["new", "evolve", "setup", "context"],
    ),
    (
        "Plan/Run",
        "inspect statically, then submit or launch",
        &["plan", "up", "when", "alloc", "run", "shell", "germinate"],
    ),
    (
        "Develop/Test",
        "iterate and smoke-test on a single host",
        &["test", "dev", "tmux", "notebook"],
    ),
    (
        "Observe/Debug",
        "monitor, inspect, and diagnose runs",
        &[
            "weather",
            "doctor",
            "debug",
            "watch",
            "replay",
            "status",
            "logs",
            "ps",
            "stats",
            "score",
            "diff",
            "artifacts",
            "sweep",
            "reach",
            "pull",
            "checkpoints",
            "experiment",
        ],
    ),
    (
        "Maintain",
        "clean up tracked state and resources",
        &[
            "cache",
            "workspace",
            "jobs",
            "clean",
            "down",
            "cancel",
            "rendezvous",
        ],
    ),
    (
        "Advanced",
        "low-level spec authoring and tooling",
        &[
            "examples",
            "docs",
            "feedback",
            "validate",
            "lint",
            "lsp",
            "inspect",
            "config",
            "render",
            "explain",
            "prepare",
            "preflight",
            "schema",
            "completions",
        ],
    ),
];

const TOP_LEVEL_HELP_PREAMBLE: &str = "\
Start from an existing spec:
  hpc-compose plan -f compose.yaml
  hpc-compose up -f compose.yaml

Create or evolve a spec:
  hpc-compose new --template minimal-batch --name my-app --output compose.yaml
  hpc-compose evolve --output compose.yaml

Not sure which command fits? Describe the goal and get a recommendation:
  hpc-compose examples recommend 'multi-node training' --tag gpu

Run when cluster conditions are friendlier:
  hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4

Debug failed run:
  hpc-compose debug -f compose.yaml --preflight";

const TOP_LEVEL_HELP_FOOTER: &str = "Use `hpc-compose help <command>` for command details.";

/// Renders the `Workflow groups:` block from [`WORKFLOW_GROUPS`]. Each group
/// shows a one-line purpose so the listing answers "which area?" at a glance,
/// then its commands; drill in with `hpc-compose help <command>` for per-command
/// detail.
fn workflow_groups_block() -> String {
    let mut block = String::from("Workflow groups:");
    for (label, description, names) in WORKFLOW_GROUPS {
        let display_names = names
            .iter()
            .map(|name| workflow_group_command_label(name))
            .collect::<Vec<_>>()
            .join(", ");
        block.push_str(&format!(
            "\n  {label}: {description}\n    {}",
            display_names
        ));
    }
    block
}

fn workflow_group_command_label(name: &str) -> &str {
    match name {
        "new" => "new/init",
        _ => name,
    }
}

/// Builds the top-level `--help` epilogue (`after_help`). Generated rather than
/// hand-maintained so the workflow-group listing stays in lockstep with the
/// actual command set (guarded by `tests::workflow_groups_match_every_command_exactly_once`).
pub(super) fn top_level_help() -> String {
    format!(
        "{TOP_LEVEL_HELP_PREAMBLE}\n\n{}\n\n{TOP_LEVEL_HELP_FOOTER}",
        workflow_groups_block()
    )
}

pub(super) const VALIDATE_HELP: &str = "\
Examples:
  hpc-compose validate -f compose.yaml
  hpc-compose validate -f compose.yaml --strict-env
  hpc-compose validate -f compose.yaml --format json";

pub(super) const LINT_HELP: &str = "\
Examples:
  hpc-compose lint -f compose.yaml
  hpc-compose lint -f compose.yaml --allow-warnings
  hpc-compose lint -f compose.yaml --fix
  hpc-compose lint -f compose.yaml --fix --dry-run
  hpc-compose lint -f compose.yaml --format json";

pub(super) const LSP_HELP: &str = "\
Examples:
  hpc-compose lsp
  hpc-compose lsp --strict-env
  hpc-compose --profile gpu lsp

The server speaks LSP over stdio and publishes diagnostics only. Editors and
agents should send full-document file:// YAML documents and inspect
Diagnostic.data.field and Diagnostic.data.recommendation for structured
authoring guidance.";

pub(super) const RENDER_HELP: &str = "\
Examples:
  hpc-compose render -f compose.yaml
  hpc-compose render -f compose.yaml --annotate
  hpc-compose render -f compose.yaml --output job.sbatch
  hpc-compose render -f compose.yaml --format json";

pub(super) const EXPLAIN_HELP: &str = "\
Examples:
  hpc-compose explain -f compose.yaml
  hpc-compose explain -f compose.yaml --field x-slurm.time
  hpc-compose explain -f compose.yaml --field services.app.readiness
  hpc-compose explain -f compose.yaml --line 42
  hpc-compose explain -f compose.yaml --format json

Line numbers refer to the preview script exactly as printed by `render` and
`plan --show-script` (JOB_ROOT keeps the portable ${SLURM_SUBMIT_DIR:-$PWD}
form), not to a submitted .sbatch, which can bake absolute runtime paths.";

pub(super) const PREPARE_HELP: &str = "\
Examples:
  hpc-compose prepare -f compose.yaml
  hpc-compose prepare -f compose.yaml --force-rebuild
  hpc-compose prepare -f compose.yaml --format json";

pub(super) const PREFLIGHT_HELP: &str = "\
Examples:
  hpc-compose preflight -f compose.yaml
  hpc-compose preflight -f compose.yaml --strict
  hpc-compose preflight -f compose.yaml --fs-probes
  hpc-compose preflight -f compose.yaml --format json";

pub(super) const WEATHER_HELP: &str = "\
Examples:
  hpc-compose weather
  hpc-compose weather --format json
  hpc-compose weather --sinfo-bin /site/bin/sinfo --squeue-bin /site/bin/squeue";

pub(super) const INSPECT_HELP: &str = "\
Examples:
  hpc-compose inspect -f compose.yaml
  hpc-compose inspect --verbose -f compose.yaml
  hpc-compose inspect -f compose.yaml --format json
  hpc-compose inspect --rightsize -f compose.yaml";

pub(super) const CONFIG_HELP: &str = "\
Examples:
  hpc-compose config -f compose.yaml
  hpc-compose config -f compose.yaml --format json";

pub(super) const SCHEMA_HELP: &str = "\
Examples:
  hpc-compose schema
  hpc-compose schema > hpc-compose.schema.json";

pub(super) const PLAN_HELP: &str = "\
Examples:
  hpc-compose plan -f compose.yaml
  hpc-compose plan --verbose -f compose.yaml
  hpc-compose plan --explain -f compose.yaml
  hpc-compose plan --show-script -f compose.yaml
  hpc-compose plan --show-script --annotate -f compose.yaml
  hpc-compose plan -f compose.yaml --format json";

pub(super) const UP_HELP: &str = "\
Examples:
  hpc-compose up -f compose.yaml
  hpc-compose up --detach -f compose.yaml
  hpc-compose up --detach --format json -f compose.yaml
  hpc-compose up --detach --format json --print-endpoints -f compose.yaml
  hpc-compose up --dry-run -f compose.yaml
  hpc-compose up --watch-queue --queue-warn-after 15m -f compose.yaml
  hpc-compose up --watch-mode line -f compose.yaml
  hpc-compose up --hold-on-exit always -f compose.yaml

Flag constraints:
  --format requires --detach or --dry-run.
  --watch-queue cannot be combined with --detach, --dry-run, or --local.
  --queue-warn-after requires --watch-queue.";

pub(super) const TEST_HELP: &str = "\
Examples:
  hpc-compose test --local -f compose.yaml
  hpc-compose test --submit --time 00:01:00 -f compose.yaml
  hpc-compose test --submit --dev-cluster -f compose.yaml
  hpc-compose test --preemption --preemption-grace 10s -f compose.yaml
  hpc-compose test --submit --timeout 180s --format json -f compose.yaml

Smoke tests are finite: every service must start, pass configured readiness, and complete successfully.
--dev-cluster delegates test --submit to the checked-in local Slurm dev cluster from a source checkout.
Preemption tests submit to Slurm, send the configured x-slurm.signal, requeue the job, and require a resumed attempt with passing service assertions.";

pub(super) const DEV_HELP: &str = "\
Examples:
  hpc-compose dev -f compose.yaml
  hpc-compose dev -f compose.yaml --watch-paths ./src
  hpc-compose dev -f compose.yaml --debounce-ms 500 --keep-running

Local dev mode watches bind-mounted source directories and asks the local supervisor to restart affected services.";

pub(super) const TMUX_HELP: &str = "\
Examples:
  hpc-compose tmux -f compose.yaml
  hpc-compose tmux -f compose.yaml --job-id local-123
  hpc-compose tmux -f compose.yaml --session demo --no-attach

tmux panes tail service logs; the local supervisor still owns process launch and restarts.";

pub(super) const SWEEP_HELP: &str = "\
Examples:
  hpc-compose sweep submit -f train.yaml --dry-run
  hpc-compose sweep submit -f train.yaml
  hpc-compose sweep status -f train.yaml
  hpc-compose sweep list -f train.yaml";

pub(super) const SWEEP_SUBMIT_HELP: &str = "\
Examples:
  hpc-compose sweep submit -f train.yaml --dry-run
  hpc-compose sweep submit -f train.yaml --max-trials 200
  hpc-compose sweep submit -f train.yaml --format json
  hpc-compose sweep submit -f train.yaml --resume            # resubmit only trials that never got a job
  hpc-compose sweep submit -f train.yaml --resume --sweep-id sweep-123 --dry-run";

pub(super) const SWEEP_STATUS_HELP: &str = "\
Examples:
  hpc-compose sweep status -f train.yaml
  hpc-compose sweep status -f train.yaml --sweep-id sweep-123
  hpc-compose sweep status -f train.yaml --format json";

pub(super) const SWEEP_LIST_HELP: &str = "\
Examples:
  hpc-compose sweep list -f train.yaml
  hpc-compose sweep list -f train.yaml --format json";

pub(super) const SWEEP_OBSERVE_HELP: &str = "\
Examples:
  hpc-compose sweep observe -f train.yaml
  hpc-compose sweep observe -f train.yaml --format json
  hpc-compose sweep observe -f train.yaml --watch --stop-when 'objective < 0.05'";

pub(super) const SWEEP_STOP_HELP: &str = "\
Examples:
  hpc-compose sweep stop -f train.yaml
  hpc-compose sweep stop -f train.yaml --yes --reason 'objective threshold met'";

pub(super) const SWEEP_RESULTS_HELP: &str = "\
Examples:
  hpc-compose sweep results -f train.yaml
  hpc-compose sweep results -f train.yaml --format csv > runs.csv
  hpc-compose sweep results -f train.yaml --include score,energy --format json";

pub(super) const WHEN_HELP: &str = "\
Examples:
  hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4
  hpc-compose when -f compose.yaml --after-job 12345
  hpc-compose when -f compose.yaml --between 22:00-06:00
  hpc-compose when --detach --format json -f compose.yaml --partition gpu8 --free-nodes 4";

pub(super) const ALLOC_HELP: &str = "\
Examples:
  hpc-compose alloc -f compose.yaml
  hpc-compose alloc -f compose.yaml -- bash -lc 'hpc-compose run app -- python -m pytest'
  hpc-compose alloc -f compose.yaml --skip-prepare";

pub(super) const STATUS_HELP: &str = "\
Examples:
  hpc-compose status -f compose.yaml
  hpc-compose status -f compose.yaml --array
  hpc-compose status -f compose.yaml --verify
  hpc-compose status -f compose.yaml --verify --format json
Use --verify to report contradictions between scheduler state, tracked runtime
files, logs, checkpoints, and artifacts. It is read-only and suggests explicit
next commands instead of repairing state automatically.";

pub(super) const STATS_HELP: &str = "\
Examples:
  hpc-compose stats -f compose.yaml
  hpc-compose stats -f compose.yaml --format json
  hpc-compose stats -f compose.yaml --accounting --format csv
  hpc-compose stats -f compose.yaml --format csv";

pub(super) const SCORE_HELP: &str = "\
Examples:
  hpc-compose score 12345
  hpc-compose score -f compose.yaml
  hpc-compose score 12345 --pue 1.3 --gpu-tdp-w 350 --format json";

pub(super) const DIFF_HELP: &str = "\
Examples:
  hpc-compose diff 12345 12346 -f compose.yaml
  hpc-compose diff 12345 12346 --format json
  hpc-compose diff --jobs 12345,12346,12347 --matrix-format json
  hpc-compose diff --across sweep-1700000000-1234 --matrix-format csv
  hpc-compose diff --against-spec --job-id 12345 -f compose.yaml
  hpc-compose diff --against-spec --fail-on-change && hpc-compose up";

pub(super) const ARTIFACTS_HELP: &str = "\
Examples:
  hpc-compose artifacts -f compose.yaml
  hpc-compose artifacts -f compose.yaml --bundle checkpoints --tarball
  hpc-compose artifacts -f compose.yaml --format json";

pub(super) const LOGS_HELP: &str = "\
Examples:
  hpc-compose logs -f compose.yaml
  hpc-compose logs -f compose.yaml --service app --follow
  hpc-compose logs -f compose.yaml --grep 'error|oom' --since 30m
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
  hpc-compose watch -f compose.yaml --job-id 12345 --lines 200
  hpc-compose watch -f compose.yaml --watch-mode line
  hpc-compose watch -f compose.yaml --hold-on-exit always";

pub(super) const REPLAY_HELP: &str = "\
Examples:
  hpc-compose replay -f compose.yaml
  hpc-compose replay -f compose.yaml --speed 10
  hpc-compose replay -f compose.yaml --job-id 12345 --service app
  hpc-compose replay -f compose.yaml --format json";

pub(super) const CHECKPOINTS_HELP: &str = "\
Examples:
  hpc-compose checkpoints -f compose.yaml
  hpc-compose checkpoints --job-id 12345
  hpc-compose checkpoints --format json";

pub(super) const DEBUG_HELP: &str = "\
Examples:
  hpc-compose debug -f compose.yaml
  hpc-compose debug -f compose.yaml --preflight
  hpc-compose debug -f compose.yaml --service app --lines 200
  hpc-compose debug -f compose.yaml --format json";

pub(super) const CANCEL_HELP: &str = "\
Examples:
  hpc-compose cancel -f compose.yaml
  hpc-compose cancel -f compose.yaml --job-id 12345
  hpc-compose cancel -f compose.yaml --yes";

pub(super) const DOWN_HELP: &str = "\
Examples:
  hpc-compose down -f compose.yaml
  hpc-compose down --job-id 12345
  hpc-compose down --job-id 12345 --purge-cache --yes";

pub(super) const RUN_HELP: &str = "\
Examples:
  hpc-compose run -f compose.yaml app -- python -m pytest
  hpc-compose run -f compose.yaml app -- bash
  hpc-compose alloc -f compose.yaml
  hpc-compose run -f compose.yaml app -- python -m pytest
  hpc-compose run --image docker://python:3.12 -- python -V
  hpc-compose run --image docker://python:3.12 --dataset /scratch/data --output ./results -- python infer.py";

pub(super) const SHELL_HELP: &str = "\
Examples:
  hpc-compose shell --image docker://ubuntu:24.04
  hpc-compose shell --image docker://nvidia/cuda:12.4.1-base-ubuntu22.04 --gpus 1
  hpc-compose shell --image docker://python:3.12 --resources cpu-small";

pub(super) const NOTEBOOK_HELP: &str = "\
Examples:
  hpc-compose notebook --kind jupyter --gpus 1 --volume ./project:/workspace
  hpc-compose notebook --kind jupyter --local --volume ./src:/workspace
  hpc-compose notebook --kind vscode --image ghcr.io/example/code:1 --gpus 1
  hpc-compose notebook --dry-run --script-out notebook.sbatch
  hpc-compose notebook --follow --kind jupyter
  hpc-compose notebook --kind jupyter --format json
Set login_host in settings so the tunnel hint names your real SSH login host.
Stop the server with the management command printed after launch.";

pub(super) const REACH_HELP: &str = "\
Examples:
  hpc-compose reach api -f compose.yaml
  hpc-compose reach api -f compose.yaml --format json
  hpc-compose reach jupyter --port 8888 --open
On an OTP/2FA login node, the printed ssh command reuses one authenticated
connection (ControlMaster), so you enter the OTP only once.";

pub(super) const PULL_HELP: &str = "\
Examples:
  hpc-compose pull -f compose.yaml --into ./results
  hpc-compose pull --job-id 4815162 --into ./results
  hpc-compose pull -f compose.yaml --format json
hpc-compose only prints the rsync command (it copies nothing). The ssh transport
uses ControlMaster, so an OTP/2FA login node prompts only once.";

pub(super) const EXPERIMENT_HELP: &str = "\
Track one run: `show` aggregates it into one read-only object, `bundle` writes
a local reproducibility bundle, and `tag`/`note` attach labels and timestamped
observations to its tracked record.";

pub(super) const EXPERIMENT_TAG_HELP: &str = "\
Examples:
  hpc-compose experiment tag baseline
  hpc-compose experiment tag baseline lr-bug --job-id 12345
  hpc-compose experiment tag --remove lr-bug --job-id 12345 --format json
Tags are a sorted set on the tracked record: adding an existing tag or removing
an absent one is a no-op. Filter with `hpc-compose jobs list --tag <TAG>`.
Allowed characters: letters, digits, '.', '_', '-' (max 64 chars, 32 tags).";

pub(super) const EXPERIMENT_NOTE_HELP: &str = "\
Examples:
  hpc-compose experiment note 'diverged after epoch 3'
  hpc-compose experiment note 'baseline for v2 sweep' --job-id 12345
  hpc-compose experiment note 'lr too high' --format json
Notes are append-only and timestamped; `experiment show` prints them with the
run. Nothing is sent to the scheduler.";

pub(super) const EXPERIMENT_SHOW_HELP: &str = "\
Examples:
  hpc-compose experiment show 12345
  hpc-compose experiment show -f compose.yaml
  hpc-compose experiment show 12345 --format json
Read-only: status + score + artifacts + provenance in one object. The printed
next_commands carry an ssh ControlMaster hint so an OTP/2FA login node prompts
only once; nothing is written and no connection is opened.";

pub(super) const EXPERIMENT_BUNDLE_HELP: &str = "\
Examples:
  hpc-compose experiment bundle 12345 --into ./bundles
  hpc-compose experiment bundle --tarball
  hpc-compose experiment bundle 12345 --include-artifacts --bundle metrics --format json
Writes hpc-compose-bundle-<job-id>/ with manifest.json, README.md, methods.md,
run metadata, checkpoint history, provenance when recorded, and artifact
metadata when present. Artifact payload is copied only with --include-artifacts.
The current compose file is not copied as submit-time source; use the recorded
effective config, submitted script, and provenance/source hash when present.";

pub(super) const NEW_HELP: &str = "\
Examples:
  hpc-compose init --template minimal-batch --name my-app --output compose.yaml
  hpc-compose new --list-templates
  hpc-compose new --describe-template minimal-batch
  hpc-compose new --template minimal-batch --name my-app --output compose.yaml
  hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml

`init` is a visible alias for `new`; `new` remains the canonical spelling in
docs and generated scripts.";

pub(super) const EXAMPLES_HELP: &str = "\
Examples:
  hpc-compose examples list
  hpc-compose examples list --tag mpi --format json
  hpc-compose examples search 'vllm worker'
  hpc-compose examples recommend 'multi-node training' --tag gpu
  hpc-compose examples recommend --format json
  hpc-compose examples coverage --format markdown";

pub(super) const DOCS_HELP: &str = "\
Examples:
  hpc-compose docs cache dir
  hpc-compose docs 'readiness never passes'
  hpc-compose docs x-slurm.cache_dir --format json

This command searches the bundled documentation only. It is static-safe and
works with --offline; it does not resolve settings, contact SSH, call Slurm, or
open a browser.";

pub(super) const FEEDBACK_HELP: &str = "\
Examples:
  hpc-compose feedback
  hpc-compose feedback --kind bug
  hpc-compose feedback --kind feature --format json

This command prints a local report and GitHub issue link only. It never sends
telemetry, opens a browser, contacts GitHub, or performs a version ping.";

pub(super) const EVOLVE_HELP: &str = "\
Examples:
  hpc-compose evolve
  hpc-compose evolve --list-lessons
  hpc-compose evolve --describe-lesson progressive-complexity
  hpc-compose evolve --output compose.yaml --name my-app --cache-dir '<shared-cache-dir>'
  hpc-compose evolve --yes --until readiness --format json";

pub(super) const CACHE_HELP: &str = "\
Examples:
  hpc-compose cache list
  hpc-compose cache inspect -f compose.yaml
  hpc-compose cache prune --age 7 --yes";

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
  hpc-compose --profile dev cache prune --age 14 --yes
  hpc-compose cache prune --all-unused -f compose.yaml --yes
  hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>' --yes
  hpc-compose cache prune --age 7 --yes --format json";

pub(super) const WORKSPACE_HELP: &str = "\
Examples:
  hpc-compose workspace status
  hpc-compose workspace allocate
  hpc-compose workspace extend --days 30
  hpc-compose workspace release --yes
Configure the workspace name in settings, e.g.:
  [profiles.dev.workspace]
  name = \"hpc-compose-cache\"
  duration_days = 30";

pub(super) const WORKSPACE_STATUS_HELP: &str = "\
Examples:
  hpc-compose workspace status
  hpc-compose workspace status --format json
  hpc-compose --profile dev workspace status";

pub(super) const WORKSPACE_ALLOCATE_HELP: &str = "\
Examples:
  hpc-compose workspace allocate
  hpc-compose workspace allocate --duration-days 60
  hpc-compose workspace allocate --format json
Idempotent: an existing workspace is reported as already allocated.";

pub(super) const WORKSPACE_EXTEND_HELP: &str = "\
Examples:
  hpc-compose workspace extend
  hpc-compose workspace extend --days 30
  hpc-compose workspace extend --format json";

pub(super) const WORKSPACE_RELEASE_HELP: &str = "\
Examples:
  hpc-compose workspace release
  hpc-compose workspace release --yes
Refuses to release while tracked jobs keep cache or runtime state under the
workspace; run `hpc-compose down --job-id <id>` or `hpc-compose clean` first.";

pub(super) const JOBS_HELP: &str = "\
Examples:
  hpc-compose jobs list
  hpc-compose jobs list --disk-usage
  hpc-compose jobs list --tag baseline --tag lr-bug
  hpc-compose jobs list --format json";

pub(super) const CLEAN_HELP: &str = "\
Examples:
  hpc-compose clean --age 7 --yes
  hpc-compose clean --all --dry-run
  hpc-compose clean --age 7 --deep --dry-run --disk-usage
  hpc-compose clean --age 7 --deep --yes
  hpc-compose clean --all --yes --format json";

pub(super) const CONTEXT_HELP: &str = "\
Examples:
  hpc-compose context
  hpc-compose context --format json
  hpc-compose --profile dev context";

pub(super) const SETUP_HELP: &str = "\
Examples:
  hpc-compose setup
  hpc-compose setup --profile-name dev --compose-file compose.yaml --default-profile dev --non-interactive
  hpc-compose setup --profile-name dev --cache-dir '<shared-cache-dir>' --default-profile dev --non-interactive
  hpc-compose setup --env 'CACHE_DIR=<shared-cache-dir>' --binary srun=/opt/slurm/bin/srun --non-interactive";

pub(super) const COMPLETIONS_HELP: &str = "\
Notes:
  Bash, Zsh, and Fish completions include live values for local project state, such as compose services, resource profiles, cluster partitions/QOS, tracked job ids, sweep ids, tags, and artifact bundles. PowerShell and Elvish use static command/flag completions.

Examples:
  hpc-compose completions bash
  hpc-compose completions zsh > ~/.zfunc/_hpc-compose
  hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish";

const TOP_LEVEL_EXAMPLES: &[&str] = &[
    "hpc-compose up -f compose.yaml",
    "hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4",
    "hpc-compose plan -f compose.yaml",
    "hpc-compose evolve --output compose.yaml",
    "hpc-compose debug -f compose.yaml --preflight",
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

const LINT_EXAMPLES: &[&str] = &[
    "hpc-compose lint -f compose.yaml",
    "hpc-compose lint -f compose.yaml --allow-warnings",
    "hpc-compose lint -f compose.yaml --format json",
];

const RENDER_EXAMPLES: &[&str] = &[
    "hpc-compose render -f compose.yaml",
    "hpc-compose render -f compose.yaml --output job.sbatch",
    "hpc-compose render -f compose.yaml --format json",
];

const PREPARE_EXAMPLES: &[&str] = &[
    "hpc-compose prepare -f compose.yaml",
    "hpc-compose prepare -f compose.yaml --force-rebuild",
    "hpc-compose prepare -f compose.yaml --format json",
];

const PREFLIGHT_EXAMPLES: &[&str] = &[
    "hpc-compose preflight -f compose.yaml",
    "hpc-compose preflight -f compose.yaml --strict",
    "hpc-compose preflight -f compose.yaml --fs-probes",
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

const PLAN_EXAMPLES: &[&str] = &[
    "hpc-compose plan -f compose.yaml",
    "hpc-compose plan --verbose -f compose.yaml",
    "hpc-compose plan --explain -f compose.yaml",
    "hpc-compose plan --show-script -f compose.yaml",
    "hpc-compose plan -f compose.yaml --format json",
];

const UP_EXAMPLES: &[&str] = &[
    "hpc-compose up -f compose.yaml",
    "hpc-compose up --detach -f compose.yaml",
    "hpc-compose up --detach --format json -f compose.yaml",
    "hpc-compose up --detach --format json --print-endpoints -f compose.yaml",
    "hpc-compose up --dry-run -f compose.yaml",
    "hpc-compose up --watch-mode line -f compose.yaml",
    "hpc-compose up --hold-on-exit always -f compose.yaml",
];

const WHEN_EXAMPLES: &[&str] = &[
    "hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4",
    "hpc-compose when -f compose.yaml --after-job 12345",
    "hpc-compose when -f compose.yaml --between 22:00-06:00",
    "hpc-compose when --detach --format json -f compose.yaml --partition gpu8 --free-nodes 4",
];

const SWEEP_EXAMPLES: &[&str] = &[
    "hpc-compose sweep submit -f train.yaml --dry-run",
    "hpc-compose sweep submit -f train.yaml",
    "hpc-compose sweep status -f train.yaml",
    "hpc-compose sweep list -f train.yaml",
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

const SCORE_EXAMPLES: &[&str] = &[
    "hpc-compose score 12345",
    "hpc-compose score -f compose.yaml",
    "hpc-compose score 12345 --pue 1.3 --gpu-tdp-w 350 --format json",
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
    "hpc-compose watch -f compose.yaml --watch-mode line",
    "hpc-compose watch -f compose.yaml --hold-on-exit always",
];

const REPLAY_EXAMPLES: &[&str] = &[
    "hpc-compose replay -f compose.yaml",
    "hpc-compose replay -f compose.yaml --speed 10",
    "hpc-compose replay -f compose.yaml --job-id 12345 --service app",
    "hpc-compose replay -f compose.yaml --format json",
];

const CHECKPOINTS_EXAMPLES: &[&str] = &[
    "hpc-compose checkpoints -f compose.yaml",
    "hpc-compose checkpoints --job-id 12345",
    "hpc-compose checkpoints --format json",
];

const DEBUG_EXAMPLES: &[&str] = &[
    "hpc-compose debug -f compose.yaml",
    "hpc-compose debug -f compose.yaml --preflight",
    "hpc-compose debug -f compose.yaml --service app --lines 200",
    "hpc-compose debug -f compose.yaml --format json",
];

const CANCEL_EXAMPLES: &[&str] = &[
    "hpc-compose cancel -f compose.yaml",
    "hpc-compose cancel -f compose.yaml --job-id 12345",
    "hpc-compose cancel -f compose.yaml --yes",
];

const DOWN_EXAMPLES: &[&str] = &[
    "hpc-compose down -f compose.yaml",
    "hpc-compose down --job-id 12345",
    "hpc-compose down --job-id 12345 --purge-cache --yes",
];

const RUN_EXAMPLES: &[&str] = &[
    "hpc-compose run -f compose.yaml app -- python -m pytest",
    "hpc-compose run -f compose.yaml app -- bash",
    "hpc-compose run --image docker://python:3.12 -- python -V",
];

const SHELL_EXAMPLES: &[&str] = &[
    "hpc-compose shell --image docker://ubuntu:24.04",
    "hpc-compose shell --image docker://nvidia/cuda:12.4.1-base-ubuntu22.04 --gpus 1",
    "hpc-compose shell --image docker://python:3.12 --resources cpu-small",
];

const NEW_EXAMPLES: &[&str] = &[
    "hpc-compose new --list-templates",
    "hpc-compose new --describe-template minimal-batch",
    "hpc-compose new --template minimal-batch --name my-app --output compose.yaml",
    "hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml",
];

const EVOLVE_EXAMPLES: &[&str] = &[
    "hpc-compose evolve",
    "hpc-compose evolve --list-lessons",
    "hpc-compose evolve --describe-lesson progressive-complexity",
    "hpc-compose evolve --output compose.yaml --name my-app --cache-dir '<shared-cache-dir>'",
    "hpc-compose evolve --yes --until readiness --format json",
];

const CACHE_EXAMPLES: &[&str] = &[
    "hpc-compose cache list",
    "hpc-compose cache inspect -f compose.yaml",
    "hpc-compose cache prune --age 7 --yes",
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
    "hpc-compose --profile dev cache prune --age 14 --yes",
    "hpc-compose cache prune --all-unused -f compose.yaml --yes",
    "hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>' --yes",
    "hpc-compose cache prune --age 7 --yes --format json",
];

const WORKSPACE_EXAMPLES: &[&str] = &[
    "hpc-compose workspace status",
    "hpc-compose workspace allocate",
    "hpc-compose workspace extend --days 30",
    "hpc-compose workspace release --yes",
];

const JOBS_EXAMPLES: &[&str] = &[
    "hpc-compose jobs list",
    "hpc-compose jobs list --disk-usage",
    "hpc-compose jobs list --format json",
];

const CLEAN_EXAMPLES: &[&str] = &[
    "hpc-compose clean --age 7 --yes",
    "hpc-compose clean --all --dry-run",
    "hpc-compose clean --all --yes --format json",
];

const CONTEXT_EXAMPLES: &[&str] = &[
    "hpc-compose context",
    "hpc-compose context --format json",
    "hpc-compose --profile dev context",
];

const SETUP_EXAMPLES: &[&str] = &[
    "hpc-compose setup",
    "hpc-compose setup --profile-name dev --compose-file compose.yaml --default-profile dev --non-interactive",
    "hpc-compose setup --profile-name dev --cache-dir '<shared-cache-dir>' --default-profile dev --non-interactive",
    "hpc-compose setup --env 'CACHE_DIR=<shared-cache-dir>' --binary srun=/opt/slurm/bin/srun --non-interactive",
];

const COMPLETIONS_EXAMPLES: &[&str] = &[
    "hpc-compose completions bash",
    "hpc-compose completions zsh > ~/.zfunc/_hpc-compose",
    "hpc-compose completions fish > ~/.config/fish/completions/hpc-compose.fish",
];

const DOCTOR_EXAMPLES: &[&str] = &[
    "hpc-compose doctor",
    "hpc-compose doctor cluster-report",
    "hpc-compose doctor readiness -f compose.yaml --service api",
    "hpc-compose doctor readiness -f compose.yaml --service api --run",
    "hpc-compose doctor mpi-smoke -f compose.yaml --service trainer",
    "hpc-compose doctor fabric-smoke -f compose.yaml --service trainer --checks auto",
];

const EXAMPLES_EXAMPLES: &[&str] = &[
    "hpc-compose examples list",
    "hpc-compose examples list --tag mpi --format json",
    "hpc-compose examples search 'vllm worker'",
    "hpc-compose examples recommend 'multi-node training' --tag gpu",
    "hpc-compose examples recommend --format json",
    "hpc-compose examples coverage --format markdown",
];

const FEEDBACK_EXAMPLES: &[&str] = &[
    "hpc-compose feedback",
    "hpc-compose feedback --kind bug",
    "hpc-compose feedback --kind feature --format json",
];

#[must_use]
pub fn examples_for_path(path: &[&str]) -> &'static [&'static str] {
    match path {
        [] => TOP_LEVEL_EXAMPLES,
        ["validate"] => VALIDATE_EXAMPLES,
        ["lint"] => LINT_EXAMPLES,
        ["render"] => RENDER_EXAMPLES,
        ["prepare"] => PREPARE_EXAMPLES,
        ["preflight"] => PREFLIGHT_EXAMPLES,
        ["inspect"] => INSPECT_EXAMPLES,
        ["config"] => CONFIG_EXAMPLES,
        ["schema"] => SCHEMA_EXAMPLES,
        ["doctor"] => DOCTOR_EXAMPLES,
        ["doctor", "cluster-report"] => DOCTOR_EXAMPLES,
        ["doctor", "mpi-smoke"] => DOCTOR_EXAMPLES,
        ["doctor", "fabric-smoke"] => DOCTOR_EXAMPLES,
        ["doctor", "readiness"] => DOCTOR_EXAMPLES,
        ["examples"] => EXAMPLES_EXAMPLES,
        ["examples", "list"] => EXAMPLES_EXAMPLES,
        ["examples", "search"] => EXAMPLES_EXAMPLES,
        ["examples", "recommend"] => EXAMPLES_EXAMPLES,
        ["examples", "coverage"] => EXAMPLES_EXAMPLES,
        ["feedback"] => FEEDBACK_EXAMPLES,
        ["plan"] => PLAN_EXAMPLES,
        ["up"] => UP_EXAMPLES,
        ["when"] => WHEN_EXAMPLES,
        ["sweep"] => SWEEP_EXAMPLES,
        ["sweep", "submit"] => SWEEP_EXAMPLES,
        ["sweep", "status"] => SWEEP_EXAMPLES,
        ["sweep", "list"] => SWEEP_EXAMPLES,
        ["sweep", "results"] => SWEEP_EXAMPLES,
        ["status"] => STATUS_EXAMPLES,
        ["stats"] => STATS_EXAMPLES,
        ["score"] => SCORE_EXAMPLES,
        ["artifacts"] => ARTIFACTS_EXAMPLES,
        ["logs"] => LOGS_EXAMPLES,
        ["ps"] => PS_EXAMPLES,
        ["watch"] => WATCH_EXAMPLES,
        ["replay"] => REPLAY_EXAMPLES,
        ["checkpoints"] => CHECKPOINTS_EXAMPLES,
        ["debug"] => DEBUG_EXAMPLES,
        ["cancel"] => CANCEL_EXAMPLES,
        ["down"] => DOWN_EXAMPLES,
        ["run"] => RUN_EXAMPLES,
        ["shell"] => SHELL_EXAMPLES,
        ["new"] => NEW_EXAMPLES,
        ["evolve"] => EVOLVE_EXAMPLES,
        ["cache"] => CACHE_EXAMPLES,
        ["cache", "list"] => CACHE_LIST_EXAMPLES,
        ["cache", "inspect"] => CACHE_INSPECT_EXAMPLES,
        ["cache", "prune"] => CACHE_PRUNE_EXAMPLES,
        ["workspace"] => WORKSPACE_EXAMPLES,
        ["workspace", "status"] => WORKSPACE_EXAMPLES,
        ["workspace", "allocate"] => WORKSPACE_EXAMPLES,
        ["workspace", "extend"] => WORKSPACE_EXAMPLES,
        ["workspace", "release"] => WORKSPACE_EXAMPLES,
        ["jobs"] => JOBS_EXAMPLES,
        ["jobs", "list"] => JOBS_EXAMPLES,
        ["clean"] => CLEAN_EXAMPLES,
        ["context"] => CONTEXT_EXAMPLES,
        ["setup"] => SETUP_EXAMPLES,
        ["completions"] => COMPLETIONS_EXAMPLES,
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::WORKFLOW_GROUPS;

    /// Real, non-hidden top-level command names from the actual clap tree.
    /// Excludes clap's auto-generated `help` subcommand, which is not a workflow
    /// command and is intentionally absent from the groups.
    ///
    /// Built on a large-stack thread because clap's command tree is deep enough
    /// to overflow the smaller default test-thread stack (same reason
    /// `manpages::render_manpages` does it).
    fn real_top_level_commands() -> BTreeSet<String> {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                crate::cli::build_cli_command()
                    .get_subcommands()
                    .filter(|cmd| !cmd.is_hide_set())
                    .map(|cmd| cmd.get_name().to_string())
                    .filter(|name| name != "help")
                    .collect()
            })
            .expect("failed to spawn cli builder thread")
            .join()
            .expect("cli builder thread panicked")
    }

    /// Flattened group membership, asserting no command is listed twice.
    fn grouped_commands() -> BTreeSet<String> {
        let mut seen = BTreeSet::new();
        for (group, _description, names) in WORKFLOW_GROUPS {
            for &name in *names {
                assert!(
                    seen.insert(name.to_string()),
                    "command '{name}' appears in more than one workflow group (second: {group})"
                );
            }
        }
        seen
    }

    /// Phase 1 drift guard: every non-hidden command is in exactly one group, and
    /// every grouped name is a real command. This is the test that would have
    /// caught `pull`/`checkpoints`/`experiment`/`reach` going missing from the
    /// hand-maintained help block.
    #[test]
    fn workflow_groups_match_every_command_exactly_once() {
        let real = real_top_level_commands();
        let grouped = grouped_commands();

        let missing: Vec<&String> = real.difference(&grouped).collect();
        assert!(
            missing.is_empty(),
            "commands missing from the --help workflow groups (drift): {missing:?}"
        );

        let stale: Vec<&String> = grouped.difference(&real).collect();
        assert!(
            stale.is_empty(),
            "workflow groups list names that are not real, non-hidden commands: {stale:?}"
        );
    }

    /// Phase 3 scope budget. Each top-level command is a discovery cost, so adding
    /// one is a deliberate UX decision. Prefer a subcommand or a *deprecating
    /// alias* over growing this number — the only safe merge candidate today is
    /// `lint` -> `validate --strict`, and only as a deprecating alias (each
    /// command otherwise has distinct UX). Raising the budget is allowed but must
    /// be a conscious edit in the same change that adds the command.
    #[test]
    fn top_level_command_count_stays_within_budget() {
        // Bumped 48 -> 49 for `explain` (spec-field <-> script-line provenance
        // queries): a static-safe inspection query over the rendered preview,
        // peer to `render`/`inspect`, that fits no existing command as a
        // subcommand (`plan --explain` already means planning hints).
        // Bumped 49 -> 50 for the `workspace` lifecycle group
        // (status/allocate/extend/release as subcommands, not new top-levels).
        // Bumped 50 -> 51 for `docs`, the roadmap's offline manual search
        // command. It is intentionally top-level because it is a global
        // discovery entrypoint, not tied to examples, specs, or tracked runs.
        // Bumped 51 -> 52 for `feedback`, an explicit no-telemetry community
        // signal surface with issue-template links.
        // Bumped 52 -> 53 for `lsp`, a diagnostics-only editor/agent authoring
        // surface that must be launched as a long-lived stdio server.
        const MAX_TOP_LEVEL_COMMANDS: usize = 53;
        let count = real_top_level_commands().len();
        assert!(
            count <= MAX_TOP_LEVEL_COMMANDS,
            "non-hidden top-level command count {count} exceeds budget \
             {MAX_TOP_LEVEL_COMMANDS}; consolidate via a subcommand or deprecating alias, \
             or bump the budget intentionally"
        );
    }

    /// The generated epilogue lists every group and surfaces `examples recommend`
    /// as the "which command?" answer (Phase 2 discoverability).
    #[test]
    fn top_level_help_lists_all_groups_and_the_recommend_entrypoint() {
        let help = super::top_level_help();
        for (label, _, _) in WORKFLOW_GROUPS {
            assert!(
                help.contains(&format!("{label}:")),
                "help missing group '{label}'"
            );
        }
        assert!(
            help.contains("examples recommend"),
            "top-level help should point undecided users at `examples recommend`"
        );
    }

    /// Phase 2 anti-drift: the structured `next_commands` hints printed after a
    /// run/read only ever point at real commands, so a rename can't leave a
    /// dangling "Next:" suggestion (the discoverability analogue of the workflow
    /// group drift guard above).
    #[test]
    fn next_step_hints_reference_only_real_commands() {
        let real = real_top_level_commands();
        let mut hints = crate::output::submit_next_commands(Some("123"), true);
        hints.extend(crate::output::inspect_next_commands(Some("123"), true));
        hints.extend(crate::output::validate_next_commands(None));
        hints.extend(crate::output::ready_to_run_next_commands(None));
        for hint in hints {
            // Each hint is "hpc-compose <command> ...".
            let command = hint
                .strip_prefix("hpc-compose ")
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap_or_default();
            assert!(
                real.contains(command),
                "next-step hint references unknown command '{command}' in: {hint}"
            );
        }
    }
}
