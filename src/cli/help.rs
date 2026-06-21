pub(super) const FILE_ARG_HELP: &str = "Compose specification file to read; if omitted, use the active context compose file or fall back to compose.yaml";

pub(super) const TOP_LEVEL_HELP: &str = "\
Start from an existing spec:
  hpc-compose plan -f compose.yaml
  hpc-compose up -f compose.yaml

Create or evolve a spec:
  hpc-compose new --template minimal-batch --name my-app --output compose.yaml
  hpc-compose evolve --output compose.yaml

Run when cluster conditions are friendlier:
  hpc-compose when -f compose.yaml --partition gpu8 --free-nodes 4

Debug failed run:
  hpc-compose debug -f compose.yaml --preflight

Workflow groups:
  Start:          new, evolve, setup, context
  Plan/Run:       plan, up, when, alloc, run, shell, germinate
  Develop/Test:   test, dev, tmux, notebook
  Observe/Debug:  weather, doctor, debug, watch, replay, status, logs, ps, stats, score, diff, artifacts, sweep
  Maintain:       cache, jobs, clean, down, cancel, rendezvous
  Advanced:       examples, validate, lint, inspect, config, render, prepare, preflight, schema, completions

Use `hpc-compose help <command>` for command details.";

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
  hpc-compose up --hold-on-exit always -f compose.yaml";

pub(super) const TEST_HELP: &str = "\
Examples:
  hpc-compose test --local -f compose.yaml
  hpc-compose test --submit --time 00:01:00 -f compose.yaml
  hpc-compose test --submit --timeout 180s --format json -f compose.yaml

Smoke tests are finite: every service must start, pass configured readiness, and complete successfully.";

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
  hpc-compose sweep submit -f train.yaml --format json";

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
  hpc-compose status -f compose.yaml --format json";

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
  hpc-compose diff --across sweep-1700000000-1234 --matrix-format csv";

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
Aggregate a single tracked run into one read-only object.";

pub(super) const EXPERIMENT_SHOW_HELP: &str = "\
Examples:
  hpc-compose experiment show 12345
  hpc-compose experiment show -f compose.yaml
  hpc-compose experiment show 12345 --format json
Read-only: status + score + artifacts + provenance in one object. The printed
next_commands carry an ssh ControlMaster hint so an OTP/2FA login node prompts
only once; nothing is written and no connection is opened.";

pub(super) const NEW_HELP: &str = "\
Examples:
  hpc-compose new --list-templates
  hpc-compose new --describe-template minimal-batch
  hpc-compose new --template minimal-batch --name my-app --output compose.yaml
  hpc-compose new --template minimal-batch --name my-app --cache-dir '<shared-cache-dir>' --output compose.yaml";

pub(super) const EXAMPLES_HELP: &str = "\
Examples:
  hpc-compose examples list
  hpc-compose examples list --tag mpi --format json
  hpc-compose examples search 'vllm worker'
  hpc-compose examples recommend 'multi-node training' --tag gpu
  hpc-compose examples recommend --format json
  hpc-compose examples coverage --format markdown";

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

pub(super) const JOBS_HELP: &str = "\
Examples:
  hpc-compose jobs list
  hpc-compose jobs list --disk-usage
  hpc-compose jobs list --format json";

pub(super) const CLEAN_HELP: &str = "\
Examples:
  hpc-compose clean --age 7 --yes
  hpc-compose clean --all --dry-run
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
        ["jobs"] => JOBS_EXAMPLES,
        ["jobs", "list"] => JOBS_EXAMPLES,
        ["clean"] => CLEAN_EXAMPLES,
        ["context"] => CONTEXT_EXAMPLES,
        ["setup"] => SETUP_EXAMPLES,
        ["completions"] => COMPLETIONS_EXAMPLES,
        _ => &[],
    }
}
