mod support;

use std::fs;

use serde_json::Value;
use support::*;

#[test]
fn inspect_json_preflight_json_and_init_cover_new_modes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_env_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let inspect_verbose = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect_verbose);
    let inspect_verbose_stdout = stdout_text(&inspect_verbose);
    assert!(inspect_verbose_stdout.contains("environment:"));
    assert!(inspect_verbose_stdout.contains("  - SECRET_TOKEN"));
    assert!(!inspect_verbose_stdout.contains("SECRET_TOKEN=super-secret"));
    assert!(inspect_verbose_stdout.contains(&format!(
        "{}:/workspace",
        tmpdir.path().join("app").display()
    )));
    assert!(inspect_verbose_stdout.contains("/hpc-compose/job"));
    assert!(inspect_verbose_stdout.contains("effective srun args:"));

    let inspect_json = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect_json);
    let inspect_json_stdout = stdout_text(&inspect_json);
    assert!(!inspect_json_stdout.contains("super-secret"));
    assert!(inspect_json_stdout.contains("<redacted>"));
    assert!(inspect_json_stdout.contains("hi-from-dotenv"));

    let preflight_json = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&preflight_json);
    let preflight_value: Value =
        serde_json::from_str(&stdout_text(&preflight_json)).expect("preflight json");
    assert!(
        preflight_value["summary"]["passed_checks"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(preflight_value["passed_checks"].is_array());

    for template in [
        "dev-python-app",
        "app-redis-worker",
        "llm-curl-workflow",
        "llm-curl-workflow-workdir",
        "llama-app",
        "llama-uv-worker",
        "multi-node-mpi",
        "multi-node-torchrun",
        "multi-node-deepspeed",
        "multi-node-accelerate",
        "multi-node-horovod",
        "multi-node-jax",
        "nccl-tests",
        "nextflow-bridge",
        "snakemake-bridge",
        "vllm-uv-worker",
        "eval-harness",
    ] {
        let output = tmpdir.path().join(format!("{template}.yaml"));
        let init = run_cli(
            tmpdir.path(),
            &[
                "new",
                "--template",
                template,
                "--name",
                "custom-init",
                "--cache-dir",
                "/tmp/custom-cache",
                "--output",
                output.to_str().expect("path"),
                "--force",
            ],
        );
        assert_success(&init);
        assert!(output.exists());
        let validate = run_cli(
            tmpdir.path(),
            &["validate", "-f", output.to_str().expect("path")],
        );
        assert_success(&validate);
        let rendered = fs::read_to_string(&output).expect("rendered template");
        assert!(rendered.contains("name: custom-init"));
        assert!(rendered.contains("job_name: custom-init"));
        assert!(rendered.contains("cache_dir: /tmp/custom-cache"));
        assert!(stdout_text(&init).contains("hpc-compose up -f"));
    }
}

#[test]
fn help_and_template_discovery_surface_guided_workflows() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let top_help = run_cli(tmpdir.path(), &["--help"]);
    assert_success(&top_help);
    let top_help_stdout = stdout_text(&top_help);
    assert!(top_help_stdout.contains("Start from an existing spec:"));
    assert!(top_help_stdout.contains("plan -f compose.yaml"));
    assert!(top_help_stdout.contains("up -f compose.yaml"));
    assert!(top_help_stdout.contains("Create or evolve a spec:"));
    assert!(top_help_stdout.contains("new --template minimal-batch"));
    assert!(top_help_stdout.contains("evolve --output compose.yaml"));
    assert!(top_help_stdout.contains("Run when cluster conditions are friendlier:"));
    assert!(top_help_stdout.contains("when -f compose.yaml --partition gpu8 --free-nodes 4"));
    assert!(top_help_stdout.contains("Debug failed run:"));
    assert!(top_help_stdout.contains("debug -f compose.yaml --preflight"));
    assert!(top_help_stdout.contains("Workflow groups:"));
    assert!(top_help_stdout.contains("Plan/Run: inspect statically, then submit or launch"));
    assert!(top_help_stdout.contains("plan, up, when, alloc, run, shell, germinate"));
    assert!(top_help_stdout.contains("Observe/Debug: monitor, inspect, and diagnose runs"));
    assert!(
        top_help_stdout.contains(
            "weather, doctor, debug, watch, replay, status, logs, ps, stats, score, diff"
        )
    );
    assert!(top_help_stdout.contains("--color <WHEN>"));
    assert!(top_help_stdout.contains("--quiet"));
    assert!(top_help_stdout.contains("--profile <NAME>"));
    assert!(top_help_stdout.contains("--settings-file <PATH>"));
    assert!(!top_help_stdout.contains("submit       "));
    assert!(
        !top_help_stdout.contains("config       Render the fully interpolated effective config")
    );
    assert!(!top_help_stdout.contains("plan         Validate and preview a static execution plan"));
    assert!(top_help_stdout.contains("Use `hpc-compose help <command>` for command details."));

    let new_help = run_cli(tmpdir.path(), &["new", "--help"]);
    assert_success(&new_help);
    let new_help_stdout = stdout_text(&new_help);
    assert!(new_help_stdout.contains("--list-templates"));
    assert!(new_help_stdout.contains("--describe-template <TEMPLATE>"));

    let evolve_help = run_cli(tmpdir.path(), &["evolve", "--help"]);
    assert_success(&evolve_help);
    let evolve_help_stdout = stdout_text(&evolve_help);
    assert!(evolve_help_stdout.contains("--list-lessons"));
    assert!(evolve_help_stdout.contains("--describe-lesson <LESSON>"));
    assert!(evolve_help_stdout.contains("--until <STEP>"));

    let init_help = run_cli(tmpdir.path(), &["init", "--help"]);
    assert_success(&init_help);
    assert!(stdout_text(&init_help).contains("--list-templates"));

    let cache_help = run_cli(tmpdir.path(), &["cache", "--help"]);
    assert_success(&cache_help);
    let cache_help_stdout = stdout_text(&cache_help);
    assert!(cache_help_stdout.contains("cache inspect -f compose.yaml"));
    assert!(cache_help_stdout.contains("list     List cached image artifacts"));
    assert!(cache_help_stdout.contains("inspect  Inspect cache reuse for the current plan"));
    assert!(cache_help_stdout.contains("prune    Prune cached image artifacts"));

    let jobs_help = run_cli(tmpdir.path(), &["jobs", "--help"]);
    assert_success(&jobs_help);
    let jobs_help_stdout = stdout_text(&jobs_help);
    assert!(jobs_help_stdout.contains("jobs list --format json"));
    assert!(jobs_help_stdout.contains("list  List tracked jobs discovered under the repo tree"));

    let submit_help = run_cli(tmpdir.path(), &["submit", "--help"]);
    assert_failure(&submit_help);
    assert!(stderr_text(&submit_help).contains("unrecognized subcommand 'submit'"));

    let plan_help = run_cli(tmpdir.path(), &["plan", "--help"]);
    assert_success(&plan_help);
    let plan_help_stdout = stdout_text(&plan_help);
    assert!(plan_help_stdout.contains("--show-script"));
    assert!(plan_help_stdout.contains("--format <FORMAT>"));
    assert!(plan_help_stdout.contains("without touching Slurm"));

    let up_help = run_cli(tmpdir.path(), &["up", "--help"]);
    assert_success(&up_help);
    let up_help_stdout = stdout_text(&up_help);
    assert!(up_help_stdout.contains("--detach"));
    assert!(up_help_stdout.contains("--watch-queue"));
    assert!(up_help_stdout.contains("--queue-warn-after <DURATION>"));
    assert!(up_help_stdout.contains("--watch-mode <MODE>"));
    assert!(!up_help_stdout.contains("--no-tui"));
    assert!(up_help_stdout.contains("Tool overrides"));

    let when_help = run_cli(tmpdir.path(), &["when", "--help"]);
    assert_success(&when_help);
    let when_help_stdout = stdout_text(&when_help);
    assert!(when_help_stdout.contains("--free-nodes <NODES>"));
    assert!(when_help_stdout.contains("--after-job <JOB_ID>"));
    assert!(when_help_stdout.contains("--between <HH:MM-HH:MM>"));
    assert!(when_help_stdout.contains("--poll-interval <DURATION>"));

    let debug_help = run_cli(tmpdir.path(), &["debug", "--help"]);
    assert_success(&debug_help);
    let debug_help_stdout = stdout_text(&debug_help);
    assert!(debug_help_stdout.contains("--preflight"));
    assert!(debug_help_stdout.contains("scheduler state"));

    let cli_reference = include_str!("../docs/src/cli-reference.md");
    assert!(cli_reference.contains("| `doctor cluster-report` |"));
    assert!(cli_reference.contains("hpc-compose doctor cluster-report"));
    let quickstart_demo = include_str!("../docs/src/quickstart-demo.cast");
    assert!(quickstart_demo.contains("hpc-compose plan -f examples/minimal-batch.yaml"));
    assert!(quickstart_demo.contains("hpc-compose plan --show-script"));
    assert!(!quickstart_demo.contains("hpc-compose validate"));
    assert!(!quickstart_demo.contains("up --dry-run --skip-prepare --no-preflight"));

    let preflight_help = run_cli(tmpdir.path(), &["preflight", "--help"]);
    assert_success(&preflight_help);
    assert!(stdout_text(&preflight_help).contains("Treat warnings as failures"));

    let list_templates = run_cli(tmpdir.path(), &["new", "--list-templates"]);
    assert_success(&list_templates);
    let list_stdout = stdout_text(&list_templates);
    assert!(list_stdout.contains("basics:"));
    assert!(list_stdout.contains("distributed:"));
    assert!(list_stdout.contains("minimal-batch"));
    assert!(list_stdout.contains("multi-node-mpi"));
    assert!(list_stdout.contains("multi-node-torchrun"));
    assert!(list_stdout.contains("multi-node-deepspeed"));
    assert!(list_stdout.contains("multi-node-accelerate"));
    assert!(list_stdout.contains("multi-node-horovod"));
    assert!(list_stdout.contains("multi-node-jax"));
    assert!(list_stdout.contains("nccl-tests"));
    assert!(list_stdout.contains("nextflow-bridge"));
    assert!(list_stdout.contains("snakemake-bridge"));

    let describe_template = run_cli(
        tmpdir.path(),
        &["new", "--describe-template", "multi-node-mpi"],
    );
    assert_success(&describe_template);
    let describe_stdout = stdout_text(&describe_template);
    assert!(describe_stdout.contains("template: multi-node-mpi"));
    assert!(describe_stdout.contains("allocation-wide"));
    assert!(describe_stdout.contains("cache dir: optional"));
    assert!(describe_stdout.contains("placeholder: <shared-cache-dir>"));
    assert!(describe_stdout.contains("hpc-compose new --template multi-node-mpi"));
    assert!(!describe_stdout.contains("--cache-dir '<shared-cache-dir>'"));

    let init_alias = run_cli(
        tmpdir.path(),
        &["init", "--describe-template", "multi-node-mpi"],
    );
    assert_success(&init_alias);
    assert!(stdout_text(&init_alias).contains("template: multi-node-mpi"));

    let list_templates_json = run_cli(
        tmpdir.path(),
        &["new", "--list-templates", "--format", "json"],
    );
    assert_success(&list_templates_json);
    let list_payload: Value =
        serde_json::from_str(&stdout_text(&list_templates_json)).expect("list json");
    assert_eq!(list_payload["cache_dir_required"], false);
    assert_eq!(list_payload["cache_dir_placeholder"], "<shared-cache-dir>");
    let templates = list_payload["templates"].as_array().expect("templates");
    let minimal = templates
        .iter()
        .find(|template| template["name"] == "minimal-batch")
        .expect("minimal-batch template");
    assert_eq!(minimal["category"], "basics");
    assert_eq!(
        templates.first().expect("first template")["name"],
        "minimal-batch"
    );

    let describe_template_json = run_cli(
        tmpdir.path(),
        &[
            "new",
            "--describe-template",
            "minimal-batch",
            "--format",
            "json",
        ],
    );
    assert_success(&describe_template_json);
    let describe_payload: Value =
        serde_json::from_str(&stdout_text(&describe_template_json)).expect("describe json");
    assert_eq!(describe_payload["cache_dir_required"], false);
    assert_eq!(
        describe_payload["cache_dir_placeholder"],
        "<shared-cache-dir>"
    );
    assert!(
        describe_payload["command"]
            .as_str()
            .unwrap_or_default()
            .contains(
                "hpc-compose new --template minimal-batch --name my-app --output compose.yaml"
            )
    );
    assert_eq!(describe_payload["template"]["category"], "basics");

    let new_non_tty = run_cli(tmpdir.path(), &["new"]);
    assert_success(&new_non_tty);
    let new_stdout = stdout_text(&new_non_tty);
    assert!(new_stdout.contains("Cache dir (optional)"));
    let generated = fs::read_to_string(tmpdir.path().join("compose.yaml")).expect("compose");
    assert!(!generated.contains("cache_dir:"));
}

#[test]
fn evolve_lists_describes_runs_and_validates_progressive_specs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let list = run_cli(tmpdir.path(), &["evolve", "--list-lessons"]);
    assert_success(&list);
    let list_stdout = stdout_text(&list);
    assert!(list_stdout.contains("progressive-complexity"));
    assert!(list_stdout.contains("5 steps"));

    let list_json = run_cli(
        tmpdir.path(),
        &["evolve", "--list-lessons", "--format", "json"],
    );
    assert_success(&list_json);
    let list_payload: Value = serde_json::from_str(&stdout_text(&list_json)).expect("list json");
    assert_eq!(list_payload["lessons"][0]["id"], "progressive-complexity");
    assert_eq!(list_payload["lessons"][0]["step_count"], 5);

    let describe = run_cli(
        tmpdir.path(),
        &["evolve", "--describe-lesson", "progressive-complexity"],
    );
    assert_success(&describe);
    let describe_stdout = stdout_text(&describe);
    assert!(describe_stdout.contains("lesson: progressive-complexity"));
    assert!(describe_stdout.contains("readiness"));
    assert!(describe_stdout.contains("multi-node-placement"));

    let describe_json = run_cli(
        tmpdir.path(),
        &[
            "evolve",
            "--describe-lesson",
            "progressive-complexity",
            "--format",
            "json",
        ],
    );
    assert_success(&describe_json);
    let describe_payload: Value =
        serde_json::from_str(&stdout_text(&describe_json)).expect("describe json");
    assert_eq!(describe_payload["id"], "progressive-complexity");
    let step_ids = describe_payload["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .map(|step| step["id"].as_str().expect("step id").to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        step_ids,
        vec![
            "minimal",
            "second-service",
            "readiness",
            "failure-policy",
            "multi-node-placement"
        ]
    );

    let interactive_json = run_cli(tmpdir.path(), &["evolve", "--format", "json"]);
    assert_failure(&interactive_json);
    assert!(
        stderr_text(&interactive_json).contains("--format json requires --yes"),
        "stderr was {}",
        stderr_text(&interactive_json)
    );

    let readiness_path = tmpdir.path().join("readiness.yaml");
    let readiness = run_cli(
        tmpdir.path(),
        &[
            "evolve",
            "--yes",
            "--until",
            "readiness",
            "--name",
            "custom-evolve",
            "--output",
            readiness_path.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&readiness);
    let readiness_payload: Value =
        serde_json::from_str(&stdout_text(&readiness)).expect("readiness json");
    assert_eq!(readiness_payload["final_step"], "readiness");
    assert_eq!(
        readiness_payload["accepted_steps"]
            .as_array()
            .expect("accepted")
            .len(),
        3
    );
    let readiness_yaml = fs::read_to_string(&readiness_path).expect("readiness yaml");
    assert!(readiness_yaml.contains("name: custom-evolve"));
    assert!(readiness_yaml.contains("condition: service_healthy"));
    assert!(!readiness_yaml.contains("cache_dir:"));

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", readiness_path.to_str().expect("path")],
    );
    assert_success(&validate);
    let plan = run_cli(
        tmpdir.path(),
        &["plan", "-f", readiness_path.to_str().expect("path")],
    );
    assert_success(&plan);

    let overwrite = run_cli(
        tmpdir.path(),
        &[
            "evolve",
            "--yes",
            "--until",
            "minimal",
            "--output",
            readiness_path.to_str().expect("path"),
        ],
    );
    assert_failure(&overwrite);
    assert!(stderr_text(&overwrite).contains("refusing to overwrite"));

    let forced = run_cli(
        tmpdir.path(),
        &[
            "evolve",
            "--yes",
            "--until",
            "minimal",
            "--cache-dir",
            "/tmp/evolve-cache",
            "--output",
            readiness_path.to_str().expect("path"),
            "--force",
            "--format",
            "json",
        ],
    );
    assert_success(&forced);
    let forced_yaml = fs::read_to_string(&readiness_path).expect("forced yaml");
    assert!(forced_yaml.contains("cache_dir: /tmp/evolve-cache"));
}

#[test]
fn evolve_interactive_accept_skip_and_quit_keeps_last_accepted_valid_spec() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output_path = tmpdir.path().join("interactive.yaml");

    let output = run_cli_with_stdin(
        tmpdir.path(),
        &["evolve", "--output", output_path.to_str().expect("path")],
        "\ns\nq\n",
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("Controls: Enter/y/a accept"));
    assert!(stdout.contains("skipped second-service"));
    assert!(stdout.contains("accepted 1 step(s); final step: minimal"));

    let yaml = fs::read_to_string(&output_path).expect("interactive yaml");
    assert!(yaml.contains("Hello from Slurm"));
    assert!(!yaml.contains("worker:"));

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", output_path.to_str().expect("path")],
    );
    assert_success(&validate);
    let plan = run_cli(
        tmpdir.path(),
        &["plan", "-f", output_path.to_str().expect("path")],
    );
    assert_success(&plan);
}

#[test]
fn doctor_fabric_smoke_renders_json_and_strips_real_workflow() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "fabric-smoke.yaml",
        &format!(
            r#"
name: fabric-smoke
runtime:
  backend: host
x-slurm:
  cache_dir: "{}"
  setup:
    - echo real setup should not run
  stage_in:
    - from: /shared/real-input
      to: /scratch/input
  artifacts:
    export_dir: /shared/real-artifacts
    paths:
      - /hpc-compose/job/logs/**
services:
  trainer:
    command: echo real trainer should not run
    x-slurm:
      ntasks: 2
      gpus_per_node: 2
      mpi:
        type: pmix
        profile: openmpi
        expected_ranks: 2
"#,
            cache_root.path().display()
        ),
    );
    let script = tmpdir.path().join("fabric.sbatch");
    let srun = tmpdir.path().join("srun");
    write_script(
        &srun,
        r#"#!/bin/bash
if [[ "${1:-}" == "--mpi=list" ]]; then
  echo "MPI plugin types: pmix pmi2"
  exit 0
fi
exit 0
"#,
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "--format",
            "json",
            "--srun-bin",
            srun.to_str().expect("path"),
            "fabric-smoke",
            "-f",
            compose.to_str().expect("path"),
            "--script-out",
            script.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("fabric json");
    assert_eq!(payload["service"], "trainer");
    assert_eq!(payload["selected_checks"], "mpi, nccl, ucx, ofi");
    assert!(payload["checks"].as_array().expect("checks").len() >= 4);
    let rendered = fs::read_to_string(script).expect("fabric script");
    assert!(rendered.contains("hpc-compose MPI/fabric smoke"));
    assert!(rendered.contains("hpc-compose NCCL smoke"));
    assert!(rendered.contains("hpc-compose UCX/IB smoke"));
    assert!(rendered.contains("hpc-compose OFI smoke"));
    assert!(rendered.contains("--mpi=pmix"));
    assert!(!rendered.contains("real setup should not run"));
    assert!(!rendered.contains("real trainer should not run"));
    assert!(!rendered.contains("/shared/real-input"));
    assert!(!rendered.contains("/shared/real-artifacts"));
}

#[test]
fn doctor_subcommands_inherit_parent_json_format() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cluster = run_cli(
        tmpdir.path(),
        &["doctor", "--format", "json", "cluster-report", "--out", "-"],
    );
    assert_success(&cluster);
    let payload: Value = serde_json::from_str(&stdout_text(&cluster)).expect("cluster json");
    assert_eq!(payload["wrote"], false);
    assert!(payload["profile"].is_object());
    assert!(payload["diagnostics"].is_object());
}

#[test]
fn doctor_fabric_smoke_submit_records_passed_and_skipped_checks() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "fabric-smoke-host.yaml",
        &format!(
            r#"
name: fabric-smoke-host
runtime:
  backend: host
x-slurm:
  cache_dir: "{}"
services:
  trainer:
    command: /bin/true
    x-slurm:
      ntasks: 2
      gpus_per_node: 1
      mpi:
        type: pmix
        expected_ranks: 2
"#,
            cache_root.path().display()
        ),
    );
    write_fake_scontrol(tmpdir.path());
    let srun = tmpdir.path().join("srun");
    write_script(
        &srun,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun"
  exit 0
fi
if [[ "${1:-}" == "--mpi=list" ]]; then
  echo "pmix pmi2"
  exit 0
fi
output_path=""
while [[ $# -gt 0 && "${1:-}" == --* ]]; do
  case "$1" in
    --output=*) output_path="${1#--output=}" ;;
  esac
  shift
done
export SLURM_NTASKS=2
export SLURM_PROCID=0
export SLURM_LOCALID=0
export SLURM_NODEID=0
PATH="$(dirname "$0"):/usr/bin:/bin"
if [[ -n "$output_path" ]]; then
  mkdir -p "$(dirname "$output_path")"
  "$@" >> "$output_path" 2>&1
else
  exec "$@"
fi
"#,
    );
    write_script(
        &tmpdir.path().join("all_reduce_perf"),
        "#!/bin/bash\necho all_reduce_perf ok\n",
    );
    let sbatch = tmpdir.path().join("sbatch");
    let bash = shell_quote(&test_bash_path().display().to_string());
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{@: -1}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
{} "$script_path"
echo "Submitted batch job 12345"
"#,
            tmpdir.path().display(),
            bash
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "fabric-smoke",
            "-f",
            compose.to_str().expect("path"),
            "--submit",
            "--timeout",
            "10s",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("fabric json");
    assert_eq!(payload["submitted"], true);
    let checks = payload["checks"].as_array().expect("checks");
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "mpi" && check["status"] == "passed")
    );
    assert!(
        checks
            .iter()
            .any(|check| check["name"] == "nccl" && check["status"] == "passed")
    );
    assert!(checks.iter().any(|check| check["name"] == "ucx"
        && matches!(check["status"].as_str(), Some("passed" | "skipped"))));
    let log = payload["result"]["service_log"]
        .as_str()
        .expect("service log");
    assert!(log.contains("hpc-compose MPI smoke rank=0 size=2 expected=2"));
    assert!(log.contains("all_reduce_perf ok"));
}

#[test]
fn doctor_fabric_smoke_explicit_nccl_fails_when_tool_is_missing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "fabric-smoke-fail.yaml",
        &format!(
            r#"
name: fabric-smoke-fail
runtime:
  backend: host
x-slurm:
  cache_dir: "{}"
services:
  trainer:
    command: /bin/true
    x-slurm:
      ntasks: 2
      mpi:
        type: pmix
        expected_ranks: 2
"#,
            cache_root.path().display()
        ),
    );
    write_fake_scontrol(tmpdir.path());
    let srun = tmpdir.path().join("srun");
    write_script(
        &srun,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun"
  exit 0
fi
if [[ "${1:-}" == "--mpi=list" ]]; then
  echo "pmix pmi2"
  exit 0
fi
output_path=""
while [[ $# -gt 0 && "${1:-}" == --* ]]; do
  case "$1" in
    --output=*) output_path="${1#--output=}" ;;
  esac
  shift
done
export SLURM_NTASKS=2
export SLURM_PROCID=0
PATH="$(dirname "$0"):/usr/bin:/bin"
if [[ -n "$output_path" ]]; then
  mkdir -p "$(dirname "$output_path")"
  "$@" >> "$output_path" 2>&1
else
  exec "$@"
fi
"#,
    );
    let sbatch = tmpdir.path().join("sbatch");
    let bash = shell_quote(&test_bash_path().display().to_string());
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{@: -1}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
{} "$script_path" || job_status=$?
echo "Submitted batch job 12345"
exit "${{job_status:-1}}"
"#,
            tmpdir.path().display(),
            bash
        ),
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "fabric-smoke",
            "-f",
            compose.to_str().expect("path"),
            "--checks",
            "nccl",
            "--submit",
            "--timeout",
            "5s",
            "--format",
            "json",
        ],
    );
    assert_failure(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("\"name\": \"nccl\""));
    assert!(stdout.contains("\"status\": \"failed\""));
    assert!(stdout.contains("all_reduce_perf not found"));
    assert!(stderr_text(&output).contains("fabric smoke probe failed"));
}

#[test]
fn init_interactive_uses_prompted_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = tmpdir.path().join("interactive-init.yaml");
    let init = run_cli_with_stdin(
        tmpdir.path(),
        &["new", "--output", output.to_str().expect("path"), "--force"],
        "2\ninteractive-app\n/tmp/interactive-cache\n",
    );
    assert_success(&init);
    let rendered = fs::read_to_string(&output).expect("rendered");
    assert!(rendered.contains("name: interactive-app"));
    assert!(rendered.contains("job_name: interactive-app"));
    assert!(rendered.contains("cache_dir: /tmp/interactive-cache"));
    let stdout = stdout_text(&init);
    assert!(stdout.contains("Choose a template:"));
    assert!(stdout.contains("hpc-compose up -f"));
}

#[test]
fn init_interactive_uses_cli_cache_dir_as_prompt_default() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = tmpdir.path().join("interactive-cli-cache.yaml");
    let init = run_cli_with_stdin(
        tmpdir.path(),
        &[
            "new",
            "--cache-dir",
            "/tmp/cli-cache",
            "--output",
            output.to_str().expect("path"),
            "--force",
        ],
        "2\ninteractive-app\n\n",
    );
    assert_success(&init);
    let rendered = fs::read_to_string(&output).expect("rendered");
    assert!(rendered.contains("name: interactive-app"));
    assert!(rendered.contains("job_name: interactive-app"));
    assert!(rendered.contains("cache_dir: /tmp/cli-cache"));
    assert!(stdout_text(&init).contains("Cache dir [/tmp/cli-cache]:"));
}

#[test]
fn completions_command_generates_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for shell in ["bash", "zsh", "fish"] {
        let output = run_cli(tmpdir.path(), &["completions", shell]);
        assert_success(&output);
        let out = stdout_text(&output);
        assert!(
            out.contains("hpc-compose"),
            "completions for {shell} should mention hpc-compose"
        );
        assert!(out.len() > 100, "completions should be non-trivial");
    }
}

#[test]
fn examples_commands_list_search_and_coverage() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let list = run_cli(tmpdir.path(), &["examples", "list", "--tag", "mpi"]);
    assert_success(&list);
    let list_text = stdout_text(&list);
    assert!(list_text.contains("multi-node-mpi"));
    assert!(list_text.contains("tags:"));

    let list_json = run_cli(
        tmpdir.path(),
        &["examples", "list", "--tag", "mpi", "--format", "json"],
    );
    assert_success(&list_json);
    let payload: serde_json::Value =
        serde_json::from_str(&stdout_text(&list_json)).expect("examples json");
    assert!(
        payload["examples"]
            .as_array()
            .expect("examples array")
            .iter()
            .any(|entry| entry["name"] == "multi-node-mpi")
    );

    let search = run_cli(tmpdir.path(), &["examples", "search", "vllm worker"]);
    assert_success(&search);
    let search_text = stdout_text(&search);
    assert!(search_text.contains("vllm-uv-worker"));
    assert!(!search_text.contains("mpi-hello"));

    let recommend = run_cli(
        tmpdir.path(),
        &[
            "examples",
            "recommend",
            "multi-node training",
            "--tag",
            "gpu",
        ],
    );
    assert_success(&recommend);
    let recommend_text = stdout_text(&recommend);
    assert!(recommend_text.contains("Safe authoring path only"));
    assert!(recommend_text.contains("multi-node-torchrun"));
    assert!(recommend_text.contains("Why:"));
    assert!(recommend_text.contains("Prerequisites to review:"));
    assert!(recommend_text.contains("hpc-compose plan -f compose.yaml"));
    assert!(!recommend_text.contains("hpc-compose up"));

    let recommend_json = run_cli(
        tmpdir.path(),
        &[
            "examples",
            "recommend",
            "vllm worker",
            "--limit",
            "1",
            "--format",
            "json",
        ],
    );
    assert_success(&recommend_json);
    let recommend_payload: Value =
        serde_json::from_str(&stdout_text(&recommend_json)).expect("recommend json");
    assert_eq!(
        recommend_payload["recommendations"][0]["example"]["name"],
        "vllm-uv-worker"
    );
    assert!(
        recommend_payload["recommendations"][0]["reasons"]
            .as_array()
            .expect("reasons array")
            .iter()
            .any(|reason| reason.as_str().is_some_and(|value| value.contains("vllm")))
    );
    assert!(
        recommend_payload["recommendations"][0]["prerequisites"]
            .as_array()
            .expect("prerequisites array")
            .iter()
            .any(|prerequisite| prerequisite
                .as_str()
                .is_some_and(|value| value.contains("GPU")))
    );
    assert!(
        recommend_payload["recommendations"][0]["next_commands"]
            .as_array()
            .expect("next commands array")
            .iter()
            .all(|command| command
                .as_str()
                .is_some_and(|value| !value.contains("hpc-compose up")))
    );

    let invalid_limit = run_cli(tmpdir.path(), &["examples", "recommend", "--limit", "0"]);
    assert_failure(&invalid_limit);
    assert!(stderr_text(&invalid_limit).contains("invalid value"));

    let coverage = run_cli(
        tmpdir.path(),
        &["examples", "coverage", "--format", "markdown"],
    );
    assert_success(&coverage);
    let coverage_text = stdout_text(&coverage);
    assert!(coverage_text.contains("| Example | Availability | Tags |"));
    assert!(coverage_text.contains("minimal-batch.yaml"));
}

#[test]
fn doctor_mpi_smoke_renders_and_requires_explicit_submit() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "mpi-smoke.yaml",
        &format!(
            r#"
name: mpi-smoke
x-slurm:
  cache_dir: "{}"
  setup:
    - echo real setup should not run
  submit_args:
    - --comment=real-smoke-side-effect
  scratch:
    scope: shared
    base: /tmp/real-smoke-scratch
    mount: /scratch
  stage_in:
    - from: /shared/real-input
      to: /scratch/input
  stage_out:
    - from: /scratch/output
      to: /shared/real-output
      when: always
      mode: copy
  burst_buffer:
    directives:
      - '#BB create_persistent name=real capacity=1G'
  artifacts:
    export_dir: /shared/real-artifacts
    paths:
      - /hpc-compose/job/logs/**
  resume:
    path: /shared/real-resume
services:
  mpi:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      ntasks: 2
      mpi:
        type: pmix_v4
        profile: openmpi
        implementation: openmpi
        expected_ranks: 2
        host_mpi:
          bind_paths:
            - /opt/site/openmpi:/opt/site/openmpi:ro
          env:
            MPI_HOME: /opt/site/openmpi
"#,
            cache_root.path().display()
        ),
    );
    let script = tmpdir.path().join("smoke.sbatch");
    let srun = tmpdir.path().join("srun");
    write_script(
        &srun,
        r#"#!/bin/bash
if [[ "${1:-}" == "--mpi=list" ]]; then
  echo "MPI plugin types: pmix pmix_v4 pmi2"
  exit 0
fi
exit 0
"#,
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "--srun-bin",
            srun.to_str().expect("path"),
            "mpi-smoke",
            "-f",
            compose.to_str().expect("path"),
            "--script-out",
            script.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("MPI smoke service: mpi"));
    assert!(stdout.contains("requested MPI type: pmix_v4"));
    assert!(stdout.contains("MPI profile: openmpi"));
    assert!(stdout.contains("MPI implementation: openmpi"));
    assert!(stdout.contains("advertised MPI types: pmi2, pmix, pmix_v4"));
    assert!(stdout.contains("bind: /opt/site/openmpi:/opt/site/openmpi:ro"));
    assert!(stdout.contains("env: MPI_HOME=/opt/site/openmpi"));
    assert!(stdout.contains("rendered srun: srun"));
    assert!(stdout.contains("submit: skipped"));
    let rendered = fs::read_to_string(script).expect("smoke script");
    assert!(rendered.contains("--mpi=pmix_v4"));
    assert!(rendered.contains("hpc-compose MPI smoke"));
    assert!(rendered.contains("HPC_COMPOSE_MPI_PROFILE=openmpi"));
    assert!(rendered.contains("mpi4py allreduce smoke"));
    assert!(rendered.contains("rank_variables SLURM_PROCID="));
    assert!(rendered.contains("expected_ranks=2"));
    assert!(!rendered.contains("real setup should not run"));
    assert!(!rendered.contains("real-smoke-side-effect"));
    assert!(!rendered.contains("/tmp/real-smoke-scratch"));
    assert!(!rendered.contains("/shared/real-input"));
    assert!(!rendered.contains("/shared/real-output"));
    assert!(!rendered.contains("#BB create_persistent name=real capacity=1G"));
    assert!(!rendered.contains("/shared/real-artifacts"));
    assert!(!rendered.contains("/shared/real-resume"));
}

#[test]
fn doctor_mpi_smoke_submit_runs_fake_slurm_probe() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "mpi-smoke-host.yaml",
        &format!(
            r#"
name: mpi-smoke-host
runtime:
  backend: host
x-slurm:
  cache_dir: "{}"
services:
  mpi:
    command: /bin/true
    x-slurm:
      ntasks: 2
      mpi:
        type: pmix
        expected_ranks: 2
"#,
            cache_root.path().display()
        ),
    );
    write_fake_scontrol(tmpdir.path());
    let srun = tmpdir.path().join("srun");
    write_script(
        &srun,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun"
  exit 0
fi
if [[ "${1:-}" == "--mpi=list" ]]; then
  echo "pmix pmi2"
  exit 0
fi
while [[ $# -gt 0 && "${1:-}" == --* ]]; do
  shift
done
export SLURM_NTASKS=2
export SLURM_PROCID=0
exec "$@"
"#,
    );
    let sbatch = tmpdir.path().join("sbatch");
    let bash = shell_quote(&test_bash_path().display().to_string());
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{@: -1}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
{} "$script_path"
echo "Submitted batch job 12345"
"#,
            tmpdir.path().display(),
            bash
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "mpi-smoke",
            "-f",
            compose.to_str().expect("path"),
            "--submit",
            "--timeout",
            "5s",
        ],
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("submit: passed"));
    assert!(stdout.contains("hpc-compose MPI smoke rank=0 size=2 expected=2"));
}

#[test]
fn new_and_setup_commands_support_json_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let scaffold_path = tmpdir.path().join("scaffold.json.yaml");
    let new_output = run_cli(
        tmpdir.path(),
        &[
            "new",
            "--template",
            "minimal-batch",
            "--name",
            "json-app",
            "--cache-dir",
            "/tmp/json-cache",
            "--output",
            scaffold_path.to_str().expect("path"),
            "--force",
            "--format",
            "json",
        ],
    );
    assert_success(&new_output);
    let scaffold: Value = serde_json::from_str(&stdout_text(&new_output)).expect("new json");
    assert_eq!(scaffold["template_name"], "minimal-batch");
    assert_eq!(scaffold["app_name"], "json-app");
    assert_eq!(scaffold["cache_dir"], "/tmp/json-cache");
    assert!(
        scaffold["output_path"]
            .as_str()
            .unwrap_or_default()
            .ends_with("scaffold.json.yaml")
    );
    assert!(scaffold_path.exists());

    let setup_output = run_cli(
        tmpdir.path(),
        &[
            "setup",
            "--profile-name",
            "dev",
            "--compose-file",
            "compose.yaml",
            "--env-file",
            ".env",
            "--env",
            "CACHE_DIR=/shared/cache",
            "--binary",
            "srun=/opt/slurm/bin/srun",
            "--cache-dir",
            "/shared/setup-cache",
            "--default-profile",
            "dev",
            "--non-interactive",
            "--format",
            "json",
        ],
    );
    assert_success(&setup_output);
    let setup: Value = serde_json::from_str(&stdout_text(&setup_output)).expect("setup json");
    assert_eq!(setup["profile"], "dev");
    assert_eq!(setup["default_profile"], "dev");
    assert_eq!(setup["compose_file"], "compose.yaml");
    assert_eq!(setup["env_files"][0], ".env");
    assert_eq!(setup["env"]["CACHE_DIR"], "/shared/cache");
    assert_eq!(setup["binaries"]["srun"], "/opt/slurm/bin/srun");
    assert_eq!(setup["cache_dir"], "/shared/setup-cache");
}

#[test]
fn setup_persists_login_host_and_login_user_to_settings() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let setup_output = run_cli(
        tmpdir.path(),
        &[
            "setup",
            "--profile-name",
            "haicore",
            "--compose-file",
            "compose.yaml",
            "--cache-dir",
            "/shared/setup-cache",
            "--login-host",
            "haicore.scc.kit.edu",
            "--login-user",
            "vy3326",
            "--non-interactive",
            "--format",
            "json",
        ],
    );
    assert_success(&setup_output);

    // The SSH login user/host persist to the profile so `up --remote` resolves
    // them instead of falling back to the local laptop username.
    let settings = fs::read_to_string(tmpdir.path().join(".hpc-compose/settings.toml"))
        .expect("settings.toml written");
    assert!(
        settings.contains("login_user = \"vy3326\""),
        "settings should persist login_user; got:\n{settings}"
    );
    assert!(
        settings.contains("login_host = \"haicore.scc.kit.edu\""),
        "settings should persist login_host; got:\n{settings}"
    );
}

#[test]
fn new_allows_omitted_cache_dir_when_writing_a_template() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output_path = tmpdir.path().join("missing-cache.yaml");
    let output = run_cli(
        tmpdir.path(),
        &[
            "new",
            "--template",
            "minimal-batch",
            "--name",
            "missing-cache",
            "--output",
            output_path.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let rendered = fs::read_to_string(output_path).expect("rendered");
    assert!(rendered.contains("name: missing-cache"));
    assert!(!rendered.contains("cache_dir:"));
}

#[test]
fn setup_interactive_accepts_prompted_env_files_vars_and_binaries() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let setup = run_cli_with_stdin(
        tmpdir.path(),
        &["setup"],
        "research\nstack.yaml\n.env,.env.local\nA=1,B=two\nenroot=/usr/local/bin/enroot,sbatch=/usr/local/bin/sbatch\n/shared/cache\nresearch\n",
    );
    assert_success(&setup);
    let stdout = stdout_text(&setup);
    assert!(stdout.contains("Profile name [dev]:"));
    assert!(stdout.contains("Compose file [compose.yaml]:"));
    assert!(stdout.contains("Profile env files (comma-separated) []:"));
    assert!(stdout.contains("Profile env vars KEY=VALUE (comma-separated) []:"));
    assert!(stdout.contains("Profile binaries NAME=PATH (comma-separated) []:"));
    assert!(stdout.contains("Cache dir []:"));
    assert!(stdout.contains("Default profile [research]:"));

    let settings_path = tmpdir.path().join(".hpc-compose/settings.toml");
    let settings = fs::read_to_string(&settings_path).expect("settings written");
    assert!(settings.contains("default_profile = \"research\""));
    assert!(settings.contains("compose_file = \"stack.yaml\""));
    assert!(settings.contains(".env"));
    assert!(settings.contains(".env.local"));
    assert!(settings.contains("A = \"1\""));
    assert!(settings.contains("B = \"two\""));
    assert!(settings.contains("enroot = \"/usr/local/bin/enroot\""));
    assert!(settings.contains("sbatch = \"/usr/local/bin/sbatch\""));
    assert!(settings.contains("dir = \"/shared/cache\""));
}
