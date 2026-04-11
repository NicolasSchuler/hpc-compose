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
        &["inspect", "-f", compose.to_str().expect("path"), "--json"],
    );
    assert_success(&inspect_json);
    let inspect_json_stdout = stdout_text(&inspect_json);
    assert!(inspect_json_stdout.contains("super-secret"));
    assert!(inspect_json_stdout.contains("hi-from-dotenv"));

    let preflight_json = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--json",
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
        "vllm-uv-worker",
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
    assert!(top_help_stdout.contains("Normal run:"));
    assert!(top_help_stdout.contains("up -f compose.yaml"));
    assert!(top_help_stdout.contains("Debugging flow:"));
    assert!(top_help_stdout.contains("Start a new spec:"));
    assert!(
        top_help_stdout.contains("config       Render the fully interpolated effective config")
    );
    assert!(top_help_stdout.contains("up           Submit, watch, and stream logs in one command"));
    assert!(top_help_stdout.contains("logs         Print tracked service logs"));
    assert!(top_help_stdout.contains("ps           Show tracked per-service runtime state"));
    assert!(top_help_stdout.contains("watch        Watch a tracked job in a live terminal UI"));
    assert!(top_help_stdout.contains("cancel       Cancel a tracked Slurm job"));
    assert!(top_help_stdout.contains("down         Cancel a tracked job and clean tracked state"));
    assert!(
        top_help_stdout.contains("run          Run a one-off command in one service environment")
    );
    assert!(
        top_help_stdout
            .contains("new          Write a starter compose file from a built-in template")
    );
    assert!(top_help_stdout.contains("jobs         List tracked jobs under the current repo tree"));
    assert!(top_help_stdout.contains("clean        Remove old tracked job directories"));
    assert!(top_help_stdout.contains("completions  Generate shell completions"));

    let new_help = run_cli(tmpdir.path(), &["new", "--help"]);
    assert_success(&new_help);
    let new_help_stdout = stdout_text(&new_help);
    assert!(new_help_stdout.contains("--list-templates"));
    assert!(new_help_stdout.contains("--describe-template <TEMPLATE>"));

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
    assert_success(&submit_help);
    let submit_help_stdout = stdout_text(&submit_help);
    assert!(
        submit_help_stdout
            .contains("Poll tracked state and stream logs after submission or local launch")
    );
    assert!(
        submit_help_stdout.contains("Run preflight, prepare, and render without calling sbatch")
    );
    assert!(submit_help_stdout.contains("--local"));
    assert!(submit_help_stdout.contains("active context compose file"));

    let preflight_help = run_cli(tmpdir.path(), &["preflight", "--help"]);
    assert_success(&preflight_help);
    assert!(stdout_text(&preflight_help).contains("Treat warnings as failures"));

    let list_templates = run_cli(tmpdir.path(), &["new", "--list-templates"]);
    assert_success(&list_templates);
    let list_stdout = stdout_text(&list_templates);
    assert!(list_stdout.contains("minimal-batch"));
    assert!(list_stdout.contains("multi-node-mpi"));
    assert!(list_stdout.contains("multi-node-torchrun"));

    let describe_template = run_cli(
        tmpdir.path(),
        &["new", "--describe-template", "multi-node-mpi"],
    );
    assert_success(&describe_template);
    let describe_stdout = stdout_text(&describe_template);
    assert!(describe_stdout.contains("template: multi-node-mpi"));
    assert!(describe_stdout.contains("allocation-wide"));
    assert!(describe_stdout.contains("cache dir: required"));
    assert!(describe_stdout.contains("placeholder: <shared-cache-dir>"));
    assert!(describe_stdout.contains("hpc-compose new --template multi-node-mpi"));
    assert!(describe_stdout.contains("--cache-dir '<shared-cache-dir>'"));

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
    assert_eq!(list_payload["cache_dir_required"], true);
    assert_eq!(list_payload["cache_dir_placeholder"], "<shared-cache-dir>");
    assert!(list_payload["default_cache_dir"].is_null());

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
    assert_eq!(describe_payload["cache_dir_required"], true);
    assert_eq!(
        describe_payload["cache_dir_placeholder"],
        "<shared-cache-dir>"
    );
    assert!(
        describe_payload["command"]
            .as_str()
            .unwrap_or_default()
            .contains("--cache-dir '<shared-cache-dir>'")
    );
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
}

#[test]
fn new_requires_cache_dir_when_writing_a_template() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(
        tmpdir.path(),
        &[
            "new",
            "--template",
            "minimal-batch",
            "--name",
            "missing-cache",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--cache-dir is required"));
}

#[test]
fn setup_interactive_accepts_prompted_env_files_vars_and_binaries() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let setup = run_cli_with_stdin(
        tmpdir.path(),
        &["setup"],
        "research\nstack.yaml\n.env,.env.local\nA=1,B=two\nenroot=/usr/local/bin/enroot,sbatch=/usr/local/bin/sbatch\nresearch\n",
    );
    assert_success(&setup);
    let stdout = stdout_text(&setup);
    assert!(stdout.contains("Profile name [dev]:"));
    assert!(stdout.contains("Compose file [compose.yaml]:"));
    assert!(stdout.contains("Profile env files (comma-separated) []:"));
    assert!(stdout.contains("Profile env vars KEY=VALUE (comma-separated) []:"));
    assert!(stdout.contains("Profile binaries NAME=PATH (comma-separated) []:"));
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
}
