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
                "init",
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
        assert!(stdout_text(&init).contains("hpc-compose submit --watch -f"));
    }
}

#[test]
fn help_and_template_discovery_surface_guided_workflows() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let top_help = run_cli(tmpdir.path(), &["--help"]);
    assert_success(&top_help);
    let top_help_stdout = stdout_text(&top_help);
    assert!(top_help_stdout.contains("Normal run:"));
    assert!(top_help_stdout.contains("submit --watch -f compose.yaml"));
    assert!(top_help_stdout.contains("Debugging flow:"));
    assert!(top_help_stdout.contains("logs         Print tracked service logs"));
    assert!(top_help_stdout.contains("cancel       Cancel a tracked Slurm job"));
    assert!(top_help_stdout.contains("clean        Remove old tracked job directories"));
    assert!(top_help_stdout.contains("completions  Generate shell completions"));

    let init_help = run_cli(tmpdir.path(), &["init", "--help"]);
    assert_success(&init_help);
    let init_help_stdout = stdout_text(&init_help);
    assert!(init_help_stdout.contains("--list-templates"));
    assert!(init_help_stdout.contains("--describe-template <TEMPLATE>"));

    let cache_help = run_cli(tmpdir.path(), &["cache", "--help"]);
    assert_success(&cache_help);
    let cache_help_stdout = stdout_text(&cache_help);
    assert!(cache_help_stdout.contains("cache inspect -f compose.yaml"));
    assert!(cache_help_stdout.contains("list     List cached image artifacts"));
    assert!(cache_help_stdout.contains("inspect  Inspect cache reuse for the current plan"));
    assert!(cache_help_stdout.contains("prune    Prune cached image artifacts"));

    let submit_help = run_cli(tmpdir.path(), &["submit", "--help"]);
    assert_success(&submit_help);
    let submit_help_stdout = stdout_text(&submit_help);
    assert!(
        submit_help_stdout
            .contains("Poll scheduler state and stream tracked logs after submission")
    );
    assert!(
        submit_help_stdout.contains("Run preflight, prepare, and render without calling sbatch")
    );

    let preflight_help = run_cli(tmpdir.path(), &["preflight", "--help"]);
    assert_success(&preflight_help);
    assert!(stdout_text(&preflight_help).contains("Treat warnings as failures"));

    let list_templates = run_cli(tmpdir.path(), &["init", "--list-templates"]);
    assert_success(&list_templates);
    let list_stdout = stdout_text(&list_templates);
    assert!(list_stdout.contains("minimal-batch"));
    assert!(list_stdout.contains("multi-node-mpi"));
    assert!(list_stdout.contains("multi-node-torchrun"));

    let describe_template = run_cli(
        tmpdir.path(),
        &["init", "--describe-template", "multi-node-mpi"],
    );
    assert_success(&describe_template);
    let describe_stdout = stdout_text(&describe_template);
    assert!(describe_stdout.contains("template: multi-node-mpi"));
    assert!(describe_stdout.contains("allocation-wide"));
    assert!(describe_stdout.contains("hpc-compose init --template multi-node-mpi"));
}

#[test]
fn init_interactive_uses_prompted_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = tmpdir.path().join("interactive-init.yaml");
    let init = run_cli_with_stdin(
        tmpdir.path(),
        &[
            "init",
            "--output",
            output.to_str().expect("path"),
            "--force",
        ],
        "2\ninteractive-app\n/tmp/interactive-cache\n",
    );
    assert_success(&init);
    let rendered = fs::read_to_string(&output).expect("rendered");
    assert!(rendered.contains("name: interactive-app"));
    assert!(rendered.contains("job_name: interactive-app"));
    assert!(rendered.contains("cache_dir: /tmp/interactive-cache"));
    let stdout = stdout_text(&init);
    assert!(stdout.contains("Choose a template:"));
    assert!(stdout.contains("hpc-compose submit --watch -f"));
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
