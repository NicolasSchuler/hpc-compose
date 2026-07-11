//! Integration coverage for the `salloc`-based `alloc` exec flow
//! (`src/commands/runtime/exec.rs::alloc`), exercised with an injected fake
//! `salloc` binary resolved through the standard `--salloc-bin` override.
//!
//! `alloc` is the only subcommand that spawns `salloc`; `shell` (the other
//! interactive exec in that module) is `srun`-based and already covered
//! elsewhere. These tests pin: (1) the salloc argv derived from the compose
//! resource spec plus the forwarded user command, (2) that the inner command
//! actually runs inside the allocation, and (3) that a nonzero salloc status is
//! propagated verbatim as the CLI's own exit code.

mod support;

use std::fs;

use support::*;

/// Compose file with a rich `x-slurm` resource block so we can assert the full
/// set of derived `salloc` flags. `submit_args` carries an unmodeled
/// passthrough flag while reservation uses its first-class field.
fn write_alloc_compose(dir: &std::path::Path, cache_dir: &std::path::Path) -> std::path::PathBuf {
    let local_image = dir.join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    write_compose(
        dir,
        "compose.yaml",
        &format!(
            r#"
name: exec-demo
x-slurm:
  cache_dir: {cache}
  job_name: exec-job
  nodes: 2
  time: "00:20:00"
  partition: gpu
  account: acct-1
  qos: high
  cpus_per_task: 8
  mem: 16G
  gpus: 2
  reservation: dev
  submit_args:
    - --exclusive
services:
  app:
    image: {image}
    command: ["echo", "ok"]
"#,
            cache = cache_dir.display(),
            image = local_image.display(),
        ),
    )
}

#[test]
fn alloc_invokes_salloc_with_derived_flags_and_runs_inner_command() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let compose = write_alloc_compose(tmpdir.path(), &cache_dir);

    let salloc_log = tmpdir.path().join("salloc.args");
    let salloc = write_fake_salloc(tmpdir.path(), &salloc_log, None);
    let scontrol = write_fake_scontrol(tmpdir.path());

    // The inner command drops a marker so we can prove `exec "$@"` reached it.
    let marker = tmpdir.path().join("inner.ran");
    let output = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--scontrol-bin",
            scontrol.to_str().expect("path"),
            "--",
            "bash",
            "-lc",
            &format!("touch '{}'", marker.display()),
        ],
    );
    assert_success(&output);

    let args = fs::read_to_string(&salloc_log).expect("salloc args");
    // Resource-derived flags from the compose x-slurm block.
    assert!(args.contains("--job-name=exec-job"), "argv: {args}");
    assert!(args.contains("--nodes=2"), "argv: {args}");
    assert!(args.contains("--time=00:20:00"), "argv: {args}");
    assert!(args.contains("--partition=gpu"), "argv: {args}");
    assert!(args.contains("--account=acct-1"), "argv: {args}");
    assert!(args.contains("--qos=high"), "argv: {args}");
    assert!(args.contains("--cpus-per-task=8"), "argv: {args}");
    assert!(args.contains("--mem=16G"), "argv: {args}");
    assert!(args.contains("--gpus=2"), "argv: {args}");
    assert!(args.contains("--reservation=dev"), "argv: {args}");
    // User-supplied unmodeled passthrough flag survives to salloc.
    assert!(args.contains("--exclusive"), "argv: {args}");
    // The bootstrap wrapper and the sentinel arg are appended after the options.
    assert!(args.contains("hpc-compose-alloc"), "argv: {args}");

    // The inner command ran inside the (faked) allocation.
    assert!(marker.exists(), "inner command marker was not created");
}

#[test]
fn alloc_propagates_salloc_exit_code() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let compose = write_alloc_compose(tmpdir.path(), &cache_dir);

    let salloc_log = tmpdir.path().join("salloc.args");
    // Fake salloc records its argv, then exits 3 without running any command,
    // simulating salloc itself failing (e.g. allocation rejected).
    let salloc = write_fake_salloc(tmpdir.path(), &salloc_log, Some(3));
    let scontrol = write_fake_scontrol(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--scontrol-bin",
            scontrol.to_str().expect("path"),
            "--",
            "true",
        ],
    );

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(3),
        "expected the CLI to exit with salloc's status 3; stdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
    // salloc was actually invoked (argv recorded) before the failure.
    assert!(salloc_log.exists(), "salloc was never invoked");
}

#[test]
fn alloc_forwards_user_command_after_the_sentinel() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let compose = write_alloc_compose(tmpdir.path(), &cache_dir);

    let salloc_log = tmpdir.path().join("salloc.args");
    let salloc = write_fake_salloc(tmpdir.path(), &salloc_log, None);
    let scontrol = write_fake_scontrol(tmpdir.path());

    // A distinctive multi-token command whose tokens must all reach salloc's
    // argv, in order, after the `hpc-compose-alloc` sentinel.
    let output = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--scontrol-bin",
            scontrol.to_str().expect("path"),
            "--",
            "echo",
            "sentinel-marker-xyz",
        ],
    );
    assert_success(&output);

    let args = fs::read_to_string(&salloc_log).expect("salloc args");
    // Order matters: the wrapper runs `bash -lc <bootstrap> hpc-compose-alloc
    // echo sentinel-marker-xyz`, so the user tokens appear after the sentinel.
    let sentinel_at = args.find("hpc-compose-alloc").expect("sentinel present");
    let echo_at = args.find(" echo ").expect("user command present");
    let marker_at = args.find("sentinel-marker-xyz").expect("user arg present");
    assert!(
        sentinel_at < echo_at && echo_at < marker_at,
        "user command not forwarded after sentinel; argv: {args}"
    );
}
