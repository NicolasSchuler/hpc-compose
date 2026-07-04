//! Integration coverage for the `up --remote` / remote follow-up delegation
//! path, exercised with injected fake `ssh`/`rsync` binaries (resolved through
//! the settings `binaries` block, exactly like every other tool override). These
//! assert on the recorded argv logs: that rsync mirrors the project to the login
//! node, that ssh carries the ControlMaster multiplexing opts on mkdir/probe,
//! that follow-ups forward the right delegated command line, and that a failing
//! ssh surfaces an actionable error instead of a silent success.

mod support;

use std::fs;

use support::*;

/// Minimal compose spec; the remote path never parses services beyond volume
/// checks, so a single trivial service is enough.
fn write_remote_compose(tmpdir: &std::path::Path) -> std::path::PathBuf {
    write_compose(
        tmpdir,
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.join("cache").display(),
            tmpdir.join("local.sqsh").display(),
        ),
    )
}

/// Writes `.hpc-compose/settings.toml` pointing ssh/rsync at the injected fakes.
fn write_remote_settings(tmpdir: &std::path::Path, ssh: &std::path::Path, rsync: &std::path::Path) {
    let dir = tmpdir.join(".hpc-compose");
    fs::create_dir_all(&dir).expect("settings dir");
    fs::write(
        dir.join("settings.toml"),
        format!(
            "version = 1\n\n[defaults.binaries]\nssh = \"{}\"\nrsync = \"{}\"\n",
            ssh.display(),
            rsync.display(),
        ),
    )
    .expect("write settings");
}

#[test]
fn up_remote_stages_project_over_ssh_and_rsync() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = &fs::canonicalize(tmpdir.path()).expect("canonicalize tmpdir");
    let root = root.as_path();
    let compose = write_remote_compose(root);

    let ssh_log = root.join("ssh.log");
    let rsync_log = root.join("rsync.log");
    let rules = root.join("ssh-rules");
    let ssh = write_fake_ssh(root, &ssh_log, &rules);
    let rsync = write_fake_rsync(root, &rsync_log);
    write_remote_settings(root, &ssh, &rsync);

    // Canned responses for each connection the staging flow makes.
    write_ssh_rule(&rules, "10-mkdir", "mkdir -p", 0, "");
    // Probe: report a usable, newer-than-client hpc-compose so no install runs.
    write_ssh_rule(
        &rules,
        "20-probe",
        "--version",
        0,
        "hpc-compose\thpc-compose 999.0.0\n",
    );
    // Delegated `up` returns a Slurm submission line so the flow completes.
    write_ssh_rule(&rules, "30-up", "up -f", 0, "Submitted batch job 12345\n");

    let output = run_cli_with_env(
        root,
        &[
            "up",
            "--remote=fakehost",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("HOME", root.to_str().expect("home"))],
    );
    assert_success(&output);

    let ssh_recorded = fs::read_to_string(&ssh_log).expect("ssh log");
    // mkdir on the stage dir and the version probe both ran over ssh...
    assert!(
        ssh_recorded.contains("mkdir -p"),
        "ssh should create the remote stage dir; log:\n{ssh_recorded}"
    );
    assert!(
        ssh_recorded.contains("--version"),
        "ssh should probe the remote hpc-compose; log:\n{ssh_recorded}"
    );
    // ...carrying the ControlMaster multiplexing opts (one OTP per session).
    assert!(
        ssh_recorded.contains("ControlMaster=auto"),
        "ssh must carry ControlMaster opts; log:\n{ssh_recorded}"
    );
    assert!(
        ssh_recorded.contains("ControlPath=~/.ssh/cm-%r@%h:%p"),
        "ssh must carry the ControlPath spelling; log:\n{ssh_recorded}"
    );

    let rsync_recorded = fs::read_to_string(&rsync_log).expect("rsync log");
    // rsync mirrors the local project into the host's stage dir with --delete...
    assert!(
        rsync_recorded.contains("--delete"),
        "rsync should mirror with --delete; log:\n{rsync_recorded}"
    );
    assert!(
        rsync_recorded.contains(&format!("{}/", root.display())),
        "rsync source should be the local project; log:\n{rsync_recorded}"
    );
    assert!(
        rsync_recorded.contains("fakehost:.hpc-compose-remote/"),
        "rsync destination should be the host stage dir; log:\n{rsync_recorded}"
    );
    // ...transporting over the injected fake ssh via rsync -e.
    assert!(
        rsync_recorded.contains(ssh.to_str().expect("ssh path")),
        "rsync -e should use the resolved ssh binary; log:\n{rsync_recorded}"
    );
}

#[test]
fn remote_followup_forwards_status_command() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = &fs::canonicalize(tmpdir.path()).expect("canonicalize tmpdir");
    let root = root.as_path();
    let compose = write_remote_compose(root);

    let ssh_log = root.join("ssh.log");
    let rsync_log = root.join("rsync.log");
    let rules = root.join("ssh-rules");
    let ssh = write_fake_ssh(root, &ssh_log, &rules);
    let rsync = write_fake_rsync(root, &rsync_log);
    write_remote_settings(root, &ssh, &rsync);

    // The follow-up only probes, then delegates the rewritten command. The
    // default (no-match) response is a permissive exit 0, so the status
    // delegation itself needs no rule.
    write_ssh_rule(
        &rules,
        "20-probe",
        "--version",
        0,
        "hpc-compose\thpc-compose 999.0.0\n",
    );

    let output = run_cli_with_env(
        root,
        &[
            "status",
            "--remote=fakehost",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "424242",
        ],
        &[("HOME", root.to_str().expect("home"))],
    );
    assert_success(&output);

    let ssh_recorded = fs::read_to_string(&ssh_log).expect("ssh log");
    // The delegated command line cd's into the stage dir and forwards `status`
    // with the stage-relative compose path and the verbatim --job-id.
    assert!(
        ssh_recorded.contains("cd '.hpc-compose-remote/"),
        "follow-up should cd into the staged checkout; log:\n{ssh_recorded}"
    );
    assert!(
        ssh_recorded.contains("'status'"),
        "follow-up should forward the status subcommand; log:\n{ssh_recorded}"
    );
    assert!(
        ssh_recorded.contains("'--job-id' '424242'"),
        "follow-up should forward --job-id verbatim; log:\n{ssh_recorded}"
    );
    // The --remote flag itself is dropped before delegation.
    assert!(
        !ssh_recorded.contains("--remote"),
        "follow-up must not forward --remote; log:\n{ssh_recorded}"
    );
}

#[test]
fn remote_followup_forwards_pull_command() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = &fs::canonicalize(tmpdir.path()).expect("canonicalize tmpdir");
    let root = root.as_path();
    let compose = write_remote_compose(root);

    let ssh_log = root.join("ssh.log");
    let rsync_log = root.join("rsync.log");
    let rules = root.join("ssh-rules");
    let ssh = write_fake_ssh(root, &ssh_log, &rules);
    let rsync = write_fake_rsync(root, &rsync_log);
    write_remote_settings(root, &ssh, &rsync);

    write_ssh_rule(
        &rules,
        "20-probe",
        "--version",
        0,
        "hpc-compose\thpc-compose 999.0.0\n",
    );

    let output = run_cli_with_env(
        root,
        &[
            "pull",
            "--remote=fakehost",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "777",
        ],
        &[("HOME", root.to_str().expect("home"))],
    );
    assert_success(&output);

    let ssh_recorded = fs::read_to_string(&ssh_log).expect("ssh log");
    assert!(
        ssh_recorded.contains("'pull'"),
        "follow-up should forward the pull subcommand; log:\n{ssh_recorded}"
    );
    assert!(
        ssh_recorded.contains("'--job-id' '777'"),
        "follow-up should forward --job-id verbatim; log:\n{ssh_recorded}"
    );
}

#[test]
fn up_remote_surfaces_ssh_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = &fs::canonicalize(tmpdir.path()).expect("canonicalize tmpdir");
    let root = root.as_path();
    let compose = write_remote_compose(root);

    let ssh_log = root.join("ssh.log");
    let rsync_log = root.join("rsync.log");
    let rules = root.join("ssh-rules");
    let ssh = write_fake_ssh(root, &ssh_log, &rules);
    let rsync = write_fake_rsync(root, &rsync_log);
    write_remote_settings(root, &ssh, &rsync);

    // The very first connection (mkdir on the stage dir) fails: the command must
    // stop with an actionable error, not silently continue to rsync/dispatch.
    write_ssh_rule(
        &rules,
        "10-mkdir",
        "mkdir -p",
        17,
        "ssh: could not resolve host\n",
    );

    let output = run_cli_with_env(
        root,
        &[
            "up",
            "--remote=fakehost",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("HOME", root.to_str().expect("home"))],
    );
    assert_failure(&output);
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("failed to create remote stage dir"),
        "ssh failure should surface an actionable error; stderr:\n{stderr}"
    );
    // rsync must not have run after the ssh mkdir failed (no silent success).
    assert!(
        !rsync_log.exists(),
        "rsync should not run once the ssh staging step failed"
    );
}
