//! `hpc-compose up --remote[=HOST]` — the thin laptop -> login-node delegating
//! executor. On a non-login host it rsyncs the compose project to the login node
//! and runs `hpc-compose up` there over SSH, streaming the output back and
//! propagating the remote exit code.
//!
//! Scope (Option B, "thin"): explicit opt-in, rsync-every-time (no content
//! hash), and SSH ControlMaster multiplexing reused from [`ssh_hint`] so an OTP
//! login node prompts once. This is deliberately NOT the full laptop thin client
//! — there is no `login`/`logout` session, no `ssh -O check` fail-fast, no auto
//! mode-detection, and no `--source-hash`/`--no-restage`. The destination's
//! port, identity, and user belong in the caller's `~/.ssh/config`, which keeps
//! the CLI surface a bare host (or host alias).

use super::*;
use crate::exit::ExitCodeError;
use crate::shell_quote;

/// Optional env var of extra ssh options (whitespace-split) appended to every
/// ssh/rsync connection for this run — e.g. `-p 2222 -i ~/.ssh/cluster` for a
/// host not described in `~/.ssh/config`. Options with embedded spaces are not
/// supported here; put those in your ssh config instead.
const REMOTE_SSH_OPTS_ENV: &str = "HPC_COMPOSE_REMOTE_SSH_OPTS";

/// Resolve the SSH destination: the explicit `--remote=HOST` value when given,
/// otherwise the configured `login_host`.
pub(crate) fn resolve_remote_host(flag_value: &str, login_host: Option<&str>) -> Result<String> {
    let trimmed = flag_value.trim();
    if !trimmed.is_empty() {
        return Ok(trimmed.to_string());
    }
    match login_host.map(str::trim) {
        Some(host) if !host.is_empty() => Ok(host.to_string()),
        _ => bail!(
            "up --remote needs a destination: pass --remote=<host> or set login_host in settings \
             (the host's port, identity, and user belong in your ~/.ssh/config)"
        ),
    }
}

/// Remote staging directory (relative to the login node's home), derived from
/// the project directory name so repeated runs of the same project reuse it.
/// Sanitized to a shell- and filesystem-safe basename.
pub(crate) fn remote_stage_path(project_dir: &Path) -> String {
    let name = project_dir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "project".to_string());
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!(".hpc-compose-remote/{safe}")
}

/// Flags forwarded to the remote `hpc-compose up`. Only run-shape flags are
/// forwarded; tool-path overrides are intentionally dropped so the login node
/// resolves its own binaries. When not detaching, the remote runs over a
/// non-TTY SSH pipe, so line mode is forced to keep the streamed output
/// deterministic.
pub(crate) fn forwarded_up_flags(dry_run: bool, detach: bool, no_preflight: bool) -> Vec<String> {
    let mut flags = Vec::new();
    if dry_run {
        flags.push("--dry-run".to_string());
    }
    if detach {
        flags.push("--detach".to_string());
    }
    if no_preflight {
        flags.push("--no-preflight".to_string());
    }
    if !detach {
        flags.push("--watch-mode".to_string());
        flags.push("line".to_string());
    }
    flags
}

/// The remote shell command: cd into the staged project, then run `up`. The
/// user-controlled values (stage, spec path) are passed through the canonical
/// shell quoter; `cd`, `&&`, and the run-shape flags are fixed safe tokens.
pub(crate) fn build_remote_command(stage: &str, spec_rel: &str, flags: &[String]) -> String {
    let mut parts = vec![
        "cd".to_string(),
        shell_quote::quote(stage),
        "&&".to_string(),
        "hpc-compose".to_string(),
        "up".to_string(),
        "-f".to_string(),
        shell_quote::quote(spec_rel),
    ];
    parts.extend(flags.iter().cloned());
    parts.join(" ")
}

/// rsync arguments to mirror the local project into the remote stage dir. The
/// trailing slash on the source copies its contents into `stage/`. Heavy and
/// machine-local trees are always excluded; an optional `.hpcignore` adds more.
pub(crate) fn build_rsync_args(
    project_dir: &Path,
    host: &str,
    stage: &str,
    ssh_command: &str,
    hpcignore: Option<&Path>,
) -> Vec<String> {
    let mut args = vec![
        "-az".to_string(),
        "--delete".to_string(),
        "-e".to_string(),
        ssh_command.to_string(),
        "--exclude".to_string(),
        ".git".to_string(),
        "--exclude".to_string(),
        "target".to_string(),
        "--exclude".to_string(),
        ".hpc-compose".to_string(),
        "--exclude".to_string(),
        ".hpc-compose-remote".to_string(),
    ];
    if let Some(ignore) = hpcignore {
        args.push("--exclude-from".to_string());
        args.push(ignore.to_string_lossy().to_string());
    }
    args.push(format!("{}/", project_dir.display()));
    args.push(format!("{host}:{stage}/"));
    args
}

/// Parse [`REMOTE_SSH_OPTS_ENV`] into individual ssh arguments (whitespace-split).
pub(crate) fn parse_extra_ssh_opts(raw: Option<&str>) -> Vec<String> {
    raw.map(|value| value.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default()
}

/// Orchestrate the thin remote submit: resolve the destination, rsync the
/// project, then delegate `up` over SSH and propagate the remote exit code.
pub(crate) fn remote_up(
    context: &ResolvedContext,
    remote_flag: &str,
    local: bool,
    dry_run: bool,
    detach: bool,
    no_preflight: bool,
) -> Result<()> {
    if local {
        bail!(
            "up --remote cannot be combined with --local: --local launches on this host, \
             --remote delegates submission to the login node"
        );
    }

    let host = resolve_remote_host(remote_flag, context.login_host.as_deref())?;
    let compose_file = context.compose_file.value.clone();
    let project_dir = compose_file
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let spec_rel = compose_file
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .context("--remote: compose file path has no file name")?;
    let stage = remote_stage_path(&project_dir);

    let extra_opts = parse_extra_ssh_opts(env::var(REMOTE_SSH_OPTS_ENV).ok().as_deref());
    // One set of multiplexing opts for every connection this run makes (mkdir,
    // rsync, delegate), so an OTP login node authenticates once via ControlMaster.
    let base_ssh_args: Vec<String> = CONTROL_MASTER_SSH_OPTS
        .iter()
        .map(|s| (*s).to_string())
        .chain(extra_opts.iter().cloned())
        .collect();
    let ssh_command = format!("ssh {}", base_ssh_args.join(" "));
    let hpcignore_path = project_dir.join(".hpcignore");
    let hpcignore = hpcignore_path.exists().then_some(hpcignore_path.as_path());
    let rsync_args = build_rsync_args(&project_dir, &host, &stage, &ssh_command, hpcignore);

    // Progress goes to stderr so the remote command's stdout (e.g. "Submitted
    // batch job N") stays clean for callers that parse it.
    eprintln!("{}", term::styled_section_header("Remote submit"));
    eprintln!("  host:    {host}");
    eprintln!("  stage:   {host}:{stage}");
    eprintln!("  syncing: {}", project_dir.display());
    eprintln!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));

    // 1. Create the remote stage dir (rsync does not portably create the
    // intermediate parent), then mirror the project into it.
    let mut mkdir_args = base_ssh_args.clone();
    mkdir_args.push(host.clone());
    mkdir_args.push(format!("mkdir -p {}", shell_quote::quote(&stage)));
    let mkdir_status = Command::new("ssh")
        .args(&mkdir_args)
        .status()
        .context("failed to run ssh")?;
    if !mkdir_status.success() {
        bail!("failed to create remote stage dir '{stage}' on {host}; check your ~/.ssh/config");
    }
    let rsync_status = Command::new("rsync")
        .args(&rsync_args)
        .status()
        .context("failed to run rsync (is rsync installed on this host?)")?;
    if !rsync_status.success() {
        bail!("rsync to {host} failed; check the destination and your ~/.ssh/config");
    }

    // 2. Delegate to the login node's hpc-compose, streaming output back.
    let flags = forwarded_up_flags(dry_run, detach, no_preflight);
    let remote_command = build_remote_command(&stage, &spec_rel, &flags);
    eprintln!("  delegating: ssh {host} '{remote_command}'");
    let mut ssh_args = base_ssh_args.clone();
    ssh_args.push(host.clone());
    ssh_args.push(remote_command);
    let ssh_status = Command::new("ssh")
        .args(&ssh_args)
        .status()
        .context("failed to run ssh")?;
    match ssh_status.code() {
        Some(0) => Ok(()),
        Some(code) => Err(ExitCodeError(code).into()),
        None => bail!("remote hpc-compose up was terminated by a signal"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_remote_host_prefers_flag_over_login_host() {
        assert_eq!(
            resolve_remote_host("login01", Some("default-login")).unwrap(),
            "login01"
        );
    }

    #[test]
    fn resolve_remote_host_falls_back_to_login_host() {
        assert_eq!(
            resolve_remote_host("", Some("default-login")).unwrap(),
            "default-login"
        );
        assert_eq!(
            resolve_remote_host("  ", Some("default-login")).unwrap(),
            "default-login"
        );
    }

    #[test]
    fn resolve_remote_host_errors_when_no_destination() {
        let err = resolve_remote_host("", None).unwrap_err().to_string();
        assert!(err.contains("--remote=<host>"));
        assert!(err.contains("login_host"));
        assert!(resolve_remote_host("", Some("   ")).is_err());
    }

    #[test]
    fn remote_stage_path_sanitizes_the_project_name() {
        assert_eq!(
            remote_stage_path(Path::new("/home/me/my proj!")),
            ".hpc-compose-remote/my-proj-"
        );
        assert_eq!(
            remote_stage_path(Path::new("/home/me/specs")),
            ".hpc-compose-remote/specs"
        );
    }

    #[test]
    fn forwarded_flags_force_line_mode_unless_detached() {
        assert_eq!(
            forwarded_up_flags(false, false, false),
            vec!["--watch-mode", "line"]
        );
        assert_eq!(forwarded_up_flags(false, true, false), vec!["--detach"]);
        assert_eq!(
            forwarded_up_flags(true, false, true),
            vec!["--dry-run", "--no-preflight", "--watch-mode", "line"]
        );
    }

    #[test]
    fn build_remote_command_quotes_and_chains() {
        let cmd = build_remote_command(
            ".hpc-compose-remote/specs",
            "hello.yaml",
            &["--detach".to_string()],
        );
        assert_eq!(
            cmd,
            "cd '.hpc-compose-remote/specs' && hpc-compose up -f 'hello.yaml' --detach"
        );
    }

    #[test]
    fn build_rsync_args_targets_stage_and_carries_ssh_and_excludes() {
        let args = build_rsync_args(
            Path::new("/home/me/specs"),
            "login01",
            ".hpc-compose-remote/specs",
            "ssh -o ControlMaster=auto",
            None,
        );
        assert!(args.contains(&"-az".to_string()));
        assert!(args.contains(&"ssh -o ControlMaster=auto".to_string()));
        assert!(args.contains(&".git".to_string()));
        assert!(args.contains(&"target".to_string()));
        // Source has a trailing slash; destination is host:stage/.
        assert_eq!(args[args.len() - 2], "/home/me/specs/");
        assert_eq!(args[args.len() - 1], "login01:.hpc-compose-remote/specs/");
    }

    #[test]
    fn build_rsync_args_appends_hpcignore_when_present() {
        let args = build_rsync_args(
            Path::new("/p"),
            "h",
            "s",
            "ssh",
            Some(Path::new("/p/.hpcignore")),
        );
        let idx = args
            .iter()
            .position(|a| a == "--exclude-from")
            .expect("exclude-from present");
        assert_eq!(args[idx + 1], "/p/.hpcignore");
    }

    #[test]
    fn parse_extra_ssh_opts_splits_on_whitespace() {
        assert_eq!(parse_extra_ssh_opts(None), Vec::<String>::new());
        assert_eq!(parse_extra_ssh_opts(Some("")), Vec::<String>::new());
        assert_eq!(
            parse_extra_ssh_opts(Some("-p 2222 -i /k")),
            vec!["-p", "2222", "-i", "/k"]
        );
    }
}
