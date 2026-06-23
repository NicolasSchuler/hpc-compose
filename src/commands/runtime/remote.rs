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

/// Options that preserve the local `up` CLI contract when delegating the actual
/// submission to a login node. Tool-path overrides are intentionally excluded:
/// the login node should resolve its own scheduler/runtime binaries from its
/// environment and staged settings.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteUpOptions {
    pub keep_failed_prep: bool,
    pub skip_prepare: bool,
    pub force_rebuild: bool,
    pub no_preflight: bool,
    pub allow_resume_changes: bool,
    pub resume_diff_only: bool,
    pub dry_run: bool,
    pub detach: bool,
    pub format: Option<OutputFormat>,
    pub print_endpoints: bool,
    pub watch_mode: WatchMode,
    pub hold_on_exit: HoldOnExit,
    pub quiet: bool,
}

fn output_format_arg(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::Text => "text",
        OutputFormat::Json => "json",
    }
}

fn watch_mode_arg(mode: WatchMode) -> &'static str {
    match mode {
        WatchMode::Auto => "auto",
        WatchMode::Tui => "tui",
        WatchMode::Line => "line",
    }
}

fn hold_on_exit_arg(hold: HoldOnExit) -> &'static str {
    match hold {
        HoldOnExit::Never => "never",
        HoldOnExit::Failure => "failure",
        HoldOnExit::Always => "always",
    }
}

/// Chooses the directory rsynced into the remote stage. When project settings
/// were discovered, the staged root must include both the compose file and the
/// settings base so the delegated command resolves the same defaults, profiles,
/// env files, and cluster profile.
pub(crate) fn remote_stage_root(context: &ResolvedContext, compose_file: &Path) -> Result<PathBuf> {
    let compose_dir = compose_file
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let Some(settings_base) = context.settings_base_dir.as_ref() else {
        return Ok(compose_dir);
    };

    if compose_file.starts_with(settings_base) {
        return Ok(settings_base.clone());
    }
    if settings_base.starts_with(&compose_dir) {
        return Ok(compose_dir);
    }
    bail!(
        "up --remote cannot stage settings from '{}' because they are outside the compose project '{}'",
        settings_base.display(),
        compose_dir.display()
    );
}

/// Global flags forwarded before the `up` subcommand so the remote process uses
/// the same settings context as the local command resolution.
pub(crate) fn forwarded_global_flags(
    context: &ResolvedContext,
    stage_root: &Path,
    quiet: bool,
) -> Result<Vec<String>> {
    let mut flags = Vec::new();
    if quiet {
        flags.push("--quiet".to_string());
    }
    if let Some(settings_path) = context.settings_path.as_ref() {
        let rel = settings_path.strip_prefix(stage_root).with_context(|| {
            format!(
                "up --remote cannot forward settings file '{}' because it is outside the staged project '{}'",
                settings_path.display(),
                stage_root.display()
            )
        })?;
        flags.push("--settings-file".to_string());
        flags.push(shell_quote::quote(&rel.to_string_lossy()));
    }
    if let Some(profile) = context.selected_profile.as_ref() {
        flags.push("--profile".to_string());
        flags.push(shell_quote::quote(profile));
    }
    Ok(flags)
}

/// Flags forwarded to the remote `hpc-compose up`. When not detaching, the
/// default remote stream runs over a non-TTY SSH pipe, so auto mode is converted
/// to line mode to keep output deterministic. Explicit TUI mode is rejected
/// before SSH because it cannot work through that non-TTY pipe.
pub(crate) fn forwarded_up_flags(options: RemoteUpOptions) -> Result<Vec<String>> {
    if !options.detach && options.watch_mode == WatchMode::Tui {
        bail!("up --remote cannot be combined with --watch-mode tui; use --watch-mode line");
    }

    let mut flags = Vec::new();
    if options.keep_failed_prep {
        flags.push("--keep-failed-prep".to_string());
    }
    if options.skip_prepare {
        flags.push("--skip-prepare".to_string());
    }
    if options.force_rebuild {
        flags.push("--force-rebuild".to_string());
    }
    if options.no_preflight {
        flags.push("--no-preflight".to_string());
    }
    if options.allow_resume_changes {
        flags.push("--allow-resume-changes".to_string());
    }
    if options.resume_diff_only {
        flags.push("--resume-diff-only".to_string());
    }
    if options.dry_run {
        flags.push("--dry-run".to_string());
    }
    if options.detach {
        flags.push("--detach".to_string());
    }
    if let Some(format) = options.format {
        flags.push("--format".to_string());
        flags.push(output_format_arg(format).to_string());
    }
    if options.print_endpoints {
        flags.push("--print-endpoints".to_string());
    }
    if !options.detach {
        flags.push("--watch-mode".to_string());
        flags.push(
            match options.watch_mode {
                WatchMode::Auto => "line",
                WatchMode::Line => "line",
                WatchMode::Tui => unreachable!("TUI mode rejected above"),
            }
            .to_string(),
        );
    } else if options.watch_mode != WatchMode::Auto {
        flags.push("--watch-mode".to_string());
        flags.push(watch_mode_arg(options.watch_mode).to_string());
    }
    if options.hold_on_exit != HoldOnExit::Failure {
        flags.push("--hold-on-exit".to_string());
        flags.push(hold_on_exit_arg(options.hold_on_exit).to_string());
    }
    Ok(flags)
}

/// The remote shell command: cd into the staged project, then run `up`. The
/// user-controlled values (stage, spec path, forwarded settings/profile values)
/// are passed through the canonical shell quoter; `cd`, `&&`, and flag names are
/// fixed safe tokens.
pub(crate) fn build_remote_command(
    stage: &str,
    spec_rel: &str,
    global_flags: &[String],
    up_flags: &[String],
) -> String {
    let mut parts = vec![
        "cd".to_string(),
        shell_quote::quote(stage),
        "&&".to_string(),
        "hpc-compose".to_string(),
    ];
    parts.extend(global_flags.iter().cloned());
    parts.extend([
        "up".to_string(),
        "-f".to_string(),
        shell_quote::quote(spec_rel),
    ]);
    parts.extend(up_flags.iter().cloned());
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
        ".hpc-compose/jobs/".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/sweeps/".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/locks/".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/logs/".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/latest*.json".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/[0-9]*/".to_string(),
        "--exclude".to_string(),
        ".hpc-compose/local-*/".to_string(),
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
    options: RemoteUpOptions,
) -> Result<()> {
    if local {
        bail!(
            "up --remote cannot be combined with --local: --local launches on this host, \
             --remote delegates submission to the login node"
        );
    }

    let host = resolve_remote_host(remote_flag, context.login_host.as_deref())?;
    let compose_file = context.compose_file.value.clone();
    let project_dir = remote_stage_root(context, &compose_file)?;
    let spec_rel = compose_file
        .strip_prefix(&project_dir)
        .with_context(|| {
            format!(
                "--remote: compose file '{}' is outside staged project '{}'",
                compose_file.display(),
                project_dir.display()
            )
        })?
        .to_string_lossy()
        .to_string();
    if spec_rel.is_empty() {
        bail!("--remote: compose file path has no file name");
    }
    let global_flags = forwarded_global_flags(context, &project_dir, options.quiet)?;
    let up_flags = forwarded_up_flags(options)?;
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
    let remote_command = build_remote_command(&stage, &spec_rel, &global_flags, &up_flags);
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
    use hpc_compose::context::{
        ResolveRequest, Settings, SettingsProfile, resolve, write_settings,
    };

    fn remote_options() -> RemoteUpOptions {
        RemoteUpOptions {
            keep_failed_prep: false,
            skip_prepare: false,
            force_rebuild: false,
            no_preflight: false,
            allow_resume_changes: false,
            resume_diff_only: false,
            dry_run: false,
            detach: false,
            format: None,
            print_endpoints: false,
            watch_mode: WatchMode::Auto,
            hold_on_exit: HoldOnExit::Failure,
            quiet: false,
        }
    }

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
            forwarded_up_flags(remote_options()).unwrap(),
            vec!["--watch-mode", "line"]
        );
        assert_eq!(
            forwarded_up_flags(RemoteUpOptions {
                detach: true,
                ..remote_options()
            })
            .unwrap(),
            vec!["--detach"]
        );
        assert_eq!(
            forwarded_up_flags(RemoteUpOptions {
                dry_run: true,
                no_preflight: true,
                ..remote_options()
            })
            .unwrap(),
            vec!["--no-preflight", "--dry-run", "--watch-mode", "line"]
        );
    }

    #[test]
    fn forwarded_flags_preserve_behavioral_up_options() {
        let flags = forwarded_up_flags(RemoteUpOptions {
            keep_failed_prep: true,
            skip_prepare: true,
            force_rebuild: true,
            no_preflight: true,
            allow_resume_changes: true,
            resume_diff_only: true,
            dry_run: true,
            detach: true,
            format: Some(OutputFormat::Json),
            print_endpoints: true,
            watch_mode: WatchMode::Line,
            hold_on_exit: HoldOnExit::Always,
            quiet: false,
        })
        .unwrap();
        assert_eq!(
            flags,
            vec![
                "--keep-failed-prep",
                "--skip-prepare",
                "--force-rebuild",
                "--no-preflight",
                "--allow-resume-changes",
                "--resume-diff-only",
                "--dry-run",
                "--detach",
                "--format",
                "json",
                "--print-endpoints",
                "--watch-mode",
                "line",
                "--hold-on-exit",
                "always",
            ]
        );
    }

    #[test]
    fn forwarded_flags_reject_tui_remote_watch() {
        let err = forwarded_up_flags(RemoteUpOptions {
            watch_mode: WatchMode::Tui,
            ..remote_options()
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("--watch-mode tui"));
    }

    #[test]
    fn remote_stage_root_and_globals_preserve_project_settings_context() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let root = tmpdir.path();
        fs::create_dir_all(root.join(".hpc-compose")).expect("settings dir");
        fs::create_dir_all(root.join("configs")).expect("configs dir");
        fs::write(root.join("configs/app.yaml"), "name: demo\nservices: {}\n").expect("compose");

        let mut settings = Settings {
            default_profile: Some("gpu".to_string()),
            ..Settings::default()
        };
        settings
            .profiles
            .insert("gpu".to_string(), SettingsProfile::default());
        write_settings(&root.join(".hpc-compose/settings.toml"), &settings).expect("settings");

        let context = resolve(&ResolveRequest {
            cwd: root.to_path_buf(),
            compose_file_override: Some(root.join("configs/app.yaml")),
            ..ResolveRequest::default()
        })
        .expect("resolve context");
        let stage_root = remote_stage_root(&context, &context.compose_file.value).unwrap();
        assert_eq!(stage_root, root);
        assert_eq!(
            context
                .compose_file
                .value
                .strip_prefix(&stage_root)
                .unwrap()
                .to_string_lossy(),
            "configs/app.yaml"
        );
        assert_eq!(
            forwarded_global_flags(&context, &stage_root, true).unwrap(),
            vec![
                "--quiet",
                "--settings-file",
                "'.hpc-compose/settings.toml'",
                "--profile",
                "'gpu'",
            ]
        );
    }

    #[test]
    fn build_remote_command_quotes_and_chains() {
        let cmd = build_remote_command(
            ".hpc-compose-remote/specs",
            "configs/hello.yaml",
            &[
                "--settings-file".to_string(),
                "'.hpc-compose/settings.toml'".to_string(),
            ],
            &[
                "--detach".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ],
        );
        assert_eq!(
            cmd,
            "cd '.hpc-compose-remote/specs' && hpc-compose --settings-file '.hpc-compose/settings.toml' up -f 'configs/hello.yaml' --detach --format json"
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
        assert!(!args.contains(&".hpc-compose".to_string()));
        assert!(args.contains(&".hpc-compose/jobs/".to_string()));
        assert!(args.contains(&".hpc-compose/sweeps/".to_string()));
        assert!(args.contains(&".hpc-compose/locks/".to_string()));
        assert!(args.contains(&".hpc-compose/latest*.json".to_string()));
        assert!(args.contains(&".hpc-compose/[0-9]*/".to_string()));
        assert!(args.contains(&".hpc-compose/local-*/".to_string()));
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
