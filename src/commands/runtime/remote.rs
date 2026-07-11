//! `hpc-compose up --remote[=HOST]` — the thin laptop -> login-node delegating
//! executor. On a non-login host it rsyncs the compose project to the login node
//! and runs `hpc-compose up` there over SSH, streaming the output back and
//! propagating the remote exit code.
//!
//! Scope (Option B, "thin"): explicit opt-in, rsync-every-time (no content
//! hash), and SSH ControlMaster multiplexing reused from `ssh_hint` so an OTP
//! login node prompts once. This is deliberately NOT the full laptop thin client
//! — there is no `login`/`logout` session, no `ssh -O check` fail-fast, no auto
//! mode-detection, and no `--source-hash`/`--no-restage`. The destination's
//! port, identity, and user belong in the caller's `~/.ssh/config`, which keeps
//! the CLI surface a bare host (or host alias).

use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use hpc_compose::cli::{HoldOnExit, OutputFormat, RemoteInstallMode, WatchMode};
use hpc_compose::context::ResolvedContext;

use super::MetricsOverrides;
use super::ssh_hint::{CONTROL_MASTER_SSH_OPTS, NONINTERACTIVE_SSH_OPTS, OTP_MULTIPLEX_NOTE};
use crate::exit::ExitCodeError;
use crate::shell_quote;
use crate::term;
use hpc_compose::context::repo_root_or_cwd;

/// Optional env var of extra ssh options (whitespace-split) appended to every
/// ssh/rsync connection for this run — e.g. `-p 2222 -i ~/.ssh/cluster` for a
/// host not described in `~/.ssh/config`. Options with embedded spaces are not
/// supported here; put those in your ssh config instead.
const REMOTE_SSH_OPTS_ENV: &str = "HPC_COMPOSE_REMOTE_SSH_OPTS";

/// Env override for the SSH username applied to a bare `up --remote` host.
const REMOTE_USER_ENV: &str = "HPC_COMPOSE_REMOTE_USER";

/// Env override for the `--remote-install` mode (`auto`/`never`/`force`).
const REMOTE_INSTALL_ENV: &str = "HPC_COMPOSE_REMOTE_INSTALL";

/// Env override for the install-script URL used to bootstrap the login node.
const REMOTE_INSTALL_URL_ENV: &str = "HPC_COMPOSE_REMOTE_INSTALL_URL";

/// Canonical installer. Fetching from `main` runs the moving script, which still
/// installs the newest published `releases/download/<tag>/...` asset (matches the
/// documented install one-liner), so no root and a checksum-verified tarball.
const DEFAULT_INSTALL_URL: &str =
    "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/main/install.sh";

/// Resolve the SSH destination: the explicit `--remote=HOST` value when given,
/// otherwise the configured `login_host`.
pub(crate) fn resolve_remote_host(flag_value: &str, login_host: Option<&str>) -> Result<String> {
    let trimmed = flag_value.trim();
    if !trimmed.is_empty() {
        validate_remote_destination(trimmed)?;
        return Ok(trimmed.to_string());
    }
    match login_host.map(str::trim) {
        Some(host) if !host.is_empty() => {
            validate_remote_destination(host)?;
            Ok(host.to_string())
        }
        _ => bail!(
            "up --remote needs a destination: pass --remote=<host> or set login_host in settings \
             (the host's port, identity, and user belong in your ~/.ssh/config)"
        ),
    }
}

/// Keeps an SSH/rsync destination on the operand side of their command-line
/// grammars. `Command` avoids shell expansion, but both clients still parse a
/// leading dash as a local option; rsync also splits `user@host:path` itself.
/// Ports belong in SSH config, while IPv6 literals use the bracketed spelling
/// required by rsync's remote-destination syntax.
fn validate_remote_destination(destination: &str) -> Result<()> {
    let invalid = |reason: &str| {
        anyhow!(
            "invalid SSH destination {destination:?}: {reason}; use a host alias, user@host, or \
             bracketed IPv6 literal, and put ports/options in ~/.ssh/config or \
             {REMOTE_SSH_OPTS_ENV}"
        )
    };

    if destination.starts_with('-') {
        return Err(invalid("it must not begin with '-'"));
    }
    if destination
        .chars()
        .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(invalid("whitespace and control characters are not allowed"));
    }
    if destination.contains(['/', '\\']) {
        return Err(invalid("path separators are not allowed"));
    }

    let (user, host) = match destination.split_once('@') {
        Some((user, host)) => {
            if user.is_empty() || host.is_empty() || host.contains('@') {
                return Err(invalid(
                    "user@host must contain exactly one non-empty '@' pair",
                ));
            }
            (Some(user), host)
        }
        None => (None, destination),
    };
    if host.starts_with('-') {
        return Err(invalid("the host component must not begin with '-'"));
    }
    if user.is_some_and(|user| user.contains(['[', ']', ':'])) {
        return Err(invalid("the user component contains destination syntax"));
    }

    if let Some(inner) = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        if inner.is_empty() || !inner.contains(':') || inner.contains(['[', ']']) {
            return Err(invalid(
                "brackets are reserved for a non-empty IPv6 literal",
            ));
        }
    } else if host.contains(['[', ']', ':']) {
        return Err(invalid(
            "ports are not accepted here and IPv6 literals must be enclosed in brackets",
        ));
    }

    Ok(())
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
    pub install_mode: RemoteInstallMode,
    pub metrics_overrides: MetricsOverrides,
    /// Forward `--prepare-verbose` so the login node streams raw image-prepare
    /// output (the `HPC_COMPOSE_PREPARE_VERBOSE` env does not cross SSH).
    pub prepare_verbose: bool,
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
    if options.prepare_verbose {
        flags.push("--prepare-verbose".to_string());
    }
    if options.metrics_overrides.disable {
        flags.push("--no-metrics".to_string());
    } else if let Some(interval) = options.metrics_overrides.interval_seconds {
        flags.push("--metrics-interval".to_string());
        flags.push(interval.to_string());
    }
    Ok(flags)
}

/// The remote shell command: cd into the staged project, then run `up`. The
/// user-controlled values (stage, spec path, forwarded settings/profile values)
/// are passed through the canonical shell quoter; `cd`, `&&`, and flag names are
/// fixed safe tokens.
pub(crate) fn build_remote_command(
    remote_bin: &str,
    stage: &str,
    spec_rel: &str,
    global_flags: &[String],
    up_flags: &[String],
) -> String {
    let mut parts = vec![
        "cd".to_string(),
        shell_quote::quote(stage),
        "&&".to_string(),
        shell_quote::quote(remote_bin),
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

/// Runs the delegated `up` over ssh, streaming its stdout back live while
/// capturing the submitted Slurm job id (from `Submitted batch job N`) so the
/// follow-up hints can carry the real id instead of a `<job-id>` placeholder.
/// stderr stays inherited so remote progress still flows through unchanged.
fn run_delegate_capturing_job_id(
    ssh_bin: &str,
    ssh_args: &[String],
) -> Result<(std::process::ExitStatus, Option<String>)> {
    let mut child = Command::new(ssh_bin)
        .args(ssh_args)
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to run ssh")?;
    let mut job_id = None;
    if let Some(stdout) = child.stdout.take() {
        let mut sink = io::stdout();
        for line in BufReader::new(stdout).lines() {
            let line = line.context("failed to read delegated hpc-compose output")?;
            if job_id.is_none() {
                job_id = extract_job_id(&line);
            }
            let _ = writeln!(sink, "{line}");
            let _ = sink.flush();
        }
    }
    let status = child.wait().context("failed to wait for delegated ssh")?;
    Ok((status, job_id))
}

/// Rewrites a local follow-up argv (`status`/`ps`/`stats`/`logs`/`score`/`pull`
/// plus its flags) for delegation to the remote staged checkout: drops the `--remote` flag,
/// rewrites local compose/settings paths to stage-relative paths, and forwards
/// every other argument — the subcommand name, `--job-id`, `--format`,
/// `--follow`, … — verbatim, so new flags work without touching this code.
fn rewrite_followup_args(
    raw: &[String],
    spec_rel: &str,
    staged_settings_rel: Option<&str>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    let mut saw_file = false;
    let mut iter = raw.iter();
    while let Some(arg) = iter.next() {
        // `--remote` uses require_equals, so the bare form carries no separate
        // value token (only `--remote=HOST` does) — never consume the next arg.
        if arg == "--remote" || arg.starts_with("--remote=") {
            continue;
        }
        if arg == "-f" || arg == "--file" {
            iter.next(); // drop the local path
            out.push("-f".to_string());
            out.push(spec_rel.to_string());
            saw_file = true;
            continue;
        }
        if arg.starts_with("-f=") || arg.starts_with("--file=") {
            out.push("-f".to_string());
            out.push(spec_rel.to_string());
            saw_file = true;
            continue;
        }
        if arg == "--settings-file" {
            iter.next(); // drop the local path
            if let Some(settings_rel) = staged_settings_rel {
                out.push("--settings-file".to_string());
                out.push(settings_rel.to_string());
            }
            continue;
        }
        if arg.starts_with("--settings-file=") {
            if let Some(settings_rel) = staged_settings_rel {
                out.push("--settings-file".to_string());
                out.push(settings_rel.to_string());
            }
            continue;
        }
        out.push(arg.clone());
    }
    if !saw_file {
        // The user relied on default compose discovery; pin the staged path.
        out.push("-f".to_string());
        out.push(spec_rel.to_string());
    }
    out
}

/// Delegates a read-only follow-up command (`status`/`ps`/`stats`/`logs`/`score`/`pull`)
/// to the login node's staged checkout, reusing the same host/login-user/staging context
/// as `up --remote`. The project is assumed already staged by a prior
/// `up --remote`, so this does **not** rsync (which would clobber the remote
/// tracking state with the laptop's); it runs the command in the existing stage
/// dir and streams the output back. `pull --remote` therefore prints the rsync
/// command from the login-node context; the user still runs that command from
/// the laptop to copy artifacts locally.
/// Dispatch helper for the read-only follow-up commands
/// (`status`/`ps`/`stats`/`logs`/`score`/`pull`). When `--remote` was supplied,
/// delegate to [`remote_followup`] and return `Some(result)`; otherwise return
/// `None` so the caller runs the command against local tracking state. The
/// caller resolves the context first, so context-dependent argument validation
/// keeps running ahead of the remote branch exactly as before.
pub(crate) fn maybe_remote_followup(
    context: &ResolvedContext,
    remote: Option<&str>,
) -> Option<Result<()>> {
    remote.map(|remote_flag| remote_followup(context, remote_flag))
}

pub(crate) fn remote_followup(context: &ResolvedContext, remote_flag: &str) -> Result<()> {
    let bare_host = resolve_remote_host(remote_flag, context.login_host.as_deref())?;
    let env_user = env::var(REMOTE_USER_ENV).ok();
    let host = apply_login_user(
        &bare_host,
        context.login_user.as_deref(),
        env_user.as_deref(),
    );
    validate_remote_destination(&host)?;
    let compose_file = context.compose_file.value.clone();
    let project_dir = remote_stage_root(context, &compose_file)?;
    let spec_rel = compose_file
        .strip_prefix(&project_dir)
        .map(|rel| rel.to_string_lossy().to_string())
        .map_err(|_| {
            anyhow!(
                "--remote: compose file '{}' is outside staged project '{}'",
                compose_file.display(),
                project_dir.display()
            )
        })?;
    if spec_rel.is_empty() {
        bail!("--remote: compose file path has no file name");
    }
    let staged_settings_rel = context
        .settings_path
        .as_ref()
        .map(|settings_path| {
            settings_path
                .strip_prefix(&project_dir)
                .map(|rel| rel.to_string_lossy().to_string())
                .with_context(|| {
                    format!(
                        "--remote cannot forward settings file '{}' because it is outside the staged project '{}'",
                        settings_path.display(),
                        project_dir.display()
                    )
                })
        })
        .transpose()?;
    let stage = remote_stage_path(&project_dir);
    ensure_control_master_dir()?;
    let extra_opts = parse_extra_ssh_opts(env::var(REMOTE_SSH_OPTS_ENV).ok().as_deref());
    let base_ssh_args = build_base_ssh_args(&extra_opts);
    let ssh_bin = context.binaries.ssh.value.as_str();
    let required = parse_version(env!("CARGO_PKG_VERSION")).unwrap_or((0, 0, 0));
    let remote_bin = probe_remote_binary(ssh_bin, &base_ssh_args, &host, required)?
        .map(|binary| binary.path)
        .ok_or_else(|| {
            anyhow!(
                "remote hpc-compose was not found on {host}; run `hpc-compose up --remote={bare_host}` \
                 first (it installs it), or {}",
                manual_install_hint()
            )
        })?;

    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut parts = vec![
        "cd".to_string(),
        shell_quote::quote(&stage),
        "&&".to_string(),
        shell_quote::quote(&remote_bin),
    ];
    parts.extend(
        rewrite_followup_args(&raw, &spec_rel, staged_settings_rel.as_deref())
            .iter()
            .map(|arg| shell_quote::quote(arg)),
    );
    let remote_cmd = parts.join(" ");

    eprintln!("{}", term::styled_section_header("Remote follow-up"));
    eprintln!("  host:  {host}");
    eprintln!("  stage: {host}:{stage}");
    eprintln!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));

    let mut args = base_ssh_args;
    args.push("--".to_string());
    args.push(host.clone());
    args.push(remote_cmd);
    let status = Command::new(ssh_bin)
        .args(&args)
        .status()
        .context("failed to run ssh for the remote follow-up command")?;
    child_status_result(
        status,
        "remote follow-up command was terminated by a signal",
    )
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
        // Runtime/job state is excluded below and therefore protected from
        // `--delete`; do not protect the parent `.hpc-compose/` directory,
        // because staged settings/cluster files must still update or disappear.
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
    args.push("--".to_string());
    args.push(format!("{}/", project_dir.display()));
    args.push(format!("{host}:{stage}/"));
    args
}

/// Parse [`REMOTE_SSH_OPTS_ENV`] into individual ssh arguments (whitespace-split).
pub(crate) fn parse_extra_ssh_opts(raw: Option<&str>) -> Vec<String> {
    raw.map(|value| value.split_whitespace().map(str::to_string).collect())
        .unwrap_or_default()
}

/// The ssh option vector shared by every connection a remote command makes
/// (mkdir, probe, rsync `-e`, delegate, follow-up). User-supplied
/// `HPC_COMPOSE_REMOTE_SSH_OPTS` come FIRST so they win ssh's first-value-wins
/// precedence (e.g. a longer `ControlPersist` for an agent loop); the defaults
/// then add connection multiplexing (one OTP per session) and neutralize an
/// interactive Host alias so delegation is not hijacked by it.
pub(crate) fn build_base_ssh_args(extra_opts: &[String]) -> Vec<String> {
    extra_opts
        .iter()
        .cloned()
        .chain(NONINTERACTIVE_SSH_OPTS.iter().map(|s| (*s).to_string()))
        .chain(CONTROL_MASTER_SSH_OPTS.iter().map(|s| (*s).to_string()))
        .collect()
}

/// Whether this binary is an *unreleased* local build (tracked working tree had
/// uncommitted changes at build time, per `build.rs`). Used to warn before
/// delegating to a login node running the published release of the same version.
fn local_build_is_unreleased() -> bool {
    option_env!("HPC_COMPOSE_BUILD_DIRTY") == Some("1")
}

/// Extracts the Slurm job id from a delegated-`up` output line such as
/// `Submitted batch job 1653779`, so the remote follow-up hints can be
/// copy-pasteable instead of carrying a `<job-id>` placeholder.
fn extract_job_id(line: &str) -> Option<String> {
    const MARKER: &str = "Submitted batch job ";
    let rest = &line[line.find(MARKER)? + MARKER.len()..];
    let id: String = rest.chars().take_while(char::is_ascii_digit).collect();
    (!id.is_empty()).then_some(id)
}

/// Applies an SSH username to a bare destination. A destination that already
/// carries a user (`user@host`) is returned unchanged; otherwise the first
/// non-empty of `env_user` then `settings_user` is prepended as `user@host`.
pub(crate) fn apply_login_user(
    dest: &str,
    settings_user: Option<&str>,
    env_user: Option<&str>,
) -> String {
    if dest.contains('@') {
        return dest.to_string();
    }
    let user = env_user
        .map(str::trim)
        .filter(|user| !user.is_empty())
        .or_else(|| settings_user.map(str::trim).filter(|user| !user.is_empty()));
    match user {
        Some(user) => format!("{user}@{dest}"),
        None => dest.to_string(),
    }
}

/// Parses a dotted `MAJOR.MINOR.PATCH` prefix from a token, ignoring any
/// pre-release/build suffix.
fn parse_semver(token: &str) -> Option<(u64, u64, u64)> {
    let core = token.split(['-', '+']).next().unwrap_or(token);
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    Some((major, minor, patch))
}

/// Extracts a version from a `--version` line such as `hpc-compose 0.1.49`.
fn parse_version(line: &str) -> Option<(u64, u64, u64)> {
    line.split_whitespace().find_map(parse_semver)
}

fn fmt_ver(version: (u64, u64, u64)) -> String {
    format!("{}.{}.{}", version.0, version.1, version.2)
}

/// One-line manual install hint shown when auto-install is disabled or fails.
fn manual_install_hint() -> String {
    format!("on the login node run: curl -fsSL {DEFAULT_INSTALL_URL} | sh")
}

/// Resolves the effective remote-install mode: the `HPC_COMPOSE_REMOTE_INSTALL`
/// env value (when a recognized mode) wins over the `--remote-install` flag.
fn resolve_install_mode(flag: RemoteInstallMode, env_value: Option<&str>) -> RemoteInstallMode {
    match env_value
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("auto") => RemoteInstallMode::Auto,
        Some("never") => RemoteInstallMode::Never,
        Some("force") => RemoteInstallMode::Force,
        _ => flag,
    }
}

/// The login node's resolved `hpc-compose`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteBinary {
    /// Invocation path, e.g. `/home/u/.local/bin/hpc-compose`.
    path: String,
    /// Parsed version; `None` when the printed `--version` was unrecognizable.
    version: Option<(u64, u64, u64)>,
}

/// Parses the capability probe's stdout (`<path>\t<version line>` lines, or `MISSING`).
fn parse_probe_candidates(stdout: &str) -> Vec<RemoteBinary> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "MISSING")
        .filter_map(|line| {
            let (path, version_line) = line.split_once('\t').unwrap_or((line, ""));
            let path = path.trim();
            (!path.is_empty()).then(|| RemoteBinary {
                path: path.to_string(),
                version: parse_version(version_line),
            })
        })
        .collect()
}

/// Chooses the first probe candidate that satisfies this client, otherwise the
/// first resolved candidate so the install/error path can report what was found.
fn select_probe_candidate(stdout: &str, required: (u64, u64, u64)) -> Option<RemoteBinary> {
    let mut candidates = parse_probe_candidates(stdout);
    if let Some(index) = candidates
        .iter()
        .position(|binary| binary.version.is_some_and(|version| version >= required))
    {
        return Some(candidates.remove(index));
    }
    candidates.into_iter().next()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePreference {
    PathFirst,
    InstallDirFirst,
}

/// POSIX-sh snippet that resolves a usable `hpc-compose` (on `PATH` or in the
/// install dir) and prints `<path>\t<version line>`, or `MISSING`. Wrap with
/// [`posix_sh`] before sending over SSH so it runs under `/bin/sh` rather than
/// the remote login shell (which may be csh/tcsh).
fn build_probe_command(preference: ProbePreference) -> String {
    let candidates = match preference {
        ProbePreference::PathFirst => "hpc-compose \"$install_path\"",
        ProbePreference::InstallDirFirst => "\"$install_path\" hpc-compose",
    };
    format!(
        "install_path=\"${{HPC_COMPOSE_INSTALL_DIR:-$HOME/.local/bin}}/hpc-compose\"; \
found=0; \
for c in {candidates}; do \
if p=\"$(command -v \"$c\" 2>/dev/null)\"; then \
printf '%s\\t%s\\n' \"$p\" \"$(\"$p\" --version 2>/dev/null)\"; found=1; \
fi; \
done; \
if [ \"$found\" -eq 0 ]; then echo MISSING; fi; \
exit 0"
    )
}

/// Shell command that installs the newest hpc-compose release (into
/// `~/.local/bin` by default) via the official installer.
fn build_install_command(url: &str) -> String {
    format!("curl -fsSL {} | sh", shell_quote::quote(url))
}

/// Wraps a POSIX-sh snippet so the remote login shell only sees a single
/// `/bin/sh -c '<snippet>'` token. The snippet contains no single quotes, so the
/// quoting survives even a csh/tcsh login shell's lexer.
fn posix_sh(snippet: &str) -> String {
    format!("/bin/sh -c {}", shell_quote::quote(snippet))
}

/// What to do with the login node's `hpc-compose`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteAction {
    Use,
    Install,
}

/// Decides what to do given the probe result and requested mode. Pure (no I/O).
fn remote_binary_action(
    mode: RemoteInstallMode,
    probe: Option<&RemoteBinary>,
    required: (u64, u64, u64),
) -> Result<RemoteAction> {
    let meets = |binary: &RemoteBinary| binary.version.is_some_and(|version| version >= required);
    match mode {
        RemoteInstallMode::Force => Ok(RemoteAction::Install),
        RemoteInstallMode::Auto => match probe {
            Some(binary) if meets(binary) => Ok(RemoteAction::Use),
            _ => Ok(RemoteAction::Install),
        },
        RemoteInstallMode::Never => match probe {
            Some(binary) if meets(binary) => Ok(RemoteAction::Use),
            Some(binary) => bail!(
                "remote hpc-compose on the login node is too old: found {}, need >= {}. \
                 Re-run without `--remote-install never` (auto upgrades it), or {}",
                binary
                    .version
                    .map(fmt_ver)
                    .unwrap_or_else(|| "unknown".to_string()),
                fmt_ver(required),
                manual_install_hint()
            ),
            None => bail!(
                "remote hpc-compose was not found on the login node (need >= {}). \
                 Re-run without `--remote-install never` (auto installs it), or {}",
                fmt_ver(required),
                manual_install_hint()
            ),
        },
    }
}

/// Whether a freshly-installed remote binary is still older than the client
/// requires. An unparseable (`None`) version is allowed through: the install
/// succeeded and the binary runs, so we cannot prove it is too old.
fn post_install_too_old(installed: Option<(u64, u64, u64)>, required: (u64, u64, u64)) -> bool {
    matches!(installed, Some(version) if version < required)
}

/// Probes the login node for a usable `hpc-compose`, reusing the multiplexed
/// SSH connection (so an OTP login node does not prompt again).
fn probe_remote_binary_with_preference(
    ssh_bin: &str,
    base_ssh_args: &[String],
    host: &str,
    preference: ProbePreference,
    required: (u64, u64, u64),
) -> Result<Option<RemoteBinary>> {
    let mut args = base_ssh_args.to_vec();
    args.push("--".to_string());
    args.push(host.to_string());
    args.push(posix_sh(&build_probe_command(preference)));
    let output = Command::new(ssh_bin)
        .args(&args)
        .output()
        .context("failed to run ssh while probing the remote hpc-compose version")?;
    // The probe snippet always exits 0 (it prints MISSING when nothing is found),
    // so a non-zero exit is a real SSH/shell failure — surface it instead of
    // silently treating the host as having no hpc-compose.
    if !output.status.success() {
        // Could not reach/probe the login node: an unreachable cluster is an
        // environment failure, so exit 3.
        return Err(crate::exit::EnvironmentError::new(format!(
            "failed to probe the remote hpc-compose on {host}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    Ok(select_probe_candidate(
        &String::from_utf8_lossy(&output.stdout),
        required,
    ))
}

fn probe_remote_binary(
    ssh_bin: &str,
    base_ssh_args: &[String],
    host: &str,
    required: (u64, u64, u64),
) -> Result<Option<RemoteBinary>> {
    probe_remote_binary_with_preference(
        ssh_bin,
        base_ssh_args,
        host,
        ProbePreference::PathFirst,
        required,
    )
}

/// Installs the newest hpc-compose release on the login node, streaming the
/// installer's output (stdio inherited) over the multiplexed connection.
fn install_remote_binary(ssh_bin: &str, base_ssh_args: &[String], host: &str) -> Result<()> {
    let url = env::var(REMOTE_INSTALL_URL_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_INSTALL_URL.to_string());
    let mut args = base_ssh_args.to_vec();
    args.push("--".to_string());
    args.push(host.to_string());
    args.push(posix_sh(&build_install_command(&url)));
    let status = Command::new(ssh_bin)
        .args(&args)
        .status()
        .context("failed to run ssh while installing hpc-compose on the login node")?;
    if !status.success() {
        // The login node could not fetch the release (no outbound network): an
        // environment readiness failure, so exit 3.
        return Err(crate::exit::EnvironmentError::new(format!(
            "failed to install hpc-compose on {host} (no outbound network from the login node?); {}",
            manual_install_hint()
        ))
        .into());
    }
    Ok(())
}

/// Ensures the login node has a usable `hpc-compose` before the expensive rsync:
/// probes its version and, per `mode`, auto-installs the newest release when it
/// is missing or too old. Returns the absolute invocation path to delegate to
/// (so a `~/.local/bin` install that is not on the non-interactive PATH works).
fn ensure_remote_binary(
    ssh_bin: &str,
    base_ssh_args: &[String],
    host: &str,
    mode: RemoteInstallMode,
) -> Result<String> {
    let required = parse_version(env!("CARGO_PKG_VERSION")).unwrap_or((0, 0, 0));
    let probe = probe_remote_binary(ssh_bin, base_ssh_args, host, required)?;
    match remote_binary_action(mode, probe.as_ref(), required)? {
        RemoteAction::Use => {
            let binary = probe.expect("RemoteAction::Use implies a probed binary");
            eprintln!(
                "  remote hpc-compose: {}{}",
                binary.path,
                binary
                    .version
                    .map(|version| format!(" ({})", fmt_ver(version)))
                    .unwrap_or_default()
            );
            // Version-collision guard: an unreleased local build and the published
            // release share the same X.Y.Z, so the probe "Use"s a remote that may
            // lack the local tree's new spec fields/flags. The SemVer check cannot
            // see that, so warn (auto-install cannot fix it — the installer would
            // fetch the same published release).
            if local_build_is_unreleased() && binary.version == Some(required) {
                hpc_compose::diagnostics::warn(format!(
                    "your local hpc-compose is an unreleased build (uncommitted changes) but the login node runs the published {ver}; commands that use unreleased spec fields or flags may fail on the remote with an \"unknown field\" / \"unexpected argument\" error. To use them, build hpc-compose for the login node's OS/arch and copy it over the remote ~/.local/bin/hpc-compose.",
                    ver = fmt_ver(required)
                ));
            }
            Ok(binary.path)
        }
        RemoteAction::Install => {
            let reason = match &probe {
                None => "not found".to_string(),
                Some(binary) => format!(
                    "version {}",
                    binary
                        .version
                        .map(fmt_ver)
                        .unwrap_or_else(|| "unknown".to_string())
                ),
            };
            eprintln!(
                "{}",
                term::styled_dim(&format!(
                    "  remote hpc-compose: {reason}; installing newest release into ~/.local/bin"
                ))
            );
            install_remote_binary(ssh_bin, base_ssh_args, host)?;
            let installed = probe_remote_binary_with_preference(
                ssh_bin,
                base_ssh_args,
                host,
                ProbePreference::InstallDirFirst,
                required,
            )?
            .ok_or_else(|| {
                anyhow!(
                    "remote install did not produce a usable hpc-compose on {host}; {}",
                    manual_install_hint()
                )
            })?;
            // The original failure was a remote binary too old to know `up`. If
            // auto-install still leaves it older than this client, fail hard
            // rather than delegate to a binary that may reject the subcommand.
            if post_install_too_old(installed.version, required) {
                bail!(
                    "remote install produced hpc-compose {} on {host}, but this client needs \
                     >= {} (the login node may pin an older release channel). {}",
                    installed
                        .version
                        .map(fmt_ver)
                        .unwrap_or_else(|| "unknown".to_string()),
                    fmt_ver(required),
                    manual_install_hint()
                );
            }
            eprintln!(
                "  installed remote hpc-compose: {}{}",
                installed.path,
                installed
                    .version
                    .map(|version| format!(" ({})", fmt_ver(version)))
                    .unwrap_or_default()
            );
            Ok(installed.path)
        }
    }
}

fn child_status_result(status: std::process::ExitStatus, signal_message: &str) -> Result<()> {
    match status.code() {
        Some(0) => Ok(()),
        Some(code) => Err(ExitCodeError(code).into()),
        None => bail!("{signal_message}"),
    }
}

fn ensure_control_master_dir() -> Result<()> {
    let Some(home) = env::var_os("HOME") else {
        return Ok(());
    };
    let dir = PathBuf::from(home).join(".ssh");
    let existed = dir.exists();
    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create SSH ControlMaster directory {}",
            dir.display()
        )
    })?;
    #[cfg(unix)]
    if !existed {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to chmod {}", dir.display()))?;
    }
    Ok(())
}

/// Orchestrate the thin remote submit: resolve the destination, rsync the
/// project, then delegate `up` over SSH and propagate the remote exit code.
/// Lexically resolves `rel` against `base`, collapsing `.`/`..` without touching
/// the filesystem (the path may not exist on this host).
fn lexical_join(base: &Path, rel: &Path) -> PathBuf {
    let mut out = base.to_path_buf();
    for component in rel.components() {
        match component {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Whether a bind-mount `source` is a *relative* path that escapes the staged
/// project dir (so it would be missing from the rsync'd copy on the login node).
/// Absolute sources and still-interpolated (`${...}`) sources are left alone:
/// the former are typically cluster paths (workspace/scratch) outside the staged
/// tree by design, the latter cannot be judged without the runtime environment.
fn volume_source_escapes_stage(source: &str, base: &Path, project_dir: &Path) -> bool {
    let source = source.trim();
    if source.is_empty() || source.contains('$') {
        return false;
    }
    let path = Path::new(source);
    if path.is_absolute() {
        return false;
    }
    !lexical_join(base, path).starts_with(project_dir)
}

/// Warns when any service bind-mount uses a relative source that escapes the
/// staged project dir. Advisory only: a spec-load failure is ignored here because
/// the login-node delegation performs the authoritative validation.
fn warn_volumes_outside_stage(compose_file: &Path, project_dir: &Path) {
    let Ok(spec) = hpc_compose::spec::ComposeSpec::load(compose_file) else {
        return;
    };
    let base = compose_file.parent().unwrap_or(project_dir);
    let mut offenders = Vec::new();
    for (service_name, service) in &spec.services {
        for volume in &service.volumes {
            let source = volume.split(':').next().unwrap_or("");
            if volume_source_escapes_stage(source, base, project_dir) {
                offenders.push(format!("{service_name}: {}", source.trim()));
            }
        }
    }
    if !offenders.is_empty() {
        hpc_compose::diagnostics::warn(format!(
            "relative bind-mount source(s) escape the staged project '{}' and will be missing on the login node: {}. Move them under the project root, stage from the repo root, or mount an absolute cluster path instead.",
            project_dir.display(),
            offenders.join(", ")
        ));
    }
}

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

    let bare_host = resolve_remote_host(remote_flag, context.login_host.as_deref())?;
    let env_user = env::var(REMOTE_USER_ENV).ok();
    let host = apply_login_user(
        &bare_host,
        context.login_user.as_deref(),
        env_user.as_deref(),
    );
    validate_remote_destination(&host)?;
    let compose_file = context.compose_file.value.clone();
    let project_dir = remote_stage_root(context, &compose_file)?;
    // Warn when only a compose subdir is staged: without a repo-root
    // .hpc-compose/settings.toml the source tree above the compose file is hidden
    // from the remote job (a common --remote pitfall).
    if context.settings_base_dir.is_none() {
        let repo_root = repo_root_or_cwd(&project_dir);
        if repo_root != project_dir {
            eprintln!(
                "{}",
                term::styled_dim(&format!(
                    "  note: staging only '{}'; add a repo-root .hpc-compose/settings.toml (or run \
                     `hpc-compose setup` at the repo root) to stage the whole repo",
                    project_dir.display()
                ))
            );
        }
    }
    warn_volumes_outside_stage(&compose_file, &project_dir);
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
    let install_env = env::var(REMOTE_INSTALL_ENV).ok();
    let install_mode = resolve_install_mode(options.install_mode, install_env.as_deref());
    let dry_run = options.dry_run;
    let up_flags = forwarded_up_flags(options)?;
    let stage = remote_stage_path(&project_dir);
    ensure_control_master_dir()?;

    let extra_opts = parse_extra_ssh_opts(env::var(REMOTE_SSH_OPTS_ENV).ok().as_deref());
    // One option set for every connection this run makes (mkdir, probe, rsync,
    // delegate): multiplexing so an OTP login node authenticates once, plus
    // RemoteCommand=none/RequestTTY=no so an interactive Host alias does not
    // hijack the non-interactive commands.
    let base_ssh_args = build_base_ssh_args(&extra_opts);
    let ssh_bin = context.binaries.ssh.value.as_str();
    let rsync_bin = context.binaries.rsync.value.as_str();
    let ssh_command = format!("{} {}", ssh_bin, base_ssh_args.join(" "));
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
    mkdir_args.push("--".to_string());
    mkdir_args.push(host.clone());
    mkdir_args.push(format!("mkdir -p {}", shell_quote::quote(&stage)));
    let mkdir_status = Command::new(ssh_bin)
        .args(&mkdir_args)
        .status()
        .context("failed to run ssh")?;
    if !mkdir_status.success() {
        // The first ssh connection failed: the login node is unreachable, so
        // exit 3 rather than the generic 1.
        return Err(crate::exit::EnvironmentError::new(format!(
            "failed to create remote stage dir '{stage}' on {host}; check your ~/.ssh/config"
        ))
        .into());
    }

    // 2. With the ControlMaster connection already open (the mkdir prompted for
    // any OTP), probe the login node's hpc-compose and bootstrap/upgrade it
    // before the expensive rsync, so an old/missing binary fails fast and gets
    // fixed rather than surfacing a raw shell error after a multi-GB sync.
    let remote_bin = ensure_remote_binary(ssh_bin, &base_ssh_args, &host, install_mode)?;

    let rsync_status = Command::new(rsync_bin)
        .args(&rsync_args)
        .status()
        .context("failed to run rsync (is rsync installed on this host?)")?;
    if !rsync_status.success() {
        // The transport to the login node failed: exit 3.
        return Err(crate::exit::EnvironmentError::new(format!(
            "rsync to {host} failed; check the destination and your ~/.ssh/config"
        ))
        .into());
    }

    // 3. Delegate to the login node's hpc-compose, streaming output back.
    let remote_command =
        build_remote_command(&remote_bin, &stage, &spec_rel, &global_flags, &up_flags);
    eprintln!("  delegating: ssh {host} '{remote_command}'");
    let mut ssh_args = base_ssh_args.clone();
    ssh_args.push("--".to_string());
    ssh_args.push(host.clone());
    ssh_args.push(remote_command);
    let (ssh_status, submitted_job_id) = run_delegate_capturing_job_id(ssh_bin, &ssh_args)?;
    match child_status_result(
        ssh_status,
        "remote hpc-compose up was terminated by a signal",
    ) {
        Ok(()) => {
            // A real submission produced a Slurm job id above; point the user at
            // the remote-aware follow-ups so they do not need to know the internal
            // remote staging paths. Skipped for dry runs (no job was submitted).
            if !dry_run {
                eprintln!(
                    "{}",
                    term::styled_dim(&remote_followup_hints(
                        &host,
                        &display_compose_path(&compose_file),
                        submitted_job_id.as_deref(),
                    ))
                );
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Best-effort display path for a compose file in follow-up hints: relative to
/// the current directory when possible (so it is copy-pasteable), else absolute.
fn display_compose_path(compose_file: &Path) -> String {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| compose_file.strip_prefix(&cwd).ok().map(Path::to_path_buf))
        .unwrap_or_else(|| compose_file.to_path_buf())
        .display()
        .to_string()
}

/// The remote-aware follow-up command hints shown after `up --remote`, so the
/// metrics/logs/artifacts workflow stays laptop-native instead of requiring an
/// SSH into the remote staged checkout.
fn remote_followup_hints(host: &str, compose_display: &str, job_id: Option<&str>) -> String {
    // The real id when we captured it (copy-pasteable); otherwise a placeholder
    // the user fills from the `Submitted batch job N` line above.
    let target = job_id.unwrap_or("<job-id>");
    let line = |cmd: &str| {
        format!("  hpc-compose {cmd:<6} --remote={host} -f {compose_display} --job-id {target}")
    };
    format!(
        "Next (run these from your laptop):\n{}\n{}\n{}\n{}\n{}",
        line("status"),
        line("stats"),
        line("logs"),
        line("score"),
        line("pull"),
    )
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
            install_mode: RemoteInstallMode::Auto,
            metrics_overrides: MetricsOverrides::default(),
            prepare_verbose: false,
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
    fn resolve_remote_host_rejects_values_that_are_not_safe_ssh_operands() {
        for destination in [
            "-oProxyCommand=touch /tmp/pwned",
            "user@-oProxyCommand=touch",
            "login node",
            "login\nnode",
            "host/path",
            r"host\path",
            "user@@host",
            "@host",
            "user@",
            "host:2222",
            "[localhost]",
            "[]",
        ] {
            assert!(
                resolve_remote_host(destination, None).is_err(),
                "destination {destination:?} must be rejected before ssh/rsync"
            );
            assert!(
                resolve_remote_host("", Some(destination)).is_err(),
                "configured destination {destination:?} must be rejected before ssh/rsync"
            );
        }
    }

    #[test]
    fn resolve_remote_host_accepts_host_user_and_bracketed_ipv6_operands() {
        for destination in [
            "login01",
            "login-alias.example",
            "nicolas@login01",
            "[2001:db8::1]",
            "nicolas@[fe80::1%en0]",
        ] {
            assert_eq!(resolve_remote_host(destination, None).unwrap(), destination);
        }
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
            install_mode: RemoteInstallMode::Auto,
            metrics_overrides: MetricsOverrides::default(),
            prepare_verbose: false,
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

    fn remote_opts_with_metrics(metrics: MetricsOverrides) -> RemoteUpOptions {
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
            watch_mode: WatchMode::Line,
            hold_on_exit: HoldOnExit::Always,
            quiet: false,
            install_mode: RemoteInstallMode::Auto,
            metrics_overrides: metrics,
            prepare_verbose: false,
        }
    }

    #[test]
    fn forwarded_up_flags_forwards_metrics_overrides() {
        // --no-metrics forwards through up --remote.
        let disabled = forwarded_up_flags(remote_opts_with_metrics(MetricsOverrides {
            disable: true,
            ..Default::default()
        }))
        .unwrap();
        assert!(disabled.iter().any(|flag| flag == "--no-metrics"));
        assert!(!disabled.iter().any(|flag| flag == "--metrics-interval"));

        // --metrics-interval N forwards the interval as a separate argument.
        let interval = forwarded_up_flags(remote_opts_with_metrics(MetricsOverrides {
            disable: false,
            interval_seconds: Some(2),
        }))
        .unwrap();
        let position = interval
            .iter()
            .position(|flag| flag == "--metrics-interval")
            .expect("interval flag forwarded");
        assert_eq!(interval.get(position + 1).map(String::as_str), Some("2"));
        assert!(!interval.iter().any(|flag| flag == "--no-metrics"));

        // Default overrides forward neither flag.
        let none =
            forwarded_up_flags(remote_opts_with_metrics(MetricsOverrides::default())).unwrap();
        assert!(!none.iter().any(|flag| flag == "--no-metrics"));
        assert!(!none.iter().any(|flag| flag == "--metrics-interval"));
    }

    #[test]
    fn forwarded_up_flags_forwards_prepare_verbose() {
        let on = forwarded_up_flags(RemoteUpOptions {
            prepare_verbose: true,
            ..remote_options()
        })
        .unwrap();
        assert!(on.iter().any(|flag| flag == "--prepare-verbose"));
        // Off by default, so a normal remote run does not enable it.
        let off = forwarded_up_flags(remote_options()).unwrap();
        assert!(!off.iter().any(|flag| flag == "--prepare-verbose"));
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
            "hpc-compose",
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
            "cd '.hpc-compose-remote/specs' && 'hpc-compose' --settings-file '.hpc-compose/settings.toml' up -f 'configs/hello.yaml' --detach --format json"
        );
    }

    #[test]
    fn build_remote_command_uses_resolved_absolute_binary() {
        let cmd = build_remote_command(
            "/home/u/.local/bin/hpc-compose",
            ".hpc-compose-remote/specs",
            "hello.yaml",
            &[],
            &["--detach".to_string()],
        );
        assert_eq!(
            cmd,
            "cd '.hpc-compose-remote/specs' && '/home/u/.local/bin/hpc-compose' up -f 'hello.yaml' --detach"
        );
    }

    #[test]
    fn apply_login_user_prepends_user_and_respects_explicit_at() {
        assert_eq!(
            apply_login_user("haicore", Some("vy3326"), None),
            "vy3326@haicore"
        );
        // env user wins over settings user
        assert_eq!(
            apply_login_user("haicore", Some("settings"), Some("envuser")),
            "envuser@haicore"
        );
        // explicit user@host is left untouched
        assert_eq!(
            apply_login_user("real@haicore", Some("vy3326"), Some("envuser")),
            "real@haicore"
        );
        // no user configured anywhere -> bare host (ssh config decides)
        assert_eq!(apply_login_user("haicore", None, None), "haicore");
        assert_eq!(apply_login_user("haicore", Some("  "), Some("")), "haicore");
    }

    #[test]
    fn parse_version_reads_version_lines_and_bare_semvers() {
        assert_eq!(parse_version("hpc-compose 0.1.49"), Some((0, 1, 49)));
        assert_eq!(parse_version("0.1.49"), Some((0, 1, 49)));
        assert_eq!(parse_version("hpc-compose 1.2.3-rc1"), Some((1, 2, 3)));
        assert_eq!(parse_version("not a version"), None);
    }

    #[test]
    fn parse_probe_candidates_handles_missing_and_resolved() {
        assert_eq!(
            parse_probe_candidates("MISSING\n"),
            Vec::<RemoteBinary>::new()
        );
        assert_eq!(parse_probe_candidates("   \n"), Vec::<RemoteBinary>::new());
        let candidates =
            parse_probe_candidates("/home/u/.local/bin/hpc-compose\thpc-compose 0.1.49\n");
        let resolved = candidates.first().expect("resolved");
        assert_eq!(resolved.path, "/home/u/.local/bin/hpc-compose");
        assert_eq!(resolved.version, Some((0, 1, 49)));
        // path present but unparsable version
        let candidates = parse_probe_candidates("/bin/hpc-compose\tgarbage");
        let unknown = candidates.first().expect("present");
        assert_eq!(unknown.version, None);
    }

    #[test]
    fn select_probe_candidate_uses_installed_binary_when_path_candidate_is_old() {
        let selected = select_probe_candidate(
            "/usr/bin/hpc-compose\thpc-compose 0.1.40\n\
             /home/u/.local/bin/hpc-compose\thpc-compose 0.1.49\n",
            (0, 1, 49),
        )
        .expect("selected");

        assert_eq!(selected.path, "/home/u/.local/bin/hpc-compose");
        assert_eq!(selected.version, Some((0, 1, 49)));
    }

    #[test]
    fn select_probe_candidate_reports_first_candidate_when_none_are_new_enough() {
        let selected = select_probe_candidate(
            "/usr/bin/hpc-compose\thpc-compose 0.1.40\n\
             /home/u/.local/bin/hpc-compose\thpc-compose 0.1.45\n",
            (0, 1, 49),
        )
        .expect("selected");

        assert_eq!(selected.path, "/usr/bin/hpc-compose");
        assert_eq!(selected.version, Some((0, 1, 40)));
    }

    #[cfg(unix)]
    #[test]
    fn probe_command_reports_path_and_install_dir_candidates() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path_dir = tmpdir.path().join("path-bin");
        let home = tmpdir.path().join("home");
        let install_dir = home.join(".local/bin");
        fs::create_dir_all(&path_dir).expect("path bin");
        fs::create_dir_all(&install_dir).expect("install bin");

        let write_binary = |path: &Path, version: &str| {
            fs::write(path, format!("#!/bin/sh\necho 'hpc-compose {version}'\n"))
                .expect("write fake hpc-compose");
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))
                .expect("chmod fake binary");
        };
        write_binary(&path_dir.join("hpc-compose"), "0.1.40");
        write_binary(&install_dir.join("hpc-compose"), "0.1.49");

        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg(build_probe_command(ProbePreference::PathFirst))
            .env("HOME", &home)
            .env("PATH", &path_dir)
            .env_remove("HPC_COMPOSE_INSTALL_DIR")
            .output()
            .expect("run probe");
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
        let candidates = parse_probe_candidates(&stdout);
        assert_eq!(candidates.len(), 2, "probe stdout was {stdout:?}");

        let selected = select_probe_candidate(&stdout, (0, 1, 49)).expect("selected");
        assert_eq!(
            selected.path,
            install_dir.join("hpc-compose").display().to_string()
        );
        assert_eq!(selected.version, Some((0, 1, 49)));
    }

    #[test]
    fn resolve_install_mode_env_overrides_flag() {
        assert_eq!(
            resolve_install_mode(RemoteInstallMode::Auto, Some("never")),
            RemoteInstallMode::Never
        );
        assert_eq!(
            resolve_install_mode(RemoteInstallMode::Never, Some(" FORCE ")),
            RemoteInstallMode::Force
        );
        assert_eq!(
            resolve_install_mode(RemoteInstallMode::Force, Some("bogus")),
            RemoteInstallMode::Force
        );
        assert_eq!(
            resolve_install_mode(RemoteInstallMode::Auto, None),
            RemoteInstallMode::Auto
        );
    }

    #[test]
    fn remote_binary_action_decides_per_mode() {
        let required = (0, 1, 49);
        let ok = RemoteBinary {
            path: "/bin/hpc-compose".to_string(),
            version: Some((0, 1, 49)),
        };
        let old = RemoteBinary {
            path: "/bin/hpc-compose".to_string(),
            version: Some((0, 1, 40)),
        };
        // auto
        assert_eq!(
            remote_binary_action(RemoteInstallMode::Auto, Some(&ok), required).unwrap(),
            RemoteAction::Use
        );
        assert_eq!(
            remote_binary_action(RemoteInstallMode::Auto, Some(&old), required).unwrap(),
            RemoteAction::Install
        );
        assert_eq!(
            remote_binary_action(RemoteInstallMode::Auto, None, required).unwrap(),
            RemoteAction::Install
        );
        // force always installs
        assert_eq!(
            remote_binary_action(RemoteInstallMode::Force, Some(&ok), required).unwrap(),
            RemoteAction::Install
        );
        // never: use if adequate, else error
        assert_eq!(
            remote_binary_action(RemoteInstallMode::Never, Some(&ok), required).unwrap(),
            RemoteAction::Use
        );
        assert!(remote_binary_action(RemoteInstallMode::Never, Some(&old), required).is_err());
        assert!(remote_binary_action(RemoteInstallMode::Never, None, required).is_err());
    }

    #[test]
    fn post_install_too_old_only_flags_known_older_versions() {
        let required = (0, 1, 49);
        // A known-older install is rejected (fail hard instead of delegating).
        assert!(post_install_too_old(Some((0, 1, 40)), required));
        // Equal or newer is accepted.
        assert!(!post_install_too_old(Some((0, 1, 49)), required));
        assert!(!post_install_too_old(Some((0, 2, 0)), required));
        // Unparseable version is allowed through (cannot prove it is too old).
        assert!(!post_install_too_old(None, required));
    }

    #[test]
    fn remote_followup_hints_substitute_real_job_id_or_placeholder() {
        // A captured job id is embedded so the hints are copy-pasteable.
        let with_id = remote_followup_hints(
            "vy3326@haicore.scc.kit.edu",
            "hpc/uv.hpc-compose.yaml",
            Some("1653779"),
        );
        assert!(with_id.contains(
            "hpc-compose stats  --remote=vy3326@haicore.scc.kit.edu -f hpc/uv.hpc-compose.yaml --job-id 1653779"
        ));
        // All five follow-up commands are advertised.
        for cmd in ["status", "stats", "logs", "score", "pull"] {
            assert!(
                with_id.contains(&format!("hpc-compose {cmd}")),
                "missing {cmd} hint"
            );
        }
        // With no captured id, fall back to a fillable placeholder.
        let placeholder = remote_followup_hints("host", "c.yaml", None);
        assert!(placeholder.contains("--job-id <job-id>"));
    }

    #[test]
    fn extract_job_id_parses_slurm_submit_line() {
        assert_eq!(
            extract_job_id("Submitted batch job 1653779").as_deref(),
            Some("1653779")
        );
        assert_eq!(
            extract_job_id("  Submitted batch job 42 (cluster)").as_deref(),
            Some("42")
        );
        assert_eq!(extract_job_id("preparing artifacts"), None);
        assert_eq!(extract_job_id("Submitted batch job "), None);
    }

    #[test]
    fn build_base_ssh_args_puts_user_opts_first_then_defaults() {
        let args = build_base_ssh_args(&["-o".to_string(), "ControlPersist=1h".to_string()]);
        // User opts come first so they win ssh's first-value-wins precedence.
        assert_eq!(&args[0], "-o");
        assert_eq!(&args[1], "ControlPersist=1h");
        // Interactive-alias neutralizers and multiplexing defaults are present.
        assert!(args.windows(2).any(|w| w == ["-o", "RemoteCommand=none"]));
        assert!(args.windows(2).any(|w| w == ["-o", "RequestTTY=no"]));
        assert!(args.iter().any(|a| a == "ControlMaster=auto"));
        // The user's ControlPersist precedes the hardcoded one, so it is the
        // first value ssh obtains.
        let first_persist = args
            .iter()
            .position(|a| a.starts_with("ControlPersist="))
            .expect("a ControlPersist value");
        assert_eq!(args[first_persist], "ControlPersist=1h");
    }

    #[test]
    fn rewrite_followup_args_drops_remote_and_pins_spec_path() {
        // --remote=HOST is dropped; -f is rewritten to the stage-relative path;
        // everything else is forwarded verbatim.
        let raw = vec![
            "stats".to_string(),
            "--remote=vy3326@host".to_string(),
            "-f".to_string(),
            "hpc/local-compose.yaml".to_string(),
            "--job-id".to_string(),
            "123".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ];
        assert_eq!(
            rewrite_followup_args(&raw, "hpc/haicore/uv.hpc-compose.yaml", None),
            vec![
                "stats",
                "-f",
                "hpc/haicore/uv.hpc-compose.yaml",
                "--job-id",
                "123",
                "--format",
                "json",
            ]
        );

        // Bare `--remote` (require_equals: no value token) and `--file=VALUE`; the
        // next arg must not be swallowed.
        let raw = vec![
            "logs".to_string(),
            "--remote".to_string(),
            "--file=compose.yaml".to_string(),
            "--follow".to_string(),
        ];
        assert_eq!(
            rewrite_followup_args(&raw, "compose.yaml", None),
            vec!["logs", "-f", "compose.yaml", "--follow"]
        );

        // No -f at all: the staged spec path is appended so discovery is pinned.
        let raw = vec!["score".to_string(), "--remote=host".to_string()];
        assert_eq!(
            rewrite_followup_args(&raw, "x.yaml", None),
            vec!["score", "-f", "x.yaml"]
        );
    }

    #[test]
    fn rewrite_followup_args_rewrites_explicit_settings_file() {
        let raw = vec![
            "--settings-file".to_string(),
            "/Users/me/project/.hpc-compose/settings.toml".to_string(),
            "stats".to_string(),
            "--remote=login".to_string(),
            "-f".to_string(),
            "/Users/me/project/compose.yaml".to_string(),
        ];
        assert_eq!(
            rewrite_followup_args(&raw, "compose.yaml", Some(".hpc-compose/settings.toml")),
            vec![
                "--settings-file",
                ".hpc-compose/settings.toml",
                "stats",
                "-f",
                "compose.yaml",
            ]
        );

        let raw = vec![
            "logs".to_string(),
            "--settings-file=/tmp/local-settings.toml".to_string(),
            "--remote".to_string(),
        ];
        assert_eq!(
            rewrite_followup_args(&raw, "subdir/compose.yaml", Some("settings/cluster.toml")),
            vec![
                "logs",
                "--settings-file",
                "settings/cluster.toml",
                "-f",
                "subdir/compose.yaml",
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn child_status_result_preserves_nonzero_exit_code() {
        use std::os::unix::process::ExitStatusExt;

        let err = child_status_result(std::process::ExitStatus::from_raw(7 << 8), "terminated")
            .unwrap_err();
        let exit = err
            .downcast_ref::<ExitCodeError>()
            .expect("exit code survives anyhow");
        assert_eq!(exit.code(), 7);
    }

    #[test]
    fn volume_source_escapes_stage_flags_relative_escapes_only() {
        let project = Path::new("/repo/sub");
        let base = Path::new("/repo/sub");
        // In-tree relative sources are fine.
        assert!(!volume_source_escapes_stage("./data", base, project));
        assert!(!volume_source_escapes_stage("data/out", base, project));
        // Relative sources that climb out of the staged subset are flagged.
        assert!(volume_source_escapes_stage("../common", base, project));
        assert!(volume_source_escapes_stage("../../outside", base, project));
        // Absolute (cluster) paths and still-interpolated sources are left alone.
        assert!(!volume_source_escapes_stage(
            "/scratch/workspace",
            base,
            project
        ));
        assert!(!volume_source_escapes_stage(
            "${WORKSPACE}/data",
            base,
            project
        ));
        assert!(!volume_source_escapes_stage("", base, project));
    }

    #[test]
    fn build_probe_and_install_commands_are_posix() {
        let probe = build_probe_command(ProbePreference::PathFirst);
        assert!(probe.contains("for c in hpc-compose \"$install_path\""));
        assert!(probe.contains("HPC_COMPOSE_INSTALL_DIR"));
        assert!(probe.contains(".local/bin"));
        assert!(probe.contains("--version"));
        assert!(!probe.contains("exit 0; fi;"));
        assert!(probe.contains("MISSING"));
        let post_install_probe = build_probe_command(ProbePreference::InstallDirFirst);
        assert!(post_install_probe.contains("for c in \"$install_path\" hpc-compose"));
        assert_eq!(
            build_install_command("https://example.test/install.sh"),
            "curl -fsSL 'https://example.test/install.sh' | sh"
        );
        // The snippet is wrapped to run under /bin/sh, not the login shell.
        assert_eq!(posix_sh("echo hi"), "/bin/sh -c 'echo hi'");
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
        // Runtime state is excluded, but the parent settings dir stays mutable:
        // changed or removed config files must not be protected from rsync.
        assert!(
            !args
                .windows(2)
                .any(|w| w == ["--filter", "P .hpc-compose/"])
        );
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
        assert_eq!(args[args.len() - 3], "--");
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
