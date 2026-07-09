//! The `workspace` command group: status/allocate/extend/release against the
//! site's hpc-workspace (`ws_*`) tools. Thin orchestration over
//! [`hpc_compose::workspace`]; Phase 1 runs the tools locally (login node or
//! dev machine) — up/preflight integration and `--remote` come later.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{OutputFormat, WorkspaceToolArgs};
use hpc_compose::context::ResolvedContext;
use hpc_compose::job::scan_job_records;
use hpc_compose::workspace::{
    DEFAULT_STATE_PROFILE_KEY, DEFAULT_WORKSPACE_DURATION_DAYS, WorkspaceObservation,
    WorkspaceTools, allocate_workspace, extend_workspace, find_workspace, job_ids_blocking_release,
    load_workspace_state, observe_workspace, record_observation, release_workspace,
    save_workspace_state, workspace_state_path,
};
use serde::Serialize;

use crate::commands::confirm;
use crate::output::{OUTPUT_SCHEMA_VERSION, resolve_output_format};

/// `workspace status` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceStatusOutput {
    pub(crate) schema_version: u32,
    /// Selected settings profile; `None` when running without a profile.
    pub(crate) profile: Option<String>,
    /// Configured workspace name.
    pub(crate) name: String,
    /// Whether `ws_find` located the workspace.
    pub(crate) exists: bool,
    /// Workspace path from `ws_find`; `None` when it does not exist.
    pub(crate) path: Option<PathBuf>,
    /// Absolute expiry time (unix seconds) computed from `ws_list`'s
    /// remaining time; `None` when unavailable.
    pub(crate) expiry_epoch: Option<u64>,
    /// Raw remaining-time string from `ws_list`.
    pub(crate) remaining_display: Option<String>,
    /// Raw expiration-date string from `ws_list` (display fallback when the
    /// remaining time could not be parsed).
    pub(crate) expiry_display: Option<String>,
    /// Extensions still available per `ws_list`.
    pub(crate) extensions_remaining: Option<u32>,
    /// Persisted workspace state file refreshed by this command.
    pub(crate) state_path: PathBuf,
}

/// `workspace allocate` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceAllocateOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// True when the workspace already existed and `ws_allocate` was skipped.
    pub(crate) already_allocated: bool,
    /// Days passed to `ws_allocate`; `None` when it already existed.
    pub(crate) duration_days: Option<u32>,
    pub(crate) path: PathBuf,
    pub(crate) expiry_epoch: Option<u64>,
    pub(crate) remaining_display: Option<String>,
    pub(crate) expiry_display: Option<String>,
    pub(crate) extensions_remaining: Option<u32>,
    pub(crate) state_path: PathBuf,
}

/// `workspace extend` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceExtendOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// Days passed to `ws_extend`.
    pub(crate) days: u32,
    pub(crate) path: PathBuf,
    pub(crate) expiry_epoch: Option<u64>,
    pub(crate) remaining_display: Option<String>,
    pub(crate) expiry_display: Option<String>,
    pub(crate) extensions_remaining: Option<u32>,
    pub(crate) state_path: PathBuf,
}

/// `workspace release` JSON output (`--format json`).
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct WorkspaceReleaseOutput {
    pub(crate) schema_version: u32,
    pub(crate) profile: Option<String>,
    pub(crate) name: String,
    /// True when `ws_release` ran; false when there was nothing to release.
    pub(crate) released: bool,
    /// Path the workspace had before release; `None` when it did not exist.
    pub(crate) path: Option<PathBuf>,
    pub(crate) state_path: PathBuf,
}

fn tools_from_args(args: &WorkspaceToolArgs) -> WorkspaceTools {
    WorkspaceTools {
        ws_find: args.ws_find_bin.clone(),
        ws_allocate: args.ws_allocate_bin.clone(),
        ws_extend: args.ws_extend_bin.clone(),
        ws_release: args.ws_release_bin.clone(),
        ws_list: args.ws_list_bin.clone(),
    }
}

/// Resolves the configured workspace name (and settings duration) or fails
/// with an actionable pointer at the settings surface.
fn configured_workspace(context: &ResolvedContext) -> Result<(String, Option<u32>)> {
    let settings = context.workspace.as_ref();
    let name = settings
        .and_then(|workspace| workspace.name.as_deref())
        .map(str::trim)
        .filter(|name| !name.is_empty());
    let Some(name) = name else {
        let profile_hint = context.selected_profile.as_deref().unwrap_or("<name>");
        bail!(
            "no workspace is configured for this context; add a workspace block to \
             .hpc-compose/settings.toml, for example:\n\n  \
             [profiles.{profile_hint}.workspace]\n  name = \"hpc-compose-cache\"\n  \
             duration_days = 30\n\n(or [defaults.workspace] to share one across profiles) \
             — see the 'Manage Cluster Workspaces' docs page (docs/src/workspaces.md)"
        );
    };
    // The name is passed to the ws_* tools as a positional argument; a
    // leading '-' would be parsed as a flag by those tools instead.
    if name.starts_with('-') {
        bail!(
            "the configured workspace name '{name}' must not start with '-': it is passed to \
             the ws_* tools as a command-line argument and would be treated as a flag; fix the \
             `name` in the settings workspace block (.hpc-compose/settings.toml)"
        );
    }
    Ok((
        name.to_string(),
        settings.and_then(|workspace| workspace.duration_days),
    ))
}

/// Validates a resolved allocation/renewal duration: the hpc-workspace tools
/// need at least one day (the settings schema pins `duration_days` to a
/// minimum of 1; this enforces the same bound for CLI flags at runtime).
fn validated_days(days: u32, flag: &str) -> Result<u32> {
    if days == 0 {
        bail!(
            "workspace duration must be at least 1 day; got 0 — pass a positive {flag} or set \
             a positive `duration_days` in the settings workspace block"
        );
    }
    Ok(days)
}

fn state_profile_key(context: &ResolvedContext) -> String {
    context
        .selected_profile
        .clone()
        .unwrap_or_else(|| DEFAULT_STATE_PROFILE_KEY.to_string())
}

/// Refreshes the state file: records the observation under the profile key,
/// or removes the entry when the workspace no longer exists. Always rewrites
/// the file so stale content (corrupt or unknown-version files, which
/// [`load_workspace_state`] tolerates as empty) is replaced with a canonical
/// current-version snapshot.
fn refresh_state(
    state_path: &std::path::Path,
    profile_key: &str,
    observation: Option<&WorkspaceObservation>,
    now: u64,
) -> Result<()> {
    let mut state = load_workspace_state(state_path)?;
    match observation {
        Some(observation) => record_observation(&mut state, profile_key, observation, now),
        None => {
            state.profiles.remove(profile_key);
        }
    }
    save_workspace_state(state_path, &state)
}

fn print_expiry_lines(
    remaining_display: Option<&str>,
    expiry_display: Option<&str>,
    extensions_remaining: Option<u32>,
) {
    match (remaining_display, expiry_display) {
        (Some(remaining), Some(expiry)) => println!("remaining: {remaining} (expires {expiry})"),
        (Some(remaining), None) => println!("remaining: {remaining}"),
        (None, Some(expiry)) => println!("expires: {expiry}"),
        (None, None) => println!("remaining: unknown (ws_list unavailable or missing this entry)"),
    }
    if let Some(extensions) = extensions_remaining {
        println!("available extensions: {extensions}");
    }
}

fn print_json<T: Serialize>(output: &T, what: &str) -> Result<()> {
    println!(
        "{}",
        crate::output::to_pretty_json(output)
            .with_context(|| format!("failed to serialize {what} output"))?
    );
    Ok(())
}

pub(crate) fn status(
    context: ResolvedContext,
    tool_args: &WorkspaceToolArgs,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (name, _) = configured_workspace(&context)?;
    let tools = tools_from_args(tool_args);
    let now = crate::time_util::unix_timestamp_now();
    let observation = observe_workspace(&tools, &name, now)?;

    let state_path = workspace_state_path(context.settings_path.as_deref(), &context.cwd);
    let profile_key = state_profile_key(&context);
    refresh_state(&state_path, &profile_key, observation.as_ref(), now)?;

    let output = WorkspaceStatusOutput {
        schema_version: OUTPUT_SCHEMA_VERSION,
        profile: context.selected_profile.clone(),
        name: name.clone(),
        exists: observation.is_some(),
        path: observation.as_ref().map(|obs| obs.path.clone()),
        expiry_epoch: observation.as_ref().and_then(|obs| obs.expiry_epoch),
        remaining_display: observation
            .as_ref()
            .and_then(|obs| obs.remaining_display.clone()),
        expiry_display: observation
            .as_ref()
            .and_then(|obs| obs.expiry_display.clone()),
        extensions_remaining: observation
            .as_ref()
            .and_then(|obs| obs.extensions_remaining),
        state_path,
    };
    match resolve_output_format(format) {
        OutputFormat::Text => {
            println!("workspace: {}", output.name);
            if let Some(profile) = &output.profile {
                println!("profile: {profile}");
            }
            if let Some(path) = &output.path {
                println!("path: {}", path.display());
                print_expiry_lines(
                    output.remaining_display.as_deref(),
                    output.expiry_display.as_deref(),
                    output.extensions_remaining,
                );
            } else {
                println!(
                    "workspace '{}' does not exist yet; run `hpc-compose workspace allocate`",
                    output.name
                );
            }
            println!("state: {}", output.state_path.display());
        }
        OutputFormat::Json => print_json(&output, "workspace status")?,
    }
    Ok(())
}

pub(crate) fn allocate(
    context: ResolvedContext,
    duration_days: Option<u32>,
    tool_args: &WorkspaceToolArgs,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (name, settings_duration) = configured_workspace(&context)?;
    let tools = tools_from_args(tool_args);
    let now = crate::time_util::unix_timestamp_now();

    // Guarded allocation: ALWAYS ws_find first. Re-allocating an existing
    // workspace errors on some hpc-workspace versions and extends on others,
    // so ws_allocate only ever runs for a missing workspace.
    let (already_allocated, requested_days) = if find_workspace(&tools, &name)?.is_some() {
        (true, None)
    } else {
        let days = validated_days(
            duration_days
                .or(settings_duration)
                .unwrap_or(DEFAULT_WORKSPACE_DURATION_DAYS),
            "--duration-days",
        )?;
        allocate_workspace(&tools, &name, days)?;
        (false, Some(days))
    };

    let observation = observe_workspace(&tools, &name, now)?.with_context(|| {
        format!("ws_allocate reported success but ws_find still cannot locate workspace '{name}'")
    })?;
    let state_path = workspace_state_path(context.settings_path.as_deref(), &context.cwd);
    let profile_key = state_profile_key(&context);
    refresh_state(&state_path, &profile_key, Some(&observation), now)?;

    let output = WorkspaceAllocateOutput {
        schema_version: OUTPUT_SCHEMA_VERSION,
        profile: context.selected_profile.clone(),
        name: name.clone(),
        already_allocated,
        duration_days: requested_days,
        path: observation.path.clone(),
        expiry_epoch: observation.expiry_epoch,
        remaining_display: observation.remaining_display.clone(),
        expiry_display: observation.expiry_display.clone(),
        extensions_remaining: observation.extensions_remaining,
        state_path,
    };
    match resolve_output_format(format) {
        OutputFormat::Text => {
            if output.already_allocated {
                println!(
                    "workspace '{}' is already allocated at {}",
                    output.name,
                    output.path.display()
                );
            } else {
                println!(
                    "allocated workspace '{}' for {} days at {}",
                    output.name,
                    output.duration_days.unwrap_or_default(),
                    output.path.display()
                );
            }
            print_expiry_lines(
                output.remaining_display.as_deref(),
                output.expiry_display.as_deref(),
                output.extensions_remaining,
            );
            println!("state: {}", output.state_path.display());
        }
        OutputFormat::Json => print_json(&output, "workspace allocate")?,
    }
    Ok(())
}

pub(crate) fn extend(
    context: ResolvedContext,
    days: Option<u32>,
    tool_args: &WorkspaceToolArgs,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (name, settings_duration) = configured_workspace(&context)?;
    let tools = tools_from_args(tool_args);
    let now = crate::time_util::unix_timestamp_now();

    if find_workspace(&tools, &name)?.is_none() {
        bail!("workspace '{name}' does not exist; run `hpc-compose workspace allocate` first");
    }
    let days = validated_days(
        days.or(settings_duration)
            .unwrap_or(DEFAULT_WORKSPACE_DURATION_DAYS),
        "--days",
    )?;
    extend_workspace(&tools, &name, days)?;

    let observation = observe_workspace(&tools, &name, now)?.with_context(|| {
        format!("workspace '{name}' disappeared while extending (ws_find no longer locates it)")
    })?;
    let state_path = workspace_state_path(context.settings_path.as_deref(), &context.cwd);
    let profile_key = state_profile_key(&context);
    refresh_state(&state_path, &profile_key, Some(&observation), now)?;

    let output = WorkspaceExtendOutput {
        schema_version: OUTPUT_SCHEMA_VERSION,
        profile: context.selected_profile.clone(),
        name: name.clone(),
        days,
        path: observation.path.clone(),
        expiry_epoch: observation.expiry_epoch,
        remaining_display: observation.remaining_display.clone(),
        expiry_display: observation.expiry_display.clone(),
        extensions_remaining: observation.extensions_remaining,
        state_path,
    };
    match resolve_output_format(format) {
        OutputFormat::Text => {
            println!(
                "extended workspace '{}' by {} days ({})",
                output.name,
                output.days,
                output.path.display()
            );
            print_expiry_lines(
                output.remaining_display.as_deref(),
                output.expiry_display.as_deref(),
                output.extensions_remaining,
            );
            println!("state: {}", output.state_path.display());
        }
        OutputFormat::Json => print_json(&output, "workspace extend")?,
    }
    Ok(())
}

pub(crate) fn release(
    context: ResolvedContext,
    yes: bool,
    tool_args: &WorkspaceToolArgs,
    format: Option<OutputFormat>,
) -> Result<()> {
    let (name, _) = configured_workspace(&context)?;
    let tools = tools_from_args(tool_args);
    let now = crate::time_util::unix_timestamp_now();
    let state_path = workspace_state_path(context.settings_path.as_deref(), &context.cwd);
    let profile_key = state_profile_key(&context);

    let Some(path) = find_workspace(&tools, &name)? else {
        // Nothing to release: clear any stale state entry and report
        // idempotently instead of failing.
        refresh_state(&state_path, &profile_key, None, now)?;
        let output = WorkspaceReleaseOutput {
            schema_version: OUTPUT_SCHEMA_VERSION,
            profile: context.selected_profile.clone(),
            name: name.clone(),
            released: false,
            path: None,
            state_path,
        };
        match resolve_output_format(format) {
            OutputFormat::Text => {
                println!(
                    "workspace '{}' does not exist; nothing to release",
                    output.name
                );
            }
            OutputFormat::Json => print_json(&output, "workspace release")?,
        }
        return Ok(());
    };

    // Refuse while tracked jobs still keep cache or runtime state under the
    // workspace: releasing would strand their artifacts and runtime dirs.
    let records = scan_job_records(&context.compose_file.value)?;
    let blocking = job_ids_blocking_release(&records, &path);
    if !blocking.is_empty() {
        bail!(
            "refusing to release workspace '{name}' ({}): tracked job(s) {} keep cache or \
             runtime state under it; run `hpc-compose down --job-id <id>` (or `hpc-compose \
             clean`) for those jobs first",
            path.display(),
            blocking.join(", ")
        );
    }

    confirm::confirm_destructive_action(
        &format!(
            "release workspace '{name}' ({}) and everything stored inside it",
            path.display()
        ),
        yes,
    )?;
    release_workspace(&tools, &name)?;
    // The workspace is gone: the command must report success no matter what
    // happens to the (regenerable) state file, so state cleanup only warns.
    if let Err(err) = refresh_state(&state_path, &profile_key, None, now) {
        hpc_compose::diagnostics::warn(format!(
            "workspace '{name}' was released, but updating the workspace state file failed: {err:#}"
        ));
    }

    let output = WorkspaceReleaseOutput {
        schema_version: OUTPUT_SCHEMA_VERSION,
        profile: context.selected_profile.clone(),
        name: name.clone(),
        released: true,
        path: Some(path),
        state_path,
    };
    match resolve_output_format(format) {
        OutputFormat::Text => {
            println!(
                "released workspace '{}' ({})",
                output.name,
                output
                    .path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            );
            println!("state: {}", output.state_path.display());
        }
        OutputFormat::Json => print_json(&output, "workspace release")?,
    }
    Ok(())
}
