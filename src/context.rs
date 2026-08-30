//! Repo-adjacent settings and execution-context resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::dotenv::parse_dotenv_lines;

const SETTINGS_SCHEMA_VERSION: u32 = 1;
const SETTINGS_RELATIVE_PATH: &str = ".hpc-compose/settings.toml";
const MIN_WATCH_REFRESH_MS: u64 = 100;
const MIN_WATCH_METRICS_REFRESH_MS: u64 = 500;

const DEFAULT_COMPOSE_FILE: &str = "compose.yaml";
const DEFAULT_HUGGINGFACE_CLI_BIN: &str = "huggingface-cli";
const DEFAULT_ENROOT_BIN: &str = "enroot";
const DEFAULT_APPTAINER_BIN: &str = "apptainer";
const DEFAULT_SINGULARITY_BIN: &str = "singularity";
const DEFAULT_SALLOC_BIN: &str = "salloc";
const DEFAULT_SBATCH_BIN: &str = "sbatch";
const DEFAULT_SRUN_BIN: &str = "srun";
const DEFAULT_SCONTROL_BIN: &str = "scontrol";
const DEFAULT_SINFO_BIN: &str = "sinfo";
const DEFAULT_SQUEUE_BIN: &str = "squeue";
const DEFAULT_SACCT_BIN: &str = "sacct";
const DEFAULT_SSTAT_BIN: &str = "sstat";
const DEFAULT_SCANCEL_BIN: &str = "scancel";
const DEFAULT_SSHARE_BIN: &str = "sshare";
const DEFAULT_SPRIO_BIN: &str = "sprio";
const DEFAULT_SSH_BIN: &str = "ssh";
const DEFAULT_RSYNC_BIN: &str = "rsync";

/// Source that provided a resolved value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueSource {
    /// Explicit CLI argument.
    Cli,
    /// Active profile in settings.
    Profile,
    /// Shared defaults in settings.
    Defaults,
    /// Compose file adjacency (for example `.env`).
    Compose,
    /// Built-in fallback.
    Builtin,
    /// Process environment variable.
    ProcessEnv,
    /// A value resolved through the top-level `secrets:` block (file or env).
    /// Always treated as sensitive for redaction regardless of its name.
    Secret,
}

/// Collects the concrete values of interpolation variables resolved through
/// [`ValueSource::Secret`] for value-equality redaction.
#[must_use]
pub(crate) fn secret_value_set(
    vars: &BTreeMap<String, String>,
    sources: &BTreeMap<String, ValueSource>,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (key, source) in sources {
        if *source == ValueSource::Secret
            && let Some(value) = vars.get(key)
        {
            out.insert(value.clone());
        }
    }
    out
}

/// A resolved value and where it came from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ResolvedValue<T> {
    /// Final value.
    pub value: T,
    /// Source that won resolution.
    pub source: ValueSource,
}

/// Binary override settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BinaryOverrides {
    #[serde(default)]
    pub enroot: Option<String>,
    #[serde(default)]
    pub apptainer: Option<String>,
    #[serde(default)]
    pub singularity: Option<String>,
    #[serde(default)]
    pub salloc: Option<String>,
    #[serde(default)]
    pub sbatch: Option<String>,
    #[serde(default)]
    pub srun: Option<String>,
    #[serde(default)]
    pub scontrol: Option<String>,
    #[serde(default)]
    pub sinfo: Option<String>,
    #[serde(default)]
    pub squeue: Option<String>,
    #[serde(default)]
    pub sacct: Option<String>,
    #[serde(default)]
    pub sstat: Option<String>,
    #[serde(default)]
    pub scancel: Option<String>,
    #[serde(default)]
    pub sshare: Option<String>,
    #[serde(default)]
    pub sprio: Option<String>,
    /// Path to the `ssh` client used by the `up --remote` / follow-up delegation
    /// path (mkdir, probe, install, delegate) and the `reach --open` forward.
    /// Defaults to `ssh` on `PATH`.
    #[serde(default)]
    pub ssh: Option<String>,
    /// Path to `rsync`, used by `up --remote` to mirror the project to the login
    /// node. Defaults to `rsync` on `PATH`.
    #[serde(default)]
    pub rsync: Option<String>,
}

/// Cache path defaults in settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CacheSettings {
    #[serde(default)]
    pub dir: Option<String>,
    /// Directory enroot uses for its temporary extraction scratch
    /// (`ENROOT_TEMP_PATH`) during prepare-time image import. Defaults to
    /// `<cache_dir>/enroot/tmp` (on the shared cache); point it at fast
    /// node-local storage (e.g. `/tmp/$USER-hpc-compose-enroot`) when the shared
    /// cache filesystem causes `Stale file handle` errors during squashfs
    /// creation. The final image and layer cache stay under `cache_dir`.
    #[serde(default)]
    pub enroot_temp_dir: Option<String>,
}

/// Workspace lifecycle defaults in settings, consumed by the `workspace`
/// command group's hpc-workspace (`ws_find`/`ws_allocate`/`ws_extend`/
/// `ws_release`/`ws_list`) integration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSettings {
    /// Workspace name passed to the `ws_*` tools (for example
    /// `hpc-compose-cache`).
    #[serde(default)]
    pub name: Option<String>,
    /// Allocation/renewal duration in days for `ws_allocate` and the
    /// `ws_extend` default. Defaults to 30 at use sites when unset.
    #[serde(default)]
    pub duration_days: Option<u32>,
    /// Allocate a missing workspace automatically at submit time. Defined now
    /// so the settings surface is stable; used from Phase 2 (up/preflight
    /// integration).
    #[serde(default)]
    pub auto_allocate: Option<bool>,
    /// Extend the workspace automatically at submit time when it would expire
    /// too soon. Defined now so the settings surface is stable; used from
    /// Phase 2 (auto-extend at submit).
    #[serde(default)]
    pub auto_extend: Option<bool>,
    /// Warn when fewer than this many days of workspace lifetime remain
    /// (default 7 at use sites). Defined now so the settings surface is
    /// stable; used from Phase 2.
    #[serde(default)]
    pub warn_days_left: Option<u32>,
    /// Extra buffer in days added on top of a job's expected queue+run time
    /// when Phase 2 decides whether the workspace outlives the job (default 2
    /// at use sites). Defined now so the settings surface is stable; used
    /// from Phase 2.
    #[serde(default)]
    pub queue_buffer_days: Option<u32>,
}

/// Reusable Slurm resource defaults in settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceProfile {
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub qos: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub ntasks: Option<u32>,
    #[serde(default)]
    pub ntasks_per_node: Option<u32>,
    #[serde(default)]
    pub cpus_per_task: Option<u32>,
    #[serde(default)]
    pub mem: Option<String>,
    #[serde(default)]
    pub gres: Option<String>,
    #[serde(default)]
    pub gpus: Option<u32>,
    #[serde(default)]
    pub gpus_per_node: Option<u32>,
    #[serde(default)]
    pub gpus_per_task: Option<u32>,
    #[serde(default)]
    pub cpus_per_gpu: Option<u32>,
    #[serde(default)]
    pub mem_per_gpu: Option<String>,
    #[serde(default)]
    pub gpu_bind: Option<String>,
    #[serde(default)]
    pub cpu_bind: Option<String>,
    #[serde(default)]
    pub mem_bind: Option<String>,
    #[serde(default)]
    pub distribution: Option<String>,
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub constraint: Option<String>,
}

/// Shared defaults in settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SettingsDefaults {
    #[serde(default)]
    pub compose_file: Option<String>,
    #[serde(default)]
    pub env_files: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub binaries: BinaryOverrides,
    #[serde(default)]
    pub cache: CacheSettings,
    /// Shared hpc-workspace (`ws_*`) lifecycle defaults for the `workspace`
    /// command group. A profile's `workspace` block overrides these per field.
    #[serde(default)]
    pub workspace: Option<WorkspaceSettings>,
    /// SSH login host used as the `up --remote` delegation destination and shown
    /// in connection hints. May be a bare host, a `~/.ssh/config` alias, or
    /// `user@host`. Used to open the connection.
    #[serde(default)]
    pub login_host: Option<String>,
    /// SSH username applied to a bare [`Self::login_host`] (or bare `--remote`
    /// host) so the `up --remote` destination becomes `user@host`. Overridden by
    /// an explicit `user@` and by `HPC_COMPOSE_REMOTE_USER`.
    #[serde(default)]
    pub login_user: Option<String>,
}

/// One named profile in settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SettingsProfile {
    #[serde(default)]
    pub compose_file: Option<String>,
    #[serde(default)]
    pub env_files: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub binaries: BinaryOverrides,
    #[serde(default)]
    pub cache: CacheSettings,
    /// hpc-workspace (`ws_*`) lifecycle settings for the `workspace` command
    /// group. Overrides the shared `defaults.workspace` block per field.
    #[serde(default)]
    pub workspace: Option<WorkspaceSettings>,
    /// SSH login host used as the `up --remote` delegation destination and shown
    /// in connection hints. May be a bare host, a `~/.ssh/config` alias, or
    /// `user@host`. Overrides the shared default. Used to open the connection.
    #[serde(default)]
    pub login_host: Option<String>,
    /// SSH username applied to a bare login host for `up --remote` (destination
    /// becomes `user@host`). Overrides the shared default.
    #[serde(default)]
    pub login_user: Option<String>,
}

/// Watch/replay TUI display preferences in settings.
///
/// Values are stored as primitives (parsed by the watch UI) so this lib type
/// stays decoupled from the binary's TUI enums. Environment variables and CLI
/// flags still take precedence over these defaults.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WatchSettings {
    /// Service ordering: `spec` or `triage`.
    #[serde(default)]
    pub sort: Option<String>,
    /// Wrap long log lines instead of truncating.
    #[serde(default)]
    pub wrap: Option<bool>,
    /// Scheduler/log refresh cadence in milliseconds.
    #[serde(default)]
    pub refresh_ms: Option<u64>,
    /// Metrics refresh cadence in milliseconds.
    #[serde(default)]
    pub metrics_refresh_ms: Option<u64>,
    /// Enable mouse capture (scroll-wheel log scrolling).
    #[serde(default)]
    pub mouse: Option<bool>,
}

/// `.hpc-compose/settings.toml` root schema.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default = "default_settings_schema_version")]
    pub version: u32,
    #[serde(default)]
    pub default_profile: Option<String>,
    #[serde(default)]
    pub defaults: SettingsDefaults,
    #[serde(default)]
    pub profiles: BTreeMap<String, SettingsProfile>,
    #[serde(default)]
    pub resource_profiles: BTreeMap<String, ResourceProfile>,
    #[serde(default)]
    pub watch: WatchSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: SETTINGS_SCHEMA_VERSION,
            default_profile: None,
            defaults: SettingsDefaults::default(),
            profiles: BTreeMap::new(),
            resource_profiles: BTreeMap::new(),
            watch: WatchSettings::default(),
        }
    }
}

fn default_settings_schema_version() -> u32 {
    SETTINGS_SCHEMA_VERSION
}

fn validate_watch_settings(settings: &WatchSettings) -> Result<()> {
    if let Some(sort) = settings.sort.as_deref()
        && !matches!(sort, "spec" | "triage")
    {
        bail!("invalid [watch].sort '{sort}'; expected 'spec' or 'triage'");
    }
    if let Some(refresh_ms) = settings.refresh_ms
        && refresh_ms < MIN_WATCH_REFRESH_MS
    {
        bail!("invalid [watch].refresh_ms {refresh_ms}; expected at least {MIN_WATCH_REFRESH_MS}");
    }
    if let Some(refresh_ms) = settings.metrics_refresh_ms
        && refresh_ms < MIN_WATCH_METRICS_REFRESH_MS
    {
        bail!(
            "invalid [watch].metrics_refresh_ms {refresh_ms}; expected at least {MIN_WATCH_METRICS_REFRESH_MS}"
        );
    }
    Ok(())
}

/// Fully resolved binaries.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ResolvedBinaries {
    pub enroot: ResolvedValue<String>,
    pub apptainer: ResolvedValue<String>,
    pub singularity: ResolvedValue<String>,
    pub salloc: ResolvedValue<String>,
    pub sbatch: ResolvedValue<String>,
    pub srun: ResolvedValue<String>,
    pub scontrol: ResolvedValue<String>,
    pub sinfo: ResolvedValue<String>,
    pub squeue: ResolvedValue<String>,
    pub sacct: ResolvedValue<String>,
    pub sstat: ResolvedValue<String>,
    pub scancel: ResolvedValue<String>,
    pub sshare: ResolvedValue<String>,
    pub sprio: ResolvedValue<String>,
    /// `ssh` client for the remote delegation path (`up --remote`, follow-ups,
    /// `reach --open`). Overridable via the settings `binaries` block; defaults
    /// to bare `ssh` on `PATH`.
    pub ssh: ResolvedValue<String>,
    /// `rsync` used by `up --remote` to mirror the project to the login node.
    /// Overridable via the settings `binaries` block; defaults to bare `rsync`.
    pub rsync: ResolvedValue<String>,
}

/// Workspace lifecycle settings after resolution: each field takes the
/// profile's `workspace` value when set, falling back to the shared
/// `defaults.workspace` value (mirroring the `login_host` precedence).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedWorkspaceSettings {
    /// Workspace name passed to the `ws_*` tools.
    #[serde(default)]
    pub name: Option<String>,
    /// Allocation/renewal duration in days (default 30 at use sites).
    #[serde(default)]
    pub duration_days: Option<u32>,
    /// Auto-allocate at submit time (used from Phase 2).
    #[serde(default)]
    pub auto_allocate: Option<bool>,
    /// Auto-extend at submit time (used from Phase 2).
    #[serde(default)]
    pub auto_extend: Option<bool>,
    /// Remaining-lifetime warning threshold in days (used from Phase 2).
    #[serde(default)]
    pub warn_days_left: Option<u32>,
    /// Queue-time buffer in days for expiry checks (used from Phase 2).
    #[serde(default)]
    pub queue_buffer_days: Option<u32>,
}

/// Effective context used to execute commands.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedContext {
    pub cwd: PathBuf,
    pub settings_path: Option<PathBuf>,
    pub settings_base_dir: Option<PathBuf>,
    pub selected_profile: Option<String>,
    pub compose_file: ResolvedValue<PathBuf>,
    pub cache_dir: ResolvedValue<PathBuf>,
    /// SSH login host used as the `up --remote` delegation destination and shown
    /// in connection hints. Profile overrides defaults; `None` when unset.
    #[serde(default)]
    pub login_host: Option<String>,
    /// SSH username applied to a bare [`Self::login_host`] for `up --remote`.
    /// Profile overrides defaults; overridden by an explicit `user@` and by
    /// `HPC_COMPOSE_REMOTE_USER`. `None` when unset.
    #[serde(default)]
    pub login_user: Option<String>,
    /// Override for enroot's temporary extraction scratch directory used during
    /// prepare-time image import. `None` falls back to `<cache_dir>/enroot/tmp`
    /// on the shared cache.
    #[serde(default)]
    pub enroot_temp_dir: Option<String>,
    /// hpc-workspace (`ws_*`) lifecycle settings for the `workspace` command
    /// group. Field-wise profile-over-defaults merge; `None` when neither the
    /// profile nor the shared defaults define a `workspace` block.
    #[serde(default)]
    pub workspace: Option<ResolvedWorkspaceSettings>,
    pub resource_profiles: BTreeMap<String, ResourceProfile>,
    pub binaries: ResolvedBinaries,
    /// `huggingface-cli` path used by `hf://` stage-in, executed cluster-side
    /// inside the Slurm allocation. Sourced from the `--huggingface-cli-bin`
    /// flag; defaults to `huggingface-cli`. Not a laptop-probed binary, so it is
    /// kept out of [`ResolvedBinaries`] / settings `binaries`.
    #[serde(default = "default_huggingface_cli_bin")]
    pub huggingface_cli_bin: String,
    pub interpolation_vars: BTreeMap<String, String>,
    pub interpolation_var_sources: BTreeMap<String, ValueSource>,
    /// Watch/replay TUI display defaults from settings.
    #[serde(default)]
    pub watch: WatchSettings,
}

impl ResolvedContext {
    /// Returns the value-equality redaction set: every value resolved through
    /// the top-level `secrets:` block. Every output path redacts against this so
    /// a benign-keyed value that equals a declared secret is still hidden.
    #[must_use]
    pub fn secret_values(&self) -> BTreeSet<String> {
        secret_value_set(&self.interpolation_vars, &self.interpolation_var_sources)
    }
}

/// Inputs used when resolving a command context.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default)]
pub struct ResolveRequest {
    pub cwd: PathBuf,
    pub profile: Option<String>,
    pub settings_file: Option<PathBuf>,
    pub compose_file_override: Option<PathBuf>,
    pub binary_overrides: BinaryOverrides,
    /// Explicit `--huggingface-cli-bin` override, when set on the command line.
    pub huggingface_cli_bin: Option<String>,
}

/// Built-in default for the `huggingface-cli` used by `hf://` stage-in.
#[must_use]
pub fn default_huggingface_cli_bin() -> String {
    DEFAULT_HUGGINGFACE_CLI_BIN.to_string()
}

impl ResolveRequest {
    /// Builds a request rooted at the current process directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the process working directory cannot be read.
    pub fn from_current_dir() -> Result<Self> {
        Ok(Self {
            cwd: env::current_dir().context("failed to determine current working directory")?,
            ..Self::default()
        })
    }
}

/// Resolves `.hpc-compose/settings.toml` by searching upward from `start`.
#[must_use]
pub fn discover_settings_path(start: &Path) -> Option<PathBuf> {
    for dir in start.ancestors() {
        let candidate = dir.join(SETTINGS_RELATIVE_PATH);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Returns `<repo-root-or-cwd>/.hpc-compose/settings.toml`.
#[must_use]
pub fn repo_adjacent_settings_path(start: &Path) -> PathBuf {
    repo_root_or_cwd(start).join(SETTINGS_RELATIVE_PATH)
}

/// Detects the nearest git root from `start`, or returns `start`.
///
/// This public compatibility wrapper preserves the historical function-item
/// provenance while delegating the path policy to its crate-private owner.
#[must_use]
pub fn repo_root_or_cwd(start: &Path) -> PathBuf {
    crate::path_util::repo_root_or_cwd(start)
}

/// Loads settings if a path exists.
///
/// # Errors
///
/// Returns an error when the file cannot be parsed, has an unsupported schema
/// version, or contains invalid watch preferences.
pub fn load_settings(path: &Path) -> Result<Settings> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read settings file {}", path.display()))?;
    let settings: Settings = toml::from_str(&raw)
        .with_context(|| format!("failed to parse settings file {}", path.display()))?;
    if settings.version != SETTINGS_SCHEMA_VERSION {
        bail!(
            "unsupported settings schema version {}; expected {}",
            settings.version,
            SETTINGS_SCHEMA_VERSION
        );
    }
    validate_watch_settings(&settings.watch)?;
    Ok(settings)
}

/// Loads settings if `path` exists, otherwise returns `None`.
///
/// # Errors
///
/// Returns parsing or schema errors when a file exists but is invalid.
pub fn load_settings_if_exists(path: &Path) -> Result<Option<Settings>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(load_settings(path)?))
}

/// Writes settings to disk, creating parent directories as needed.
///
/// # Errors
///
/// Returns an error when the settings version or watch preferences are invalid,
/// or when serialization or file writes fail.
pub fn write_settings(path: &Path, settings: &Settings) -> Result<()> {
    if settings.version != SETTINGS_SCHEMA_VERSION {
        bail!(
            "refusing to write settings with unsupported schema version {}",
            settings.version
        );
    }
    validate_watch_settings(&settings.watch)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = toml::to_string_pretty(settings).context("failed to serialize settings")?;
    crate::secure_io::write_atomic(path, rendered.as_bytes(), true)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Resolves command-level context from settings, profile, and CLI overrides.
///
/// # Errors
///
/// Returns an error when settings parsing fails, a requested profile is
/// missing, or a referenced env file cannot be read.
pub fn resolve(request: &ResolveRequest) -> Result<ResolvedContext> {
    resolve_inner(request, None)
}

/// Resolves command context while reading the root compose document from an
/// in-memory overlay for secret interpolation.
///
/// This is intended for static authoring tools. Settings, `.env`, `env_file`,
/// `extends`, and secret files remain disk-backed.
///
/// # Errors
///
/// Returns an error when settings parsing fails, a requested profile is
/// missing, a referenced env file cannot be read, or a top-level secret source
/// in `compose_text` cannot be resolved.
pub fn resolve_with_compose_text(
    request: &ResolveRequest,
    compose_text: &str,
) -> Result<ResolvedContext> {
    resolve_inner(request, Some(compose_text))
}

struct SelectedSettings {
    path: Option<PathBuf>,
    settings: Option<Settings>,
    profile_name: Option<String>,
}

impl SelectedSettings {
    fn profile(&self) -> Option<&SettingsProfile> {
        let name = self.profile_name.as_ref()?;
        let settings = self
            .settings
            .as_ref()
            .expect("a selected profile must have loaded settings");
        Some(
            settings
                .profiles
                .get(name)
                .expect("the selected profile was validated during settings selection"),
        )
    }

    fn defaults(&self) -> Option<&SettingsDefaults> {
        self.settings.as_ref().map(|settings| &settings.defaults)
    }
}

fn select_settings(request: &ResolveRequest) -> Result<SelectedSettings> {
    let path = if let Some(path) = request.settings_file.as_ref() {
        if !path.exists() {
            bail!("settings file does not exist: {}", path.display());
        }
        Some(crate::path_util::absolute_path(path, &request.cwd))
    } else {
        discover_settings_path(&request.cwd)
    };

    let settings = match path.as_ref() {
        Some(path) => Some(load_settings(path)?),
        None => None,
    };

    let profile_name = request.profile.clone().or_else(|| {
        settings
            .as_ref()
            .and_then(|settings| settings.default_profile.clone())
    });
    match (settings.as_ref(), profile_name.as_ref()) {
        (Some(settings), Some(name)) => {
            settings
                .profiles
                .get(name)
                .with_context(|| format!("profile '{name}' is not defined in settings"))?;
        }
        (None, Some(name)) => {
            bail!(
                "profile '{}' was requested, but no settings file was found (expected {} in this repository tree)",
                name,
                SETTINGS_RELATIVE_PATH
            );
        }
        _ => {}
    }

    Ok(SelectedSettings {
        path,
        settings,
        profile_name,
    })
}

fn resolve_inner(
    request: &ResolveRequest,
    compose_text_override: Option<&str>,
) -> Result<ResolvedContext> {
    let selected = select_settings(request)?;
    let profile_cfg = selected.profile();
    let defaults_cfg = selected.defaults();

    let settings_base = selected
        .path
        .as_deref()
        .map(settings_base_dir)
        .unwrap_or_else(|| request.cwd.clone());

    let compose_file = resolve_compose_file(
        request.compose_file_override.as_deref(),
        profile_cfg.and_then(|profile| profile.compose_file.as_deref()),
        defaults_cfg.and_then(|defaults| defaults.compose_file.as_deref()),
        &request.cwd,
        &settings_base,
    );

    let binaries = resolve_binaries(
        &request.binary_overrides,
        profile_cfg.map(|profile| &profile.binaries),
        defaults_cfg.map(|defaults| &defaults.binaries),
    );
    let cache_dir = resolve_cache_dir_default(
        profile_cfg.and_then(|profile| profile.cache.dir.as_deref()),
        defaults_cfg.and_then(|defaults| defaults.cache.dir.as_deref()),
        &settings_base,
    );
    let login_host = resolve_login_host(
        profile_cfg.and_then(|profile| profile.login_host.as_deref()),
        defaults_cfg.and_then(|defaults| defaults.login_host.as_deref()),
    );
    let login_user = resolve_login_user(
        profile_cfg.and_then(|profile| profile.login_user.as_deref()),
        defaults_cfg.and_then(|defaults| defaults.login_user.as_deref()),
    );
    let enroot_temp_dir = profile_cfg
        .and_then(|profile| profile.cache.enroot_temp_dir.as_deref())
        .or_else(|| defaults_cfg.and_then(|defaults| defaults.cache.enroot_temp_dir.as_deref()))
        .map(str::to_string);
    let workspace = resolve_workspace_settings(
        profile_cfg.and_then(|profile| profile.workspace.as_ref()),
        defaults_cfg.and_then(|defaults| defaults.workspace.as_ref()),
    );

    let mut interpolation_vars = BTreeMap::new();
    let mut interpolation_var_sources = BTreeMap::new();
    load_compose_dotenv(
        &compose_file.value,
        &mut interpolation_vars,
        &mut interpolation_var_sources,
    )?;
    if let Some(defaults) = defaults_cfg {
        apply_settings_env_files(
            &defaults.env_files,
            &settings_base,
            ValueSource::Defaults,
            &mut interpolation_vars,
            &mut interpolation_var_sources,
        )?;
        apply_env_map(
            &defaults.env,
            ValueSource::Defaults,
            &mut interpolation_vars,
            &mut interpolation_var_sources,
        );
    }
    if let Some(profile) = profile_cfg {
        apply_settings_env_files(
            &profile.env_files,
            &settings_base,
            ValueSource::Profile,
            &mut interpolation_vars,
            &mut interpolation_var_sources,
        )?;
        apply_env_map(
            &profile.env,
            ValueSource::Profile,
            &mut interpolation_vars,
            &mut interpolation_var_sources,
        );
    }
    for (key, value) in env::vars() {
        interpolation_var_sources.insert(key.clone(), ValueSource::ProcessEnv);
        interpolation_vars.insert(key, value);
    }
    let resolved_secrets = match compose_text_override {
        Some(raw) => {
            resolve_compose_secrets_from_str(&compose_file.value, raw, &interpolation_vars)?
        }
        None => resolve_compose_secrets(&compose_file.value, &interpolation_vars)?,
    };
    for (name, value) in resolved_secrets {
        interpolation_var_sources.insert(name.clone(), ValueSource::Secret);
        interpolation_vars.insert(name, value);
    }

    let resolved_settings_base_dir = selected.path.as_deref().map(settings_base_dir);

    Ok(ResolvedContext {
        cwd: request.cwd.clone(),
        settings_path: selected.path.clone(),
        settings_base_dir: resolved_settings_base_dir,
        selected_profile: selected.profile_name.clone(),
        compose_file,
        cache_dir,
        login_host,
        login_user,
        enroot_temp_dir,
        workspace,
        resource_profiles: selected
            .settings
            .as_ref()
            .map(|settings| settings.resource_profiles.clone())
            .unwrap_or_default(),
        binaries,
        huggingface_cli_bin: request
            .huggingface_cli_bin
            .clone()
            .unwrap_or_else(default_huggingface_cli_bin),
        interpolation_vars,
        interpolation_var_sources,
        watch: selected
            .settings
            .as_ref()
            .map(|settings| settings.watch.clone())
            .unwrap_or_default(),
    })
}

/// Resolves only the effective binary paths from settings, profile, and CLI
/// overrides.
///
/// # Errors
///
/// Returns an error when settings parsing fails, a requested profile is
/// missing, or a requested settings file path does not exist.
pub fn resolve_binaries_only(request: &ResolveRequest) -> Result<ResolvedBinaries> {
    let selected = select_settings(request)?;
    let profile_cfg = selected.profile();
    let defaults_cfg = selected.defaults();
    Ok(resolve_binaries(
        &request.binary_overrides,
        profile_cfg.map(|profile| &profile.binaries),
        defaults_cfg.map(|defaults| &defaults.binaries),
    ))
}

fn resolve_compose_file(
    cli_override: Option<&Path>,
    profile_value: Option<&str>,
    defaults_value: Option<&str>,
    cwd: &Path,
    settings_base: &Path,
) -> ResolvedValue<PathBuf> {
    if let Some(path) = cli_override {
        return ResolvedValue {
            value: crate::path_util::absolute_path(path, cwd),
            source: ValueSource::Cli,
        };
    }
    if let Some(path) = profile_value {
        return ResolvedValue {
            value: resolve_string_path(path, settings_base),
            source: ValueSource::Profile,
        };
    }
    if let Some(path) = defaults_value {
        return ResolvedValue {
            value: resolve_string_path(path, settings_base),
            source: ValueSource::Defaults,
        };
    }
    ResolvedValue {
        value: crate::path_util::absolute_path(Path::new(DEFAULT_COMPOSE_FILE), cwd),
        source: ValueSource::Builtin,
    }
}

fn resolve_binaries(
    cli: &BinaryOverrides,
    profile: Option<&BinaryOverrides>,
    defaults: Option<&BinaryOverrides>,
) -> ResolvedBinaries {
    ResolvedBinaries {
        enroot: resolve_binary(
            cli.enroot.clone(),
            profile.and_then(|p| p.enroot.clone()),
            defaults.and_then(|d| d.enroot.clone()),
            DEFAULT_ENROOT_BIN,
        ),
        apptainer: resolve_binary(
            cli.apptainer.clone(),
            profile.and_then(|p| p.apptainer.clone()),
            defaults.and_then(|d| d.apptainer.clone()),
            DEFAULT_APPTAINER_BIN,
        ),
        singularity: resolve_binary(
            cli.singularity.clone(),
            profile.and_then(|p| p.singularity.clone()),
            defaults.and_then(|d| d.singularity.clone()),
            DEFAULT_SINGULARITY_BIN,
        ),
        salloc: resolve_binary(
            cli.salloc.clone(),
            profile.and_then(|p| p.salloc.clone()),
            defaults.and_then(|d| d.salloc.clone()),
            DEFAULT_SALLOC_BIN,
        ),
        sbatch: resolve_binary(
            cli.sbatch.clone(),
            profile.and_then(|p| p.sbatch.clone()),
            defaults.and_then(|d| d.sbatch.clone()),
            DEFAULT_SBATCH_BIN,
        ),
        srun: resolve_binary(
            cli.srun.clone(),
            profile.and_then(|p| p.srun.clone()),
            defaults.and_then(|d| d.srun.clone()),
            DEFAULT_SRUN_BIN,
        ),
        scontrol: resolve_binary(
            cli.scontrol.clone(),
            profile.and_then(|p| p.scontrol.clone()),
            defaults.and_then(|d| d.scontrol.clone()),
            DEFAULT_SCONTROL_BIN,
        ),
        sinfo: resolve_binary(
            cli.sinfo.clone(),
            profile.and_then(|p| p.sinfo.clone()),
            defaults.and_then(|d| d.sinfo.clone()),
            DEFAULT_SINFO_BIN,
        ),
        squeue: resolve_binary(
            cli.squeue.clone(),
            profile.and_then(|p| p.squeue.clone()),
            defaults.and_then(|d| d.squeue.clone()),
            DEFAULT_SQUEUE_BIN,
        ),
        sacct: resolve_binary(
            cli.sacct.clone(),
            profile.and_then(|p| p.sacct.clone()),
            defaults.and_then(|d| d.sacct.clone()),
            DEFAULT_SACCT_BIN,
        ),
        sstat: resolve_binary(
            cli.sstat.clone(),
            profile.and_then(|p| p.sstat.clone()),
            defaults.and_then(|d| d.sstat.clone()),
            DEFAULT_SSTAT_BIN,
        ),
        scancel: resolve_binary(
            cli.scancel.clone(),
            profile.and_then(|p| p.scancel.clone()),
            defaults.and_then(|d| d.scancel.clone()),
            DEFAULT_SCANCEL_BIN,
        ),
        sshare: resolve_binary(
            cli.sshare.clone(),
            profile.and_then(|p| p.sshare.clone()),
            defaults.and_then(|d| d.sshare.clone()),
            DEFAULT_SSHARE_BIN,
        ),
        sprio: resolve_binary(
            cli.sprio.clone(),
            profile.and_then(|p| p.sprio.clone()),
            defaults.and_then(|d| d.sprio.clone()),
            DEFAULT_SPRIO_BIN,
        ),
        ssh: resolve_binary(
            cli.ssh.clone(),
            profile.and_then(|p| p.ssh.clone()),
            defaults.and_then(|d| d.ssh.clone()),
            DEFAULT_SSH_BIN,
        ),
        rsync: resolve_binary(
            cli.rsync.clone(),
            profile.and_then(|p| p.rsync.clone()),
            defaults.and_then(|d| d.rsync.clone()),
            DEFAULT_RSYNC_BIN,
        ),
    }
}

fn resolve_binary(
    cli: Option<String>,
    profile: Option<String>,
    defaults: Option<String>,
    builtin: &str,
) -> ResolvedValue<String> {
    if let Some(value) = cli {
        return ResolvedValue {
            value,
            source: ValueSource::Cli,
        };
    }
    if let Some(value) = profile {
        return ResolvedValue {
            value,
            source: ValueSource::Profile,
        };
    }
    if let Some(value) = defaults {
        return ResolvedValue {
            value,
            source: ValueSource::Defaults,
        };
    }
    ResolvedValue {
        value: builtin.to_string(),
        source: ValueSource::Builtin,
    }
}

fn resolve_cache_dir_default(
    profile_value: Option<&str>,
    defaults_value: Option<&str>,
    settings_base: &Path,
) -> ResolvedValue<PathBuf> {
    if let Some(path) = profile_value {
        return ResolvedValue {
            value: resolve_string_path(path, settings_base),
            source: ValueSource::Profile,
        };
    }
    if let Some(path) = defaults_value {
        return ResolvedValue {
            value: resolve_string_path(path, settings_base),
            source: ValueSource::Defaults,
        };
    }
    ResolvedValue {
        value: crate::path_util::default_cache_dir(),
        source: ValueSource::Builtin,
    }
}

/// Resolves the login/jump host shown in connection hints: profile value wins
/// over the shared default; `None` when neither is set. Descriptive only.
fn resolve_login_host(profile_value: Option<&str>, defaults_value: Option<&str>) -> Option<String> {
    profile_value.or(defaults_value).map(str::to_string)
}

/// Resolves the SSH login username applied to a bare `up --remote` host: profile
/// value wins over the shared default; `None` when neither is set.
fn resolve_login_user(profile_value: Option<&str>, defaults_value: Option<&str>) -> Option<String> {
    profile_value.or(defaults_value).map(str::to_string)
}

/// Resolves workspace lifecycle settings field-wise: each field takes the
/// profile value when set, else the shared default. Returns `None` only when
/// neither layer defines a `workspace` block at all.
fn resolve_workspace_settings(
    profile_value: Option<&WorkspaceSettings>,
    defaults_value: Option<&WorkspaceSettings>,
) -> Option<ResolvedWorkspaceSettings> {
    if profile_value.is_none() && defaults_value.is_none() {
        return None;
    }
    fn pick<T: Clone>(
        profile: Option<&WorkspaceSettings>,
        defaults: Option<&WorkspaceSettings>,
        field: impl Fn(&WorkspaceSettings) -> Option<T>,
    ) -> Option<T> {
        profile
            .and_then(&field)
            .or_else(|| defaults.and_then(&field))
    }
    Some(ResolvedWorkspaceSettings {
        name: pick(profile_value, defaults_value, |w| w.name.clone()),
        duration_days: pick(profile_value, defaults_value, |w| w.duration_days),
        auto_allocate: pick(profile_value, defaults_value, |w| w.auto_allocate),
        auto_extend: pick(profile_value, defaults_value, |w| w.auto_extend),
        warn_days_left: pick(profile_value, defaults_value, |w| w.warn_days_left),
        queue_buffer_days: pick(profile_value, defaults_value, |w| w.queue_buffer_days),
    })
}

fn load_compose_dotenv(
    compose_file: &Path,
    vars: &mut BTreeMap<String, String>,
    sources: &mut BTreeMap<String, ValueSource>,
) -> Result<()> {
    let compose_dir = compose_file
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let compose_dotenv = compose_dir.join(".env");
    if compose_dotenv.exists() {
        let parsed = parse_env_file(&compose_dotenv)?;
        for (key, value) in parsed {
            vars.insert(key.clone(), value);
            sources.insert(key, ValueSource::Compose);
        }
    }
    Ok(())
}

fn apply_settings_env_files(
    paths: &[String],
    settings_base: &Path,
    source: ValueSource,
    vars: &mut BTreeMap<String, String>,
    sources: &mut BTreeMap<String, ValueSource>,
) -> Result<()> {
    for raw in paths {
        let path = resolve_string_path(raw, settings_base);
        if !path.exists() {
            bail!("settings env file does not exist: {}", path.display());
        }
        let parsed = parse_env_file(&path)?;
        for (key, value) in parsed {
            vars.insert(key.clone(), value);
            sources.insert(key, source);
        }
    }
    Ok(())
}

fn apply_env_map(
    map: &BTreeMap<String, String>,
    source: ValueSource,
    vars: &mut BTreeMap<String, String>,
    sources: &mut BTreeMap<String, ValueSource>,
) {
    for (key, value) in map {
        vars.insert(key.clone(), value.clone());
        sources.insert(key.clone(), source);
    }
}

/// Resolves the compose file's top-level `secrets:` block into a map of
/// name → value, so the caller can merge them tagged [`ValueSource::Secret`].
///
/// `file:` sources are read relative to the compose file directory; `env:`
/// sources read the named variable from `lookup_vars` (the interpolation map
/// built so far, which already includes process env).
fn resolve_compose_secrets(
    compose_file: &Path,
    lookup_vars: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    // Tolerate spec parse failures here: a broken spec is reported by the
    // normal load path, and secrets cannot be resolved without a parseable
    // file. Only secret-specific errors (missing file/env) propagate.
    let Ok(secrets) = hpc_compose::spec::ComposeSpec::load_secrets(compose_file) else {
        return Ok(BTreeMap::new());
    };
    resolve_secret_specs(compose_file, &secrets, lookup_vars)
}

fn resolve_compose_secrets_from_str(
    compose_file: &Path,
    raw: &str,
    lookup_vars: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    // Mirror the disk-backed path: if the open buffer is not parseable yet,
    // leave the normal spec loader to report that error and skip secrets here.
    let Ok(secrets) = hpc_compose::spec::ComposeSpec::load_secrets_from_str(compose_file, raw)
    else {
        return Ok(BTreeMap::new());
    };
    resolve_secret_specs(compose_file, &secrets, lookup_vars)
}

fn resolve_secret_specs(
    compose_file: &Path,
    secrets: &BTreeMap<String, hpc_compose::spec::SecretSpec>,
    lookup_vars: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let compose_dir = compose_file
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let mut resolved = BTreeMap::new();
    for (name, spec) in secrets {
        let value = if let Some(file_rel) = &spec.file {
            let path = resolve_string_path(file_rel, &compose_dir);
            let raw = fs::read_to_string(&path).with_context(|| {
                format!(
                    "secret '{name}' file '{}' could not be read",
                    path.display()
                )
            })?;
            raw.trim().to_string()
        } else if let Some(env_name) = &spec.env {
            lookup_vars
                .get(env_name)
                .with_context(|| {
                    format!("secret '{name}' references env var '{env_name}' which is not set")
                })?
                .clone()
        } else {
            // SecretSpec::validate enforces one-of; unreachable for a valid spec.
            continue;
        };
        resolved.insert(name.clone(), value);
    }
    Ok(resolved)
}

fn settings_base_dir(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    if parent.file_name() == Some(OsStr::new(".hpc-compose")) {
        return parent
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
    }
    parent.to_path_buf()
}

fn resolve_string_path(value: &str, base: &Path) -> PathBuf {
    let expanded = shellexpand::tilde(value).to_string();
    let raw = PathBuf::from(expanded);
    crate::path_util::absolute_path(&raw, base)
}

fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read env file {}", path.display()))?;
    parse_dotenv_lines(&raw).map_err(|error| {
        anyhow::anyhow!(
            "failed to parse {}: line {} {}",
            path.display(),
            error.line,
            error.kind.reason()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_value_set_projects_exact_secret_values_without_filtering() {
        let vars = BTreeMap::from([
            ("empty".to_string(), String::new()),
            ("short".to_string(), "1".to_string()),
            ("whitespace".to_string(), "  secret value\t".to_string()),
            ("unicode".to_string(), "sëcret-🔐".to_string()),
            ("first_duplicate".to_string(), "shared".to_string()),
            ("second_duplicate".to_string(), "shared".to_string()),
            ("process".to_string(), "visible".to_string()),
            ("unsourced".to_string(), "not-selected".to_string()),
        ]);
        let sources = BTreeMap::from([
            ("empty".to_string(), ValueSource::Secret),
            ("short".to_string(), ValueSource::Secret),
            ("whitespace".to_string(), ValueSource::Secret),
            ("unicode".to_string(), ValueSource::Secret),
            ("first_duplicate".to_string(), ValueSource::Secret),
            ("second_duplicate".to_string(), ValueSource::Secret),
            ("process".to_string(), ValueSource::ProcessEnv),
            ("missing_value".to_string(), ValueSource::Secret),
        ]);

        assert_eq!(
            secret_value_set(&vars, &sources),
            BTreeSet::from([
                String::new(),
                "1".to_string(),
                "  secret value\t".to_string(),
                "shared".to_string(),
                "sëcret-🔐".to_string(),
            ])
        );
    }

    fn settings_fixture() -> Settings {
        let mut settings = Settings {
            default_profile: Some("dev".into()),
            ..Settings::default()
        };
        settings.defaults.compose_file = Some("compose-default.yaml".into());
        settings.defaults.env_files = vec![".env.defaults".into()];
        settings
            .defaults
            .env
            .insert("A".into(), "defaults-map".into());
        settings.defaults.binaries.srun = Some("/defaults/srun".into());
        settings.defaults.cache.dir = Some("defaults-cache".into());
        settings.defaults.login_host = Some("login-defaults.example".into());
        settings.defaults.workspace = Some(WorkspaceSettings {
            name: Some("defaults-workspace".into()),
            duration_days: Some(10),
            warn_days_left: Some(4),
            ..WorkspaceSettings::default()
        });

        let mut profile = SettingsProfile {
            compose_file: Some("compose-profile.yaml".into()),
            env_files: vec![".env.profile".into()],
            ..SettingsProfile::default()
        };
        profile.env.insert("A".into(), "profile-map".into());
        profile.binaries.srun = Some("/profile/srun".into());
        profile.cache.dir = Some("profile-cache".into());
        profile.login_host = Some("login-profile.example".into());
        profile.workspace = Some(WorkspaceSettings {
            name: Some("profile-workspace".into()),
            ..WorkspaceSettings::default()
        });
        settings.profiles.insert("dev".into(), profile);
        settings
    }

    #[test]
    fn resolve_workspace_settings_merges_profile_over_defaults_per_field() {
        let defaults = WorkspaceSettings {
            name: Some("d".into()),
            duration_days: Some(10),
            auto_allocate: Some(false),
            ..WorkspaceSettings::default()
        };
        let profile = WorkspaceSettings {
            name: Some("p".into()),
            queue_buffer_days: Some(3),
            ..WorkspaceSettings::default()
        };

        let merged = resolve_workspace_settings(Some(&profile), Some(&defaults))
            .expect("both layers present");
        assert_eq!(merged.name.as_deref(), Some("p"));
        assert_eq!(merged.duration_days, Some(10));
        assert_eq!(merged.auto_allocate, Some(false));
        assert_eq!(merged.queue_buffer_days, Some(3));
        assert_eq!(merged.warn_days_left, None);

        let defaults_only =
            resolve_workspace_settings(None, Some(&defaults)).expect("defaults present");
        assert_eq!(defaults_only.name.as_deref(), Some("d"));
        assert!(resolve_workspace_settings(None, None).is_none());
    }

    #[test]
    fn resolve_login_host_prefers_profile_then_defaults() {
        assert_eq!(
            resolve_login_host(Some("p"), Some("d")).as_deref(),
            Some("p")
        );
        assert_eq!(resolve_login_host(None, Some("d")).as_deref(), Some("d"));
        assert_eq!(resolve_login_host(Some("p"), None).as_deref(), Some("p"));
        assert_eq!(resolve_login_host(None, None), None);
    }

    #[test]
    fn discover_settings_path_walks_upward() {
        let tmp = tempfile::tempdir().expect("tmp");
        let repo = tmp.path().join("repo");
        let nested = repo.join("a/b/c");
        fs::create_dir_all(nested.clone()).expect("mkdir");
        let settings_path = repo.join(".hpc-compose/settings.toml");
        fs::create_dir_all(settings_path.parent().expect("parent")).expect("mkdir");
        fs::write(
            &settings_path,
            "version = 1\n[profiles.dev]\ncompose_file = \"compose.yaml\"\n",
        )
        .expect("write");

        assert_eq!(discover_settings_path(&nested), Some(settings_path));
    }

    #[test]
    fn resolve_applies_profile_and_defaults_precedence() {
        let tmp = tempfile::tempdir().expect("tmp");
        let repo = tmp.path().join("repo");
        fs::create_dir_all(repo.join(".hpc-compose")).expect("mkdir");
        fs::write(
            repo.join(".hpc-compose/settings.toml"),
            toml::to_string_pretty(&settings_fixture()).expect("settings"),
        )
        .expect("write");
        fs::write(repo.join(".env.defaults"), "A=defaults-file\n").expect("write defaults env");
        fs::write(repo.join(".env.profile"), "A=profile-file\n").expect("write profile env");
        fs::write(
            repo.join("compose-profile.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");

        let resolved = resolve(&ResolveRequest {
            cwd: repo.clone(),
            ..ResolveRequest::default()
        })
        .expect("resolve");

        assert_eq!(resolved.selected_profile.as_deref(), Some("dev"));
        assert!(
            resolved
                .compose_file
                .value
                .ends_with("repo/compose-profile.yaml")
        );
        assert_eq!(resolved.compose_file.source, ValueSource::Profile);
        assert_eq!(resolved.binaries.srun.value, "/profile/srun");
        assert_eq!(resolved.binaries.srun.source, ValueSource::Profile);
        assert_eq!(resolved.cache_dir.value, repo.join("profile-cache"));
        assert_eq!(resolved.cache_dir.source, ValueSource::Profile);
        // Profile login_host overrides the shared default.
        assert_eq!(
            resolved.login_host.as_deref(),
            Some("login-profile.example")
        );
        // Workspace settings merge field-wise: the profile's name wins while
        // unset profile fields fall back to the shared defaults.
        let workspace = resolved.workspace.as_ref().expect("workspace resolved");
        assert_eq!(workspace.name.as_deref(), Some("profile-workspace"));
        assert_eq!(workspace.duration_days, Some(10));
        assert_eq!(workspace.warn_days_left, Some(4));
        assert_eq!(workspace.auto_allocate, None);
        assert_eq!(
            resolved.interpolation_vars.get("A").map(String::as_str),
            Some("profile-map")
        );
        assert_eq!(
            resolved.interpolation_var_sources.get("A"),
            Some(&ValueSource::Profile)
        );
    }

    #[test]
    fn resolve_prefers_cli_overrides() {
        let tmp = tempfile::tempdir().expect("tmp");
        let repo = tmp.path().join("repo");
        fs::create_dir_all(repo.join(".hpc-compose")).expect("mkdir");
        fs::write(
            repo.join(".hpc-compose/settings.toml"),
            toml::to_string_pretty(&settings_fixture()).expect("settings"),
        )
        .expect("write");
        fs::write(repo.join(".env.defaults"), "A=defaults-file\n").expect("write defaults env");
        fs::write(repo.join(".env.profile"), "A=profile-file\n").expect("write profile env");
        fs::write(
            repo.join("compose-cli.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");

        let binary_overrides = BinaryOverrides {
            srun: Some("/cli/srun".into()),
            ..BinaryOverrides::default()
        };
        let resolved = resolve(&ResolveRequest {
            cwd: repo,
            compose_file_override: Some(PathBuf::from("compose-cli.yaml")),
            binary_overrides,
            ..ResolveRequest::default()
        })
        .expect("resolve");
        assert_eq!(resolved.compose_file.source, ValueSource::Cli);
        assert!(resolved.compose_file.value.ends_with("compose-cli.yaml"));
        assert_eq!(resolved.binaries.srun.value, "/cli/srun");
        assert_eq!(resolved.binaries.srun.source, ValueSource::Cli);
    }

    #[test]
    fn resolve_cache_settings_use_defaults_tilde_and_builtin_fallback() {
        let tmp = tempfile::tempdir().expect("tmp");
        let repo = tmp.path().join("repo");
        fs::create_dir_all(repo.join(".hpc-compose")).expect("mkdir");
        let mut settings = settings_fixture();
        settings.profiles.get_mut("dev").expect("dev").cache.dir = None;
        settings.defaults.cache.dir = Some("~/hpc-compose-cache".into());
        fs::write(
            repo.join(".hpc-compose/settings.toml"),
            toml::to_string_pretty(&settings).expect("settings"),
        )
        .expect("write");
        fs::write(repo.join(".env.defaults"), "A=defaults-file\n").expect("write defaults env");
        fs::write(repo.join(".env.profile"), "A=profile-file\n").expect("write profile env");
        fs::write(
            repo.join("compose-profile.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");

        let resolved = resolve(&ResolveRequest {
            cwd: repo.clone(),
            ..ResolveRequest::default()
        })
        .expect("resolve");
        assert_eq!(resolved.cache_dir.source, ValueSource::Defaults);
        assert!(resolved.cache_dir.value.ends_with("hpc-compose-cache"));

        let no_settings = tmp.path().join("no-settings");
        fs::create_dir_all(&no_settings).expect("no settings dir");
        fs::write(
            no_settings.join("compose.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");
        let fallback = resolve(&ResolveRequest {
            cwd: no_settings,
            settings_file: None,
            compose_file_override: Some(PathBuf::from("compose.yaml")),
            ..ResolveRequest::default()
        })
        .expect("fallback");
        assert_eq!(fallback.cache_dir.source, ValueSource::Builtin);
        assert!(fallback.cache_dir.value.ends_with(".cache/hpc-compose"));
    }

    #[test]
    fn settings_schema_version_must_match() {
        let tmp = tempfile::tempdir().expect("tmp");
        let path = tmp.path().join("settings.toml");
        fs::write(&path, "version = 2\n").expect("write");
        let err = load_settings(&path).expect_err("schema mismatch");
        assert!(
            err.to_string()
                .contains("unsupported settings schema version")
        );
    }

    #[test]
    fn watch_settings_accept_schema_boundaries_on_load_and_write() {
        let tmp = tempfile::tempdir().expect("tmp");
        let loaded_path = tmp.path().join("loaded.toml");
        fs::write(
            &loaded_path,
            "version = 1\n[watch]\nsort = \"spec\"\nrefresh_ms = 100\nmetrics_refresh_ms = 500\n",
        )
        .expect("write boundary settings");

        let loaded = load_settings(&loaded_path).expect("load boundary settings");
        assert_eq!(loaded.watch.sort.as_deref(), Some("spec"));
        assert_eq!(loaded.watch.refresh_ms, Some(100));
        assert_eq!(loaded.watch.metrics_refresh_ms, Some(500));

        let written_path = tmp.path().join("written.toml");
        let settings = Settings {
            watch: WatchSettings {
                sort: Some("triage".into()),
                refresh_ms: Some(100),
                metrics_refresh_ms: Some(500),
                ..WatchSettings::default()
            },
            ..Settings::default()
        };
        write_settings(&written_path, &settings).expect("write boundary settings");
        assert_eq!(
            load_settings(&written_path)
                .expect("reload boundary settings")
                .watch,
            settings.watch
        );
    }

    #[test]
    fn load_settings_rejects_invalid_watch_values() {
        let tmp = tempfile::tempdir().expect("tmp");
        let path = tmp.path().join("settings.toml");
        for (watch, expected) in [
            ("sort = \"banana\"", "invalid [watch].sort 'banana'"),
            ("refresh_ms = 99", "invalid [watch].refresh_ms 99"),
            (
                "metrics_refresh_ms = 499",
                "invalid [watch].metrics_refresh_ms 499",
            ),
        ] {
            fs::write(&path, format!("version = 1\n[watch]\n{watch}\n"))
                .expect("write invalid settings");
            let err = load_settings(&path).expect_err("invalid watch settings");
            assert!(
                err.to_string().contains(expected),
                "unexpected error for {watch}: {err:#}"
            );
        }
    }

    #[test]
    fn write_settings_rejects_invalid_watch_values_before_writing() {
        let tmp = tempfile::tempdir().expect("tmp");
        let invalid = [
            (
                WatchSettings {
                    sort: Some("banana".into()),
                    ..WatchSettings::default()
                },
                "invalid [watch].sort 'banana'",
            ),
            (
                WatchSettings {
                    refresh_ms: Some(99),
                    ..WatchSettings::default()
                },
                "invalid [watch].refresh_ms 99",
            ),
            (
                WatchSettings {
                    metrics_refresh_ms: Some(499),
                    ..WatchSettings::default()
                },
                "invalid [watch].metrics_refresh_ms 499",
            ),
        ];

        for (index, (watch, expected)) in invalid.into_iter().enumerate() {
            let path = tmp.path().join(format!("invalid-{index}/settings.toml"));
            let settings = Settings {
                watch,
                ..Settings::default()
            };
            let err = write_settings(&path, &settings).expect_err("invalid watch settings");
            assert!(
                err.to_string().contains(expected),
                "unexpected write error: {err:#}"
            );
            assert!(!path.exists(), "invalid settings should not be written");
        }
    }

    #[cfg(unix)]
    #[test]
    fn write_settings_restricts_existing_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().expect("tmp");
        let path = tmp.path().join("settings.toml");
        fs::write(&path, "version = 1\n").expect("seed");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("seed mode");

        write_settings(&path, &Settings::default()).expect("write settings");

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "settings.toml should be owner-only");
        assert!(
            fs::read_dir(tmp.path())
                .expect("read tmpdir")
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp.")),
            "settings writes should not leave atomic temp files behind"
        );
    }

    #[test]
    fn resolve_login_user_prefers_profile_then_default() {
        // Profile wins over the shared default.
        assert_eq!(
            resolve_login_user(Some("vy3326"), Some("fallback")).as_deref(),
            Some("vy3326")
        );
        // Default applies when the profile is silent.
        assert_eq!(
            resolve_login_user(None, Some("fallback")).as_deref(),
            Some("fallback")
        );
        // Neither set: no login user, so the bare host (or user@host) is used as-is
        // — not the local laptop username (the original bug this guards).
        assert_eq!(resolve_login_user(None, None), None);
    }

    #[test]
    fn repo_adjacent_settings_path_uses_git_root_when_present() {
        let tmp = tempfile::tempdir().expect("tmp");
        let repo = tmp.path().join("repo");
        fs::create_dir_all(repo.join(".git")).expect("mkdir");
        let nested = repo.join("a/b/c");
        fs::create_dir_all(&nested).expect("mkdir nested");

        let expected = repo.join(".hpc-compose/settings.toml");
        assert_eq!(repo_adjacent_settings_path(&nested), expected);
    }

    #[test]
    fn helper_functions_cover_defaults_paths_and_env_parsing() {
        assert_eq!(default_settings_schema_version(), 1);
        let request = ResolveRequest::from_current_dir().expect("request");
        assert_eq!(request.cwd, env::current_dir().expect("cwd"));

        let tmp = tempfile::tempdir().expect("tmp");
        let missing = tmp.path().join("missing.toml");
        assert!(
            load_settings_if_exists(&missing)
                .expect("missing")
                .is_none()
        );

        let invalid_settings = Settings {
            version: 99,
            ..Settings::default()
        };
        let err = write_settings(&tmp.path().join("nested/settings.toml"), &invalid_settings)
            .expect_err("invalid version");
        assert!(err.to_string().contains("unsupported schema version"));

        let env_file = tmp.path().join("vars.env");
        fs::write(
            &env_file,
            "\n# comment\nexport QUOTED=\"value\"\nSINGLE='two'\nPLAIN=three\n",
        )
        .expect("env file");
        let parsed = parse_env_file(&env_file).expect("parse env");
        assert_eq!(parsed.get("QUOTED").map(String::as_str), Some("value"));
        assert_eq!(parsed.get("SINGLE").map(String::as_str), Some("two"));
        assert_eq!(parsed.get("PLAIN").map(String::as_str), Some("three"));

        let invalid_syntax = tmp.path().join("invalid-syntax.env");
        fs::write(&invalid_syntax, "MISSING\n").expect("invalid syntax env");
        assert!(
            parse_env_file(&invalid_syntax)
                .expect_err("missing equals")
                .to_string()
                .contains("must use KEY=VALUE syntax")
        );

        let invalid_key = tmp.path().join("invalid-key.env");
        fs::write(&invalid_key, "=oops\n").expect("invalid key env");
        assert!(
            parse_env_file(&invalid_key)
                .expect_err("empty key")
                .to_string()
                .contains("empty variable name")
        );

        assert_eq!(
            settings_base_dir(&tmp.path().join("settings.toml")),
            tmp.path()
        );
        assert_eq!(
            resolve_string_path("compose.yaml", tmp.path()),
            tmp.path().join("compose.yaml")
        );
        assert_eq!(
            crate::path_util::absolute_path(Path::new("/tmp/absolute"), tmp.path()),
            PathBuf::from("/tmp/absolute")
        );
    }

    #[test]
    fn env_file_parser_preserves_dotenv_edge_grammar() {
        let tmp = tempfile::tempdir().expect("tmp");
        let path = tmp.path().join("vars.env");
        fs::write(
            &path,
            concat!(
                "  # comment with leading whitespace\n",
                " export EMPTY =   \n",
                "DUPLICATE=first\n",
                " DUPLICATE = second \n",
                "DOUBLE=\"two words\"\n",
                "SINGLE='one word'\n",
                "EMPTY_DOUBLE=\"\"\n",
                "EMPTY_SINGLE=''\n",
                "UNMATCHED_SINGLE='left\n",
                "UNMATCHED_DOUBLE=\"right\n",
                "MISMATCHED_SINGLE='left\"\n",
                "MISMATCHED_DOUBLE=\"right'\n",
            ),
        )
        .expect("env file");

        let parsed = parse_env_file(&path).expect("parse env");
        assert_eq!(parsed.get("EMPTY").map(String::as_str), Some(""));
        assert_eq!(parsed.get("DUPLICATE").map(String::as_str), Some("second"));
        assert_eq!(parsed.get("DOUBLE").map(String::as_str), Some("two words"));
        assert_eq!(parsed.get("SINGLE").map(String::as_str), Some("one word"));
        assert_eq!(parsed.get("EMPTY_DOUBLE").map(String::as_str), Some(""));
        assert_eq!(parsed.get("EMPTY_SINGLE").map(String::as_str), Some(""));
        assert_eq!(
            parsed.get("UNMATCHED_SINGLE").map(String::as_str),
            Some("'left")
        );
        assert_eq!(
            parsed.get("UNMATCHED_DOUBLE").map(String::as_str),
            Some("\"right")
        );
        assert_eq!(
            parsed.get("MISMATCHED_SINGLE").map(String::as_str),
            Some("'left\"")
        );
        assert_eq!(
            parsed.get("MISMATCHED_DOUBLE").map(String::as_str),
            Some("\"right'")
        );
    }

    #[test]
    fn env_file_parser_reports_exact_line_and_reason() {
        let tmp = tempfile::tempdir().expect("tmp");
        let path = tmp.path().join("vars.env");

        fs::write(&path, "GOOD=1\n# comment\nBROKEN\n").expect("env file");
        assert_eq!(
            parse_env_file(&path)
                .expect_err("missing equals")
                .to_string(),
            format!(
                "failed to parse {}: line 3 must use KEY=VALUE syntax",
                path.display()
            )
        );

        fs::write(&path, "GOOD=1\n\n = value\n").expect("env file");
        assert_eq!(
            parse_env_file(&path).expect_err("empty key").to_string(),
            format!(
                "failed to parse {}: line 3 has an empty variable name",
                path.display()
            )
        );
    }

    #[test]
    fn resolve_binaries_only_preserves_settings_and_missing_profile_errors() {
        let tmp = tempfile::tempdir().expect("tmp");

        let missing_settings = tmp.path().join("missing-settings.toml");
        let error = resolve_binaries_only(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(missing_settings.clone()),
            ..ResolveRequest::default()
        })
        .expect_err("missing explicit settings");
        assert_eq!(
            error.to_string(),
            format!(
                "settings file does not exist: {}",
                missing_settings.display()
            )
        );

        let malformed_settings = tmp.path().join("malformed-settings.toml");
        fs::write(&malformed_settings, "version = [\n").expect("malformed settings");
        let error = resolve_binaries_only(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(malformed_settings.clone()),
            ..ResolveRequest::default()
        })
        .expect_err("malformed explicit settings");
        assert_eq!(
            error.to_string(),
            format!(
                "failed to parse settings file {}",
                malformed_settings.display()
            )
        );

        let no_settings = tmp.path().join("no-settings");
        fs::create_dir(&no_settings).expect("no-settings dir");
        let error = resolve_binaries_only(&ResolveRequest {
            cwd: no_settings,
            profile: Some("missing".into()),
            ..ResolveRequest::default()
        })
        .expect_err("profile without settings");
        assert_eq!(
            error.to_string(),
            format!(
                "profile 'missing' was requested, but no settings file was found (expected {} in this repository tree)",
                SETTINGS_RELATIVE_PATH
            )
        );
    }

    #[test]
    fn resolve_binaries_only_preserves_undefined_explicit_and_default_profile_errors() {
        let tmp = tempfile::tempdir().expect("tmp");
        let settings_path = tmp.path().join("settings.toml");
        let settings = Settings {
            default_profile: Some("undefined-default".into()),
            ..Settings::default()
        };
        write_settings(&settings_path, &settings).expect("settings");

        let error = resolve_binaries_only(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            profile: Some("undefined-explicit".into()),
            settings_file: Some(settings_path.clone()),
            ..ResolveRequest::default()
        })
        .expect_err("undefined explicit profile");
        assert_eq!(
            error.to_string(),
            "profile 'undefined-explicit' is not defined in settings"
        );

        let error = resolve_binaries_only(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(settings_path),
            ..ResolveRequest::default()
        })
        .expect_err("undefined default profile");
        assert_eq!(
            error.to_string(),
            "profile 'undefined-default' is not defined in settings"
        );
    }

    #[test]
    fn resolve_binaries_only_preserves_cli_profile_defaults_builtin_precedence() {
        let tmp = tempfile::tempdir().expect("tmp");
        let settings_path = tmp.path().join("settings.toml");
        let mut settings = Settings {
            default_profile: Some("unused-default".into()),
            ..Settings::default()
        };
        settings.defaults.binaries.srun = Some("/defaults/srun".into());
        settings.defaults.binaries.squeue = Some("/defaults/squeue".into());
        settings.defaults.binaries.sacct = Some("/defaults/sacct".into());
        let mut selected = SettingsProfile::default();
        selected.binaries.srun = Some("/profile/srun".into());
        selected.binaries.squeue = Some("/profile/squeue".into());
        settings.profiles.insert("selected".into(), selected);
        settings
            .profiles
            .insert("unused-default".into(), SettingsProfile::default());
        write_settings(&settings_path, &settings).expect("settings");

        let binaries = resolve_binaries_only(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            profile: Some("selected".into()),
            settings_file: Some(settings_path),
            binary_overrides: BinaryOverrides {
                srun: Some("/cli/srun".into()),
                ..BinaryOverrides::default()
            },
            ..ResolveRequest::default()
        })
        .expect("resolve binaries");

        assert_eq!(binaries.srun.value, "/cli/srun");
        assert_eq!(binaries.srun.source, ValueSource::Cli);
        assert_eq!(binaries.squeue.value, "/profile/squeue");
        assert_eq!(binaries.squeue.source, ValueSource::Profile);
        assert_eq!(binaries.sacct.value, "/defaults/sacct");
        assert_eq!(binaries.sacct.source, ValueSource::Defaults);
        assert_eq!(binaries.sstat.value, DEFAULT_SSTAT_BIN);
        assert_eq!(binaries.sstat.source, ValueSource::Builtin);
    }

    #[test]
    fn resolve_binaries_only_does_not_read_compose_env_or_secret_inputs() {
        let tmp = tempfile::tempdir().expect("tmp");
        let settings_path = tmp.path().join("settings.toml");
        let compose_path = tmp.path().join("compose.yaml");
        let mut settings = Settings::default();
        settings.defaults.compose_file = Some("compose.yaml".into());
        settings.defaults.env_files = vec!["missing.env".into()];
        settings.defaults.binaries.sbatch = Some("/defaults/sbatch".into());
        write_settings(&settings_path, &settings).expect("settings");
        fs::write(
            &compose_path,
            "secrets:\n  TOKEN:\n    file: missing-secret.txt\nservices:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");

        let request = ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(settings_path.clone()),
            ..ResolveRequest::default()
        };
        let binaries =
            resolve_binaries_only(&request).expect("ignore missing env and secret files");
        assert_eq!(binaries.sbatch.value, "/defaults/sbatch");
        assert_eq!(binaries.sbatch.source, ValueSource::Defaults);

        fs::remove_file(compose_path).expect("remove compose");
        let binaries = resolve_binaries_only(&ResolveRequest {
            compose_file_override: Some(PathBuf::from("also-missing.yaml")),
            ..request
        })
        .expect("ignore missing compose file");
        assert_eq!(binaries.sbatch.value, "/defaults/sbatch");
        assert_eq!(binaries.sbatch.source, ValueSource::Defaults);
    }

    #[test]
    fn resolve_with_explicit_settings_file_covers_defaults_and_errors() {
        let tmp = tempfile::tempdir().expect("tmp");

        let settings_dir = tmp.path().join("config");
        fs::create_dir_all(&settings_dir).expect("settings dir");
        let settings_path = settings_dir.join("settings.toml");

        let mut settings = Settings::default();
        settings.defaults.compose_file = Some("compose-default.yaml".into());
        settings.defaults.env_files = vec!["defaults.env".into()];
        settings
            .defaults
            .env
            .insert("MAP".into(), "defaults-map".into());
        settings.defaults.binaries.squeue = Some("/defaults/squeue".into());
        settings.defaults.binaries.sshare = Some("/defaults/sshare".into());
        settings.defaults.binaries.sprio = Some("/defaults/sprio".into());
        write_settings(&settings_path, &settings).expect("write settings");

        fs::write(
            settings_dir.join("compose-default.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("compose");
        fs::write(settings_dir.join(".env"), "DOTENV=compose\n").expect("dotenv");
        fs::write(settings_dir.join("defaults.env"), "FILE_ENV=defaults\n").expect("defaults env");

        let resolved = resolve(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(settings_path.clone()),
            ..ResolveRequest::default()
        })
        .expect("resolve explicit settings");
        assert_eq!(resolved.compose_file.source, ValueSource::Defaults);
        assert_eq!(resolved.binaries.squeue.source, ValueSource::Defaults);
        assert_eq!(resolved.binaries.sshare.value, "/defaults/sshare");
        assert_eq!(resolved.binaries.sprio.value, "/defaults/sprio");
        assert_eq!(
            resolved
                .interpolation_vars
                .get("DOTENV")
                .map(String::as_str),
            Some("compose")
        );
        assert_eq!(
            resolved.interpolation_var_sources.get("DOTENV"),
            Some(&ValueSource::Compose)
        );
        assert_eq!(
            resolved
                .interpolation_vars
                .get("FILE_ENV")
                .map(String::as_str),
            Some("defaults")
        );
        assert_eq!(
            resolved.interpolation_var_sources.get("FILE_ENV"),
            Some(&ValueSource::Defaults)
        );
        assert_eq!(
            resolved.interpolation_vars.get("MAP").map(String::as_str),
            Some("defaults-map")
        );
        assert_eq!(resolved.settings_base_dir, Some(settings_dir.clone()));

        let missing_settings = resolve(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(tmp.path().join("missing.toml")),
            ..ResolveRequest::default()
        })
        .expect_err("missing settings");
        assert!(
            missing_settings
                .to_string()
                .contains("settings file does not exist")
        );

        let missing_profile = resolve(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            profile: Some("dev".into()),
            ..ResolveRequest::default()
        })
        .expect_err("profile without settings");
        assert!(
            missing_profile
                .to_string()
                .contains("no settings file was found")
        );

        let bad_dir = tmp.path().join("bad");
        fs::create_dir_all(&bad_dir).expect("bad dir");
        let bad_settings_path = bad_dir.join("settings.toml");
        let mut bad_settings = Settings::default();
        bad_settings.defaults.compose_file = Some("compose.yaml".into());
        bad_settings.defaults.env_files = vec!["missing.env".into()];
        write_settings(&bad_settings_path, &bad_settings).expect("bad settings");
        fs::write(
            bad_dir.join("compose.yaml"),
            "services:\n  app:\n    image: redis:7\n",
        )
        .expect("bad compose");

        let missing_env = resolve(&ResolveRequest {
            cwd: tmp.path().to_path_buf(),
            settings_file: Some(bad_settings_path),
            ..ResolveRequest::default()
        })
        .expect_err("missing env file");
        assert!(
            missing_env
                .to_string()
                .contains("settings env file does not exist")
        );
    }
}
