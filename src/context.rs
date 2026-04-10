//! Repo-adjacent settings and execution-context resolution.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const SETTINGS_SCHEMA_VERSION: u32 = 1;
const SETTINGS_RELATIVE_PATH: &str = ".hpc-compose/settings.toml";

const DEFAULT_COMPOSE_FILE: &str = "compose.yaml";
const DEFAULT_ENROOT_BIN: &str = "enroot";
const DEFAULT_SBATCH_BIN: &str = "sbatch";
const DEFAULT_SRUN_BIN: &str = "srun";
const DEFAULT_SQUEUE_BIN: &str = "squeue";
const DEFAULT_SACCT_BIN: &str = "sacct";
const DEFAULT_SSTAT_BIN: &str = "sstat";
const DEFAULT_SCANCEL_BIN: &str = "scancel";

/// Source that provided a resolved value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
}

/// A resolved value and where it came from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedValue<T> {
    /// Final value.
    pub value: T,
    /// Source that won resolution.
    pub source: ValueSource,
}

/// Binary override settings.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BinaryOverrides {
    #[serde(default)]
    pub enroot: Option<String>,
    #[serde(default)]
    pub sbatch: Option<String>,
    #[serde(default)]
    pub srun: Option<String>,
    #[serde(default)]
    pub squeue: Option<String>,
    #[serde(default)]
    pub sacct: Option<String>,
    #[serde(default)]
    pub sstat: Option<String>,
    #[serde(default)]
    pub scancel: Option<String>,
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
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            version: SETTINGS_SCHEMA_VERSION,
            default_profile: None,
            defaults: SettingsDefaults::default(),
            profiles: BTreeMap::new(),
        }
    }
}

fn default_settings_schema_version() -> u32 {
    SETTINGS_SCHEMA_VERSION
}

/// Fully resolved binaries.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedBinaries {
    pub enroot: ResolvedValue<String>,
    pub sbatch: ResolvedValue<String>,
    pub srun: ResolvedValue<String>,
    pub squeue: ResolvedValue<String>,
    pub sacct: ResolvedValue<String>,
    pub sstat: ResolvedValue<String>,
    pub scancel: ResolvedValue<String>,
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
    pub binaries: ResolvedBinaries,
    pub interpolation_vars: BTreeMap<String, String>,
    pub interpolation_var_sources: BTreeMap<String, ValueSource>,
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
#[must_use]
pub fn repo_root_or_cwd(start: &Path) -> PathBuf {
    for dir in start.ancestors() {
        let git = dir.join(".git");
        if git.exists() {
            return dir.to_path_buf();
        }
    }
    start.to_path_buf()
}

/// Loads settings if a path exists.
///
/// # Errors
///
/// Returns an error when the file cannot be parsed or has an unsupported
/// schema version.
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
/// Returns an error when serialization or file writes fail.
pub fn write_settings(path: &Path, settings: &Settings) -> Result<()> {
    if settings.version != SETTINGS_SCHEMA_VERSION {
        bail!(
            "refusing to write settings with unsupported schema version {}",
            settings.version
        );
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = toml::to_string_pretty(settings).context("failed to serialize settings")?;
    fs::write(path, rendered).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Resolves command-level context from settings, profile, and CLI overrides.
///
/// # Errors
///
/// Returns an error when settings parsing fails, a requested profile is
/// missing, or a referenced env file cannot be read.
pub fn resolve(request: &ResolveRequest) -> Result<ResolvedContext> {
    let settings_path = if let Some(path) = request.settings_file.as_ref() {
        if !path.exists() {
            bail!("settings file does not exist: {}", path.display());
        }
        Some(absolute_path(path, &request.cwd))
    } else {
        discover_settings_path(&request.cwd)
    };

    let settings = match settings_path.as_ref() {
        Some(path) => Some(load_settings(path)?),
        None => None,
    };

    let selected_profile = request.profile.clone().or_else(|| {
        settings
            .as_ref()
            .and_then(|cfg| cfg.default_profile.clone())
    });
    let profile_cfg = match (settings.as_ref(), selected_profile.as_ref()) {
        (Some(cfg), Some(name)) => {
            let profile = cfg
                .profiles
                .get(name)
                .with_context(|| format!("profile '{name}' is not defined in settings"))?;
            Some(profile)
        }
        (None, Some(name)) => {
            bail!(
                "profile '{}' was requested, but no settings file was found (expected {} in this repository tree)",
                name,
                SETTINGS_RELATIVE_PATH
            );
        }
        _ => None,
    };
    let defaults_cfg = settings.as_ref().map(|cfg| &cfg.defaults);

    let settings_base = settings_path
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

    let resolved_settings_base_dir = settings_path.as_deref().map(settings_base_dir);

    Ok(ResolvedContext {
        cwd: request.cwd.clone(),
        settings_path,
        settings_base_dir: resolved_settings_base_dir,
        selected_profile,
        compose_file,
        binaries,
        interpolation_vars,
        interpolation_var_sources,
    })
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
            value: absolute_path(path, cwd),
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
        value: absolute_path(Path::new(DEFAULT_COMPOSE_FILE), cwd),
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
    absolute_path(&raw, base)
}

fn absolute_path(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read env file {}", path.display()))?;
    let mut vars = BTreeMap::new();
    for (index, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let stripped = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = stripped.split_once('=') else {
            bail!(
                "failed to parse {}: line {} must use KEY=VALUE syntax",
                path.display(),
                index + 1
            );
        };
        let key = key.trim();
        if key.is_empty() {
            bail!(
                "failed to parse {}: line {} has an empty variable name",
                path.display(),
                index + 1
            );
        }
        let value = value.trim();
        let value = if quoted(value, '"') || quoted(value, '\'') {
            value[1..value.len() - 1].to_string()
        } else {
            value.to_string()
        };
        vars.insert(key.to_string(), value);
    }
    Ok(vars)
}

fn quoted(value: &str, quote: char) -> bool {
    value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote)
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let mut profile = SettingsProfile {
            compose_file: Some("compose-profile.yaml".into()),
            env_files: vec![".env.profile".into()],
            ..SettingsProfile::default()
        };
        profile.env.insert("A".into(), "profile-map".into());
        profile.binaries.srun = Some("/profile/srun".into());
        settings.profiles.insert("dev".into(), profile);
        settings
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
            absolute_path(Path::new("/tmp/absolute"), tmp.path()),
            PathBuf::from("/tmp/absolute")
        );
        assert!(quoted("\"value\"", '"'));
        assert!(!quoted("value", '"'));
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
