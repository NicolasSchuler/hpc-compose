use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap_complete::Shell;
use hpc_compose::cli::OutputFormat;
use hpc_compose::cli::build_cli_command;
use hpc_compose::context::{
    BinaryOverrides, Settings, repo_adjacent_settings_path, write_settings,
};
use hpc_compose::init::{
    default_cache_dir as default_init_cache_dir, next_commands, prompt_for_init, render_template,
    write_initialized_template,
};

use crate::output;

#[allow(clippy::too_many_arguments)]
pub(crate) fn new_command(
    template: Option<String>,
    list_templates: bool,
    describe_template: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    output_path: PathBuf,
    force: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    if list_templates {
        match output::resolve_output_format(format, false) {
            OutputFormat::Text => output::print_template_list(),
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "templates": output::template_infos(),
                        "default_cache_dir": default_init_cache_dir(),
                    }))
                    .context("failed to serialize template list output")?
                );
            }
        }
        return Ok(());
    }
    if let Some(template_name) = describe_template {
        match output::resolve_output_format(format, false) {
            OutputFormat::Text => output::print_template_description(&template_name)?,
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&output::build_template_description(
                        &template_name,
                    )?)
                    .context("failed to serialize template description output")?
                );
            }
        }
        return Ok(());
    }
    let answers = output::resolve_init_answers(template, name, cache_dir, prompt_for_init)?;
    let rendered = render_template(
        &answers.template_name,
        &answers.app_name,
        &answers.cache_dir,
    )?;
    let path = write_initialized_template(&output_path, &rendered, force)?;
    let next_commands = next_commands(&path);
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("wrote {}", path.display());
            for command in next_commands {
                println!("{command}");
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::TemplateWriteOutput {
                    template_name: answers.template_name,
                    app_name: answers.app_name,
                    cache_dir: answers.cache_dir,
                    output_path: path,
                    next_commands,
                })
                .context("failed to serialize scaffold output")?
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn setup(
    settings_file_override: Option<PathBuf>,
    global_profile: Option<String>,
    profile_name: Option<String>,
    compose_file: Option<String>,
    env_files: Vec<String>,
    env_entries: Vec<String>,
    binary_entries: Vec<String>,
    default_profile: Option<String>,
    non_interactive: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to determine current working directory")?;
    let settings_path = settings_file_override
        .map(|path| absolute_path(&path, &cwd))
        .unwrap_or_else(|| repo_adjacent_settings_path(&cwd));
    let mut settings = hpc_compose::context::load_settings_if_exists(&settings_path)?
        .unwrap_or_else(Settings::default);

    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout();

    let selected_profile = if non_interactive {
        profile_name
            .or(global_profile)
            .or_else(|| settings.default_profile.clone())
            .unwrap_or_else(|| "dev".to_string())
    } else {
        let default = profile_name
            .clone()
            .or(global_profile.clone())
            .or_else(|| settings.default_profile.clone())
            .unwrap_or_else(|| "dev".to_string());
        prompt(&mut stdin, &mut stdout, "Profile name", &default)?
    };

    let existing_profile = settings.profiles.get(&selected_profile).cloned();
    let compose_default = compose_file.clone().or_else(|| {
        existing_profile
            .as_ref()
            .and_then(|profile| profile.compose_file.clone())
            .or_else(|| settings.defaults.compose_file.clone())
            .or_else(|| Some("compose.yaml".to_string()))
    });
    let resolved_compose_file = if non_interactive {
        compose_default
            .unwrap_or_else(|| "compose.yaml".to_string())
            .trim()
            .to_string()
    } else {
        prompt(
            &mut stdin,
            &mut stdout,
            "Compose file",
            compose_default.as_deref().unwrap_or("compose.yaml"),
        )?
    };

    let env_files_value = if non_interactive {
        if env_files.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| profile.env_files.clone())
                .unwrap_or_default()
        } else {
            dedup_preserve_order(env_files)
        }
    } else {
        let default = if env_files.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| profile.env_files.join(","))
                .unwrap_or_default()
        } else {
            env_files.join(",")
        };
        parse_csv_entries(&prompt(
            &mut stdin,
            &mut stdout,
            "Profile env files (comma-separated)",
            &default,
        )?)
    };

    let env_value = if non_interactive {
        if env_entries.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| profile.env.clone())
                .unwrap_or_default()
        } else {
            parse_env_entries(&env_entries)?
        }
    } else {
        let default = if env_entries.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| {
                    profile
                        .env
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default()
        } else {
            env_entries.join(",")
        };
        parse_env_entries(&parse_csv_entries(&prompt(
            &mut stdin,
            &mut stdout,
            "Profile env vars KEY=VALUE (comma-separated)",
            &default,
        )?))?
    };

    let binaries_value = if non_interactive {
        if binary_entries.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| profile.binaries.clone())
                .unwrap_or_default()
        } else {
            parse_binary_entries(&binary_entries)?
        }
    } else {
        let default = if binary_entries.is_empty() {
            existing_profile
                .as_ref()
                .map(|profile| format_binary_entries(&profile.binaries))
                .unwrap_or_default()
        } else {
            binary_entries.join(",")
        };
        parse_binary_entries(&parse_csv_entries(&prompt(
            &mut stdin,
            &mut stdout,
            "Profile binaries NAME=PATH (comma-separated)",
            &default,
        )?))?
    };

    let selected_default_profile = if non_interactive {
        default_profile
            .or_else(|| settings.default_profile.clone())
            .unwrap_or_else(|| selected_profile.clone())
    } else {
        let default = default_profile
            .clone()
            .or_else(|| settings.default_profile.clone())
            .unwrap_or_else(|| selected_profile.clone());
        prompt(&mut stdin, &mut stdout, "Default profile", &default)?
    };

    let profile = settings
        .profiles
        .entry(selected_profile.clone())
        .or_default();
    profile.compose_file = Some(resolved_compose_file);
    profile.env_files = env_files_value;
    profile.env = env_value;
    profile.binaries = binaries_value;
    let setup_output = output::SetupOutput {
        settings_path: settings_path.clone(),
        profile: selected_profile.clone(),
        default_profile: selected_default_profile.clone(),
        compose_file: profile.compose_file.clone().unwrap_or_default(),
        env_files: profile.env_files.clone(),
        env: profile.env.clone(),
        binaries: profile.binaries.clone(),
    };
    settings.default_profile = Some(selected_default_profile.clone());
    settings.version = 1;

    write_settings(&settings_path, &settings)?;
    match output::resolve_output_format(format, false) {
        OutputFormat::Text => {
            println!("wrote {}", settings_path.display());
            println!("profile: {}", selected_profile);
            println!("default profile: {}", selected_default_profile);
        }
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&setup_output)
                    .context("failed to serialize setup output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn completions(shell: Shell) -> Result<()> {
    let mut cmd = build_cli_command();
    clap_complete::generate(shell, &mut cmd, "hpc-compose", &mut io::stdout());
    Ok(())
}

fn prompt(
    input: &mut impl BufRead,
    output: &mut impl Write,
    label: &str,
    default: &str,
) -> Result<String> {
    write!(output, "{label} [{default}]: ").ok();
    output.flush().ok();
    let mut line = String::new();
    input
        .read_line(&mut line)
        .context("failed to read interactive input")?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn parse_csv_entries(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_env_entries(entries: &[String]) -> Result<std::collections::BTreeMap<String, String>> {
    let mut out = std::collections::BTreeMap::new();
    for entry in entries {
        let Some((key, value)) = entry.split_once('=') else {
            bail!("invalid env entry '{entry}', expected KEY=VALUE");
        };
        let key = key.trim();
        if key.is_empty() {
            bail!("invalid env entry '{entry}', key must not be empty");
        }
        out.insert(key.to_string(), value.to_string());
    }
    Ok(out)
}

fn parse_binary_entries(entries: &[String]) -> Result<BinaryOverrides> {
    let mut overrides = BinaryOverrides::default();
    for entry in entries {
        let Some((name, value)) = entry.split_once('=') else {
            bail!("invalid binary entry '{entry}', expected NAME=PATH");
        };
        let name = name.trim();
        let value = value.trim();
        if value.is_empty() {
            bail!("invalid binary entry '{entry}', path must not be empty");
        }
        match name {
            "enroot" => overrides.enroot = Some(value.to_string()),
            "sbatch" => overrides.sbatch = Some(value.to_string()),
            "srun" => overrides.srun = Some(value.to_string()),
            "squeue" => overrides.squeue = Some(value.to_string()),
            "sacct" => overrides.sacct = Some(value.to_string()),
            "sstat" => overrides.sstat = Some(value.to_string()),
            "scancel" => overrides.scancel = Some(value.to_string()),
            _ => bail!(
                "invalid binary name '{name}'; supported names: enroot, sbatch, srun, squeue, sacct, sstat, scancel"
            ),
        }
    }
    Ok(overrides)
}

fn format_binary_entries(overrides: &BinaryOverrides) -> String {
    let mut entries = Vec::new();
    if let Some(value) = &overrides.enroot {
        entries.push(format!("enroot={value}"));
    }
    if let Some(value) = &overrides.sbatch {
        entries.push(format!("sbatch={value}"));
    }
    if let Some(value) = &overrides.srun {
        entries.push(format!("srun={value}"));
    }
    if let Some(value) = &overrides.squeue {
        entries.push(format!("squeue={value}"));
    }
    if let Some(value) = &overrides.sacct {
        entries.push(format!("sacct={value}"));
    }
    if let Some(value) = &overrides.sstat {
        entries.push(format!("sstat={value}"));
    }
    if let Some(value) = &overrides.scancel {
        entries.push(format!("scancel={value}"));
    }
    entries.join(",")
}

fn dedup_preserve_order(items: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for item in items {
        if seen.insert(item.clone()) {
            out.push(item);
        }
    }
    out
}

fn absolute_path(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    use clap_complete::Shell;
    use hpc_compose::context::load_settings;

    #[test]
    fn helper_parsers_cover_success_and_error_paths() {
        assert_eq!(
            parse_csv_entries(" .env , .env.dev ,,"),
            vec![".env".to_string(), ".env.dev".to_string()]
        );
        assert_eq!(
            dedup_preserve_order(vec![
                "a".to_string(),
                "b".to_string(),
                "a".to_string(),
                "c".to_string(),
            ]),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        let env = parse_env_entries(&["A=1".to_string(), "B=two".to_string()]).expect("env");
        assert_eq!(env.get("A"), Some(&"1".to_string()));
        assert_eq!(env.get("B"), Some(&"two".to_string()));
        assert!(
            parse_env_entries(&["missing".to_string()])
                .expect_err("missing equals")
                .to_string()
                .contains("expected KEY=VALUE")
        );
        assert!(
            parse_env_entries(&[" =value".to_string()])
                .expect_err("empty key")
                .to_string()
                .contains("key must not be empty")
        );

        let binaries = parse_binary_entries(&[
            "enroot=/bin/enroot".to_string(),
            "sbatch=/bin/sbatch".to_string(),
            "srun=/bin/srun".to_string(),
            "squeue=/bin/squeue".to_string(),
            "sacct=/bin/sacct".to_string(),
            "sstat=/bin/sstat".to_string(),
            "scancel=/bin/scancel".to_string(),
        ])
        .expect("binaries");
        assert_eq!(binaries.enroot.as_deref(), Some("/bin/enroot"));
        assert_eq!(binaries.scancel.as_deref(), Some("/bin/scancel"));
        let formatted = format_binary_entries(&binaries);
        assert!(formatted.contains("enroot=/bin/enroot"));
        assert!(formatted.contains("scancel=/bin/scancel"));
        assert!(
            parse_binary_entries(&["unknown=/bin/x".to_string()])
                .expect_err("unknown binary")
                .to_string()
                .contains("invalid binary name")
        );
        assert!(
            parse_binary_entries(&["srun=".to_string()])
                .expect_err("empty path")
                .to_string()
                .contains("path must not be empty")
        );

        let cwd = Path::new("/tmp/project");
        assert_eq!(
            absolute_path(Path::new("compose.yaml"), cwd),
            cwd.join("compose.yaml")
        );
        assert_eq!(
            absolute_path(Path::new("/tmp/abs"), cwd),
            PathBuf::from("/tmp/abs")
        );
    }

    #[test]
    fn setup_non_interactive_writes_and_reuses_settings() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let settings_path = tmpdir.path().join(".hpc-compose/settings.toml");

        setup(
            Some(settings_path.clone()),
            Some("dev".to_string()),
            None,
            Some("compose.yaml".to_string()),
            vec![
                ".env".to_string(),
                ".env".to_string(),
                ".env.dev".to_string(),
            ],
            vec!["CACHE_DIR=/shared/cache".to_string()],
            vec!["srun=/opt/slurm/bin/srun".to_string()],
            Some("dev".to_string()),
            true,
            None,
        )
        .expect("setup");

        let settings = load_settings(&settings_path).expect("load settings");
        assert_eq!(settings.default_profile.as_deref(), Some("dev"));
        let profile = settings.profiles.get("dev").expect("dev profile");
        assert_eq!(profile.compose_file.as_deref(), Some("compose.yaml"));
        assert_eq!(
            profile.env_files,
            vec![".env".to_string(), ".env.dev".to_string()]
        );
        assert_eq!(
            profile.env.get("CACHE_DIR"),
            Some(&"/shared/cache".to_string())
        );
        assert_eq!(
            profile.binaries.srun.as_deref(),
            Some("/opt/slurm/bin/srun")
        );

        setup(
            Some(settings_path.clone()),
            None,
            Some("dev".to_string()),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
            true,
            None,
        )
        .expect("setup reuse");
        let reused = load_settings(&settings_path).expect("reload settings");
        let reused_profile = reused.profiles.get("dev").expect("dev profile");
        assert_eq!(reused_profile.compose_file.as_deref(), Some("compose.yaml"));
        assert_eq!(
            reused_profile.env_files,
            vec![".env".to_string(), ".env.dev".to_string()]
        );
        assert_eq!(
            reused_profile.binaries.srun.as_deref(),
            Some("/opt/slurm/bin/srun")
        );
    }

    #[test]
    fn completions_emits_supported_shell_output() {
        completions(Shell::Bash).expect("bash completions");
        completions(Shell::Zsh).expect("zsh completions");
    }

    #[test]
    fn init_command_and_prompt_cover_remaining_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let output_path = tmpdir.path().join("compose.yaml");

        new_command(
            Some("dev-python-app".to_string()),
            false,
            None,
            Some("demo-app".to_string()),
            Some("/shared/cache".to_string()),
            output_path.clone(),
            false,
            None,
        )
        .expect("init writes template");
        assert!(output_path.exists());

        new_command(
            None,
            true,
            None,
            None,
            None,
            tmpdir.path().join("ignored.yaml"),
            false,
            None,
        )
        .expect("list templates");
        new_command(
            None,
            false,
            Some("dev-python-app".to_string()),
            None,
            None,
            tmpdir.path().join("ignored.yaml"),
            false,
            None,
        )
        .expect("describe template");

        let mut default_input = Cursor::new(b"\n");
        let mut captured = Vec::new();
        assert_eq!(
            prompt(&mut default_input, &mut captured, "Profile name", "dev")
                .expect("default prompt"),
            "dev"
        );

        let mut explicit_input = Cursor::new(b"prod\n");
        assert_eq!(
            prompt(&mut explicit_input, &mut captured, "Profile name", "dev")
                .expect("explicit prompt"),
            "prod"
        );
    }

    #[test]
    fn init_commands_cover_json_and_setup_output_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let output_path = tmpdir.path().join("compose-json.yaml");
        let settings_path = tmpdir.path().join(".hpc-compose/settings.toml");

        new_command(
            None,
            true,
            None,
            None,
            None,
            tmpdir.path().join("ignored.yaml"),
            false,
            Some(OutputFormat::Json),
        )
        .expect("list templates json");
        new_command(
            None,
            false,
            Some("dev-python-app".to_string()),
            None,
            None,
            tmpdir.path().join("ignored.yaml"),
            false,
            Some(OutputFormat::Json),
        )
        .expect("describe template json");
        new_command(
            Some("dev-python-app".to_string()),
            false,
            None,
            Some("demo-json".to_string()),
            Some("/shared/cache-json".to_string()),
            output_path.clone(),
            false,
            Some(OutputFormat::Json),
        )
        .expect("write template json");
        assert!(output_path.exists());

        setup(
            Some(settings_path.clone()),
            Some("json".to_string()),
            None,
            Some("compose-json.yaml".to_string()),
            vec![".env".to_string()],
            vec!["CACHE_DIR=/shared/cache-json".to_string()],
            vec!["squeue=/opt/slurm/bin/squeue".to_string()],
            Some("json".to_string()),
            true,
            Some(OutputFormat::Json),
        )
        .expect("setup json");
        let settings = load_settings(&settings_path).expect("load settings json");
        assert_eq!(settings.default_profile.as_deref(), Some("json"));
    }
}
