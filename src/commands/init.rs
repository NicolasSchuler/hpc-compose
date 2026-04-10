use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap_complete::Shell;
use hpc_compose::cli::build_cli_command;
use hpc_compose::context::{
    BinaryOverrides, Settings, repo_adjacent_settings_path, write_settings,
};
use hpc_compose::init::{
    next_commands, prompt_for_init, render_template, write_initialized_template,
};

use crate::output;

#[allow(clippy::too_many_arguments)]
pub(crate) fn init(
    template: Option<String>,
    list_templates: bool,
    describe_template: Option<String>,
    name: Option<String>,
    cache_dir: Option<String>,
    output_path: PathBuf,
    force: bool,
) -> Result<()> {
    if list_templates {
        output::print_template_list();
        return Ok(());
    }
    if let Some(template_name) = describe_template {
        output::print_template_description(&template_name)?;
        return Ok(());
    }
    let answers = output::resolve_init_answers(template, name, cache_dir, prompt_for_init)?;
    let rendered = render_template(
        &answers.template_name,
        &answers.app_name,
        &answers.cache_dir,
    )?;
    let path = write_initialized_template(&output_path, &rendered, force)?;
    println!("wrote {}", path.display());
    for command in next_commands(&path) {
        println!("{command}");
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
    settings.default_profile = Some(selected_default_profile.clone());
    settings.version = 1;

    write_settings(&settings_path, &settings)?;
    println!("wrote {}", settings_path.display());
    println!("profile: {}", selected_profile);
    println!("default profile: {}", selected_default_profile);
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
