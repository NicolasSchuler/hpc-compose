use std::collections::BTreeSet;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

#[cfg(test)]
use std::path::Path;

use anyhow::{Context, Result, bail};
use clap_complete::Shell;
use hpc_compose::cli::OutputFormat;
use hpc_compose::cli::build_cli_command;
use hpc_compose::context::{
    BinaryOverrides, Settings, repo_adjacent_settings_path, write_settings,
};
use hpc_compose::init::{
    cache_dir_placeholder as init_cache_dir_placeholder, next_commands,
    prompt_for_init_with_cache_dir_default, render_template_with_optional_cache_dir,
    write_initialized_template,
};
use hpc_compose::term;

use crate::output::{common as output_common, init as output_init};

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
        match output_common::resolve_output_format(format) {
            OutputFormat::Text => output_init::print_template_list(),
            OutputFormat::Json => {
                println!(
                    "{}",
                    crate::output::to_pretty_json(&output_init::TemplateListOutput {
                        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                        templates: output_init::template_infos(),
                        cache_dir_required: false,
                        cache_dir_placeholder: init_cache_dir_placeholder().to_string(),
                    })
                    .context("failed to serialize template list output")?
                );
            }
        }
        return Ok(());
    }
    if let Some(template_name) = describe_template {
        match output_common::resolve_output_format(format) {
            OutputFormat::Text => output_init::print_template_description(&template_name)?,
            OutputFormat::Json => {
                println!(
                    "{}",
                    crate::output::to_pretty_json(&output_init::build_template_description(
                        &template_name,
                    )?)
                    .context("failed to serialize template description output")?
                );
            }
        }
        return Ok(());
    }
    let prompt_cache_dir = cache_dir.clone();
    let answers = output_init::resolve_init_answers(template, name, cache_dir, || {
        prompt_for_init_with_cache_dir_default(prompt_cache_dir.as_deref())
    })?;
    let rendered = render_template_with_optional_cache_dir(
        &answers.template_name,
        &answers.app_name,
        answers.cache_dir.as_deref(),
    )?;
    let path = write_initialized_template(&output_path, &rendered, force)?;
    let next_commands = next_commands(&path);
    match output_common::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("wrote {}", path.display());
            for command in next_commands {
                println!("{command}");
            }
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&output_init::TemplateWriteOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
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
    cache_dir: Option<String>,
    login_host: Option<String>,
    login_user: Option<String>,
    default_profile: Option<String>,
    non_interactive: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to determine current working directory")?;
    let settings_path = settings_file_override
        .map(|path| crate::path_util::absolute_path(&path, &cwd))
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

    let cache_dir_value = if non_interactive {
        cache_dir.or_else(|| {
            existing_profile
                .as_ref()
                .and_then(|profile| profile.cache.dir.clone())
        })
    } else {
        let default = cache_dir
            .clone()
            .or_else(|| {
                existing_profile
                    .as_ref()
                    .and_then(|profile| profile.cache.dir.clone())
            })
            .unwrap_or_default();
        let value = prompt(&mut stdin, &mut stdout, "Cache dir", &default)?;
        optional_nonempty(value)?
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

    // Login host/user are flag-driven (no interactive prompt): a provided flag
    // wins, otherwise the profile keeps its current value.
    let login_host_value = login_host.or_else(|| {
        existing_profile
            .as_ref()
            .and_then(|profile| profile.login_host.clone())
    });
    let login_user_value = login_user.or_else(|| {
        existing_profile
            .as_ref()
            .and_then(|profile| profile.login_user.clone())
    });

    let profile = settings
        .profiles
        .entry(selected_profile.clone())
        .or_default();
    profile.compose_file = Some(resolved_compose_file);
    profile.env_files = env_files_value;
    profile.env = env_value;
    profile.binaries = binaries_value;
    profile.cache.dir = cache_dir_value;
    profile.login_host = login_host_value;
    profile.login_user = login_user_value;
    let setup_output = output_init::SetupOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        settings_path: settings_path.clone(),
        profile: selected_profile.clone(),
        default_profile: selected_default_profile.clone(),
        compose_file: profile.compose_file.clone().unwrap_or_default(),
        env_files: profile.env_files.clone(),
        env: profile.env.clone(),
        binaries: profile.binaries.clone(),
        cache_dir: profile.cache.dir.clone(),
        login_host: profile.login_host.clone(),
        login_user: profile.login_user.clone(),
    };
    settings.default_profile = Some(selected_default_profile.clone());
    settings.version = 1;

    write_settings(&settings_path, &settings)?;
    match output_common::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("wrote {}", settings_path.display());
            println!("profile: {}", selected_profile);
            println!("default profile: {}", selected_default_profile);
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&setup_output)
                    .context("failed to serialize setup output")?
            );
        }
    }
    Ok(())
}

pub(crate) fn completions(shell: Shell) -> Result<()> {
    completions_to_writer(shell, &mut io::stdout())
}

fn completions_to_writer(shell: Shell, writer: &mut impl Write) -> Result<()> {
    let output = std::thread::Builder::new()
        .name("hpc-compose-completions".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let mut cmd = build_cli_command();
            let mut output = Vec::new();
            clap_complete::generate(shell, &mut cmd, "hpc-compose", &mut output);
            output
        })
        .context("failed to spawn completion generator")?
        .join()
        .map_err(|_| anyhow::anyhow!("completion generator panicked"))?;
    let output = add_dynamic_completion_hooks(shell, output)?;
    writer
        .write_all(&output)
        .context("failed to write shell completions")?;
    Ok(())
}

fn add_dynamic_completion_hooks(shell: Shell, output: Vec<u8>) -> Result<Vec<u8>> {
    match shell {
        Shell::Bash => {
            let script =
                String::from_utf8(output).context("bash completions were not valid UTF-8")?;
            Ok(inject_bash_dynamic_completion(script).into_bytes())
        }
        Shell::Zsh => {
            let script =
                String::from_utf8(output).context("zsh completions were not valid UTF-8")?;
            Ok(inject_zsh_dynamic_completion(script).into_bytes())
        }
        Shell::Fish => {
            let mut script =
                String::from_utf8(output).context("fish completions were not valid UTF-8")?;
            script.push_str(FISH_DYNAMIC_COMPLETIONS);
            Ok(script.into_bytes())
        }
        _ => Ok(output),
    }
}

fn inject_bash_dynamic_completion(script: String) -> String {
    let mut script = script.replacen("_hpc-compose() {", "_hpc-compose_static() {", 1);
    script.push_str(BASH_DYNAMIC_COMPLETIONS);
    script
}

fn inject_zsh_dynamic_completion(script: String) -> String {
    let mut script = script.replacen("_hpc-compose() {", "_hpc-compose_static() {", 1);
    script.push_str(ZSH_DYNAMIC_COMPLETIONS);
    script
}

const BASH_DYNAMIC_COMPLETIONS: &str = r#"

__hpc_compose_dynamic_command() {
    local -a commands
    local word skip_next=""
    for word in "${COMP_WORDS[@]:1:COMP_CWORD-1}"; do
        if [[ -n "$skip_next" ]]; then
            skip_next=""
            continue
        fi
        case "$word" in
            --) break ;;
            -f|--file|--profile|--settings-file|--color|--format|--output|--into|--timeout|--log-file|--queue-warn-after|--poll-interval|--wait-until|--at|--env|--set|--remove)
                skip_next=1
                continue
                ;;
        esac
        [[ "$word" == -* ]] && continue
        commands+=("$word")
    done
    printf '%s\n' "${commands[*]}"
}

__hpc_compose_dynamic_kind() {
    local command_path="$1"
    local flag="$2"
    case "$flag" in
        --partition) printf '%s\n' partition ;;
        --qos) printf '%s\n' qos ;;
        --resources) printf '%s\n' resources ;;
        --service) printf '%s\n' service ;;
        --job-id|--after-job) printf '%s\n' job-id ;;
        --tag|--remove)
            case "$command_path" in
                experiment|experiment\ *) printf '%s\n' tag ;;
            esac
            ;;
        --sweep-id|--sweep|--across) printf '%s\n' sweep-id ;;
        --bundle)
            case "$command_path" in
                experiment\ bundle*) printf '%s\n' bundle ;;
            esac
            ;;
    esac
}

__hpc_compose_dynamic_arg() {
    local flag="$1"
    local i
    for ((i = 1; i < COMP_CWORD; i++)); do
        case "${COMP_WORDS[i]}" in
            "$flag")
                if ((i + 1 < COMP_CWORD)); then
                    printf '%s\n' "${COMP_WORDS[$((i + 1))]}"
                    return
                fi
                ;;
            "$flag"=*)
                printf '%s\n' "${COMP_WORDS[i]#*=}"
                return
                ;;
            -f*)
                if [[ "$flag" == "-f" && "${COMP_WORDS[i]}" != "-f" ]]; then
                    printf '%s\n' "${COMP_WORDS[i]#-f}"
                    return
                fi
                ;;
        esac
    done
}

__hpc_compose_experiment_bundle_job_id() {
    local command_path="$1"
    [[ "$command_path" == "experiment bundle"* ]] || return
    local word previous=""
    for word in "${COMP_WORDS[@]:1:COMP_CWORD-1}"; do
        if [[ -n "$previous" ]]; then
            previous=""
            continue
        fi
        case "$word" in
            experiment|bundle) continue ;;
            --) break ;;
            -f|--file|--profile|--settings-file|--color|--format|--output|--into)
                previous=skip
                continue
                ;;
            --*) continue ;;
            -*) continue ;;
        esac
        printf '%s\n' "$word"
        return
    done
}

__hpc_compose_dynamic_values() {
    local kind="$1"
    local cur="$2"
    local file profile settings job_id
    local -a globals args
    file="$(__hpc_compose_dynamic_arg --file)"
    if [[ -z "$file" ]]; then
        file="$(__hpc_compose_dynamic_arg -f)"
    fi
    profile="$(__hpc_compose_dynamic_arg --profile)"
    settings="$(__hpc_compose_dynamic_arg --settings-file)"
    job_id="$(__hpc_compose_dynamic_arg --job-id)"
    if [[ -z "$job_id" ]]; then
        job_id="$(__hpc_compose_experiment_bundle_job_id "$(__hpc_compose_dynamic_command)")"
    fi
    [[ -n "$profile" ]] && globals+=(--profile "$profile")
    [[ -n "$settings" ]] && globals+=(--settings-file "$settings")
    args=(__complete-values --kind "$kind" --prefix "$cur")
    [[ -n "$file" ]] && args+=(--file "$file")
    [[ -n "$job_id" ]] && args+=(--job-id "$job_id")
    command hpc-compose "${globals[@]}" "${args[@]}" 2>/dev/null
}

_hpc-compose() {
    local cur prev command_path kind
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[$((COMP_CWORD - 1))]}"
    command_path="$(__hpc_compose_dynamic_command)"
    kind="$(__hpc_compose_dynamic_kind "$command_path" "$prev")"
    if [[ -n "$kind" ]]; then
        COMPREPLY=()
        while IFS= read -r value; do
            COMPREPLY+=("$value")
        done < <(__hpc_compose_dynamic_values "$kind" "$cur")
        if ((${#COMPREPLY[@]} > 0)); then
            return 0
        fi
    fi
    _hpc-compose_static "$@"
}
"#;

const ZSH_DYNAMIC_COMPLETIONS: &str = r#"

__hpc_compose_dynamic_command() {
    local -a commands
    local word skip_next
    for word in "${words[@]:1:CURRENT-2}"; do
        if [[ -n "$skip_next" ]]; then
            skip_next=
            continue
        fi
        case "$word" in
            --) break ;;
            -f|--file|--profile|--settings-file|--color|--format|--output|--into|--timeout|--log-file|--queue-warn-after|--poll-interval|--wait-until|--at|--env|--set|--remove)
                skip_next=1
                continue
                ;;
        esac
        [[ "$word" == -* ]] && continue
        commands+=("$word")
    done
    print -r -- "${commands[*]}"
}

__hpc_compose_dynamic_kind() {
    local command_path="$1"
    local flag="$2"
    case "$flag" in
        --partition) print -r -- partition ;;
        --qos) print -r -- qos ;;
        --resources) print -r -- resources ;;
        --service) print -r -- service ;;
        --job-id|--after-job) print -r -- job-id ;;
        --tag|--remove)
            case "$command_path" in
                experiment|experiment\ *) print -r -- tag ;;
            esac
            ;;
        --sweep-id|--sweep|--across) print -r -- sweep-id ;;
        --bundle)
            case "$command_path" in
                experiment\ bundle*) print -r -- bundle ;;
            esac
            ;;
    esac
}

__hpc_compose_zsh_arg() {
    local flag="$1"
    local i=1
    while ((i < CURRENT)); do
        case "${words[i]}" in
            "$flag")
                if ((i + 1 < CURRENT)); then
                    print -r -- "${words[$((i + 1))]}"
                    return
                fi
                ;;
            "$flag"=*)
                print -r -- "${words[i]#*=}"
                return
                ;;
            -f*)
                if [[ "$flag" == "-f" && "${words[i]}" != "-f" ]]; then
                    print -r -- "${words[i]#-f}"
                    return
                fi
                ;;
        esac
        ((i++))
    done
}

__hpc_compose_experiment_bundle_job_id() {
    local command_path="$1"
    [[ "$command_path" == "experiment bundle"* ]] || return
    local word skip_next
    for word in "${words[@]:1:CURRENT-2}"; do
        if [[ -n "$skip_next" ]]; then
            skip_next=
            continue
        fi
        case "$word" in
            experiment|bundle) continue ;;
            --) break ;;
            -f|--file|--profile|--settings-file|--color|--format|--output|--into)
                skip_next=1
                continue
                ;;
            --*|-*) continue ;;
        esac
        print -r -- "$word"
        return
    done
}

__hpc_compose_zsh_values() {
    local kind="$1"
    local file profile settings job_id
    local -a globals args
    file="$(__hpc_compose_zsh_arg --file)"
    if [[ -z "$file" ]]; then
        file="$(__hpc_compose_zsh_arg -f)"
    fi
    profile="$(__hpc_compose_zsh_arg --profile)"
    settings="$(__hpc_compose_zsh_arg --settings-file)"
    job_id="$(__hpc_compose_zsh_arg --job-id)"
    if [[ -z "$job_id" ]]; then
        job_id="$(__hpc_compose_experiment_bundle_job_id "$(__hpc_compose_dynamic_command)")"
    fi
    [[ -n "$profile" ]] && globals+=(--profile "$profile")
    [[ -n "$settings" ]] && globals+=(--settings-file "$settings")
    args=(__complete-values --kind "$kind" --prefix "$PREFIX")
    [[ -n "$file" ]] && args+=(--file "$file")
    [[ -n "$job_id" ]] && args+=(--job-id "$job_id")
    command hpc-compose "${globals[@]}" "${args[@]}" 2>/dev/null
}

_hpc-compose() {
    local prev command_path kind
    prev="${words[$((CURRENT - 1))]}"
    command_path="$(__hpc_compose_dynamic_command)"
    kind="$(__hpc_compose_dynamic_kind "$command_path" "$prev")"
    if [[ -n "$kind" ]]; then
        local -a values
        values=("${(@f)$(__hpc_compose_zsh_values "$kind")}")
        if ((${#values} > 0)); then
            compadd -- "${values[@]}"
            return
        fi
    fi
    _hpc-compose_static "$@"
}
compdef _hpc-compose hpc-compose
"#;

const FISH_DYNAMIC_COMPLETIONS: &str = r#"

function __hpc_compose_dynamic_arg
    set -l flag $argv[1]
    set -l words (commandline -opc)
    set -l count_words (count $words)
    for i in (seq 1 $count_words)
        switch $words[$i]
            case $flag
                set -l next (math $i + 1)
                if test $next -le $count_words
                    printf '%s\n' $words[$next]
                    return
                end
            case "$flag=*"
                string replace -r "^$flag=" "" -- $words[$i]
                return
            case "-f*"
                if test "$flag" = "-f"; and test "$words[$i]" != "-f"
                    string replace -r "^-f" "" -- $words[$i]
                    return
                end
        end
    end
end

function __hpc_compose_dynamic_contains_command --argument-names wanted
    set -l words (commandline -opc)
    string match -q -- "* $wanted *" " $words "
end

function __hpc_compose_experiment_bundle_job_id
    __hpc_compose_dynamic_contains_command "experiment bundle"; or return
    set -l words (commandline -opc)
    set -l skip_next 0
    for word in $words[2..-1]
        if test "$skip_next" = 1
            set skip_next 0
            continue
        end
        switch $word
            case experiment bundle
                continue
            case --file -f --profile --settings-file --color --format --output --into
                set skip_next 1
                continue
            case "--*" "-*"
                continue
        end
        printf '%s\n' $word
        return
    end
end

function __hpc_compose_complete_values
    set -l kind $argv[1]
    set -l token (commandline -ct)
    set -l file (__hpc_compose_dynamic_arg --file)
    if test -z "$file"
        set file (__hpc_compose_dynamic_arg -f)
    end
    set -l profile (__hpc_compose_dynamic_arg --profile)
    set -l settings (__hpc_compose_dynamic_arg --settings-file)
    set -l job_id (__hpc_compose_dynamic_arg --job-id)
    if test -z "$job_id"
        set job_id (__hpc_compose_experiment_bundle_job_id)
    end
    set -l globals
    set -l args __complete-values --kind $kind --prefix "$token"
    if test -n "$profile"
        set globals $globals --profile "$profile"
    end
    if test -n "$settings"
        set globals $globals --settings-file "$settings"
    end
    if test -n "$file"
        set args $args --file "$file"
    end
    if test -n "$job_id"
        set args $args --job-id "$job_id"
    end
    command hpc-compose $globals $args 2>/dev/null
end

complete -c hpc-compose -l partition -f -n "__hpc_compose_dynamic_contains_command up" -a "(__hpc_compose_complete_values partition)"
complete -c hpc-compose -l qos -f -n "__hpc_compose_dynamic_contains_command up" -a "(__hpc_compose_complete_values qos)"
complete -c hpc-compose -l resources -f -n "__hpc_compose_dynamic_contains_command up" -a "(__hpc_compose_complete_values resources)"
complete -c hpc-compose -l service -f -n "__hpc_compose_dynamic_contains_command experiment" -a "(__hpc_compose_complete_values service)"
complete -c hpc-compose -l job-id -f -n "__hpc_compose_dynamic_contains_command experiment" -a "(__hpc_compose_complete_values job-id)"
complete -c hpc-compose -l after-job -f -n "__hpc_compose_dynamic_contains_command up" -a "(__hpc_compose_complete_values job-id)"
complete -c hpc-compose -l tag -f -n "__hpc_compose_dynamic_contains_command experiment" -a "(__hpc_compose_complete_values tag)"
complete -c hpc-compose -l remove -f -n "__hpc_compose_dynamic_contains_command experiment" -a "(__hpc_compose_complete_values tag)"
complete -c hpc-compose -l sweep-id -f -n "__hpc_compose_dynamic_contains_command sweep" -a "(__hpc_compose_complete_values sweep-id)"
complete -c hpc-compose -l sweep -f -n "__hpc_compose_dynamic_contains_command experiment" -a "(__hpc_compose_complete_values sweep-id)"
complete -c hpc-compose -l across -f -n "__hpc_compose_dynamic_contains_command sweep" -a "(__hpc_compose_complete_values sweep-id)"
complete -c hpc-compose -l bundle -f -n "__hpc_compose_dynamic_contains_command experiment bundle" -a "(__hpc_compose_complete_values bundle)"
"#;

fn prompt(
    input: &mut impl BufRead,
    output: &mut impl Write,
    label: &str,
    default: &str,
) -> Result<String> {
    write!(
        output,
        "{} [{}]: ",
        term::styled_bold(label),
        term::styled_dim(default)
    )
    .ok();
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

fn optional_nonempty(value: String) -> Result<Option<String>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed != value {
        bail!("cache dir must not have leading or trailing whitespace");
    }
    Ok(Some(value))
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
            "apptainer" => overrides.apptainer = Some(value.to_string()),
            "singularity" => overrides.singularity = Some(value.to_string()),
            "salloc" => overrides.salloc = Some(value.to_string()),
            "sbatch" => overrides.sbatch = Some(value.to_string()),
            "srun" => overrides.srun = Some(value.to_string()),
            "scontrol" => overrides.scontrol = Some(value.to_string()),
            "sinfo" => overrides.sinfo = Some(value.to_string()),
            "squeue" => overrides.squeue = Some(value.to_string()),
            "sacct" => overrides.sacct = Some(value.to_string()),
            "sstat" => overrides.sstat = Some(value.to_string()),
            "scancel" => overrides.scancel = Some(value.to_string()),
            "sshare" => overrides.sshare = Some(value.to_string()),
            "sprio" => overrides.sprio = Some(value.to_string()),
            "ssh" => overrides.ssh = Some(value.to_string()),
            "rsync" => overrides.rsync = Some(value.to_string()),
            _ => bail!(
                "invalid binary name '{name}'; supported names: enroot, apptainer, singularity, salloc, sbatch, srun, scontrol, sinfo, squeue, sacct, sstat, scancel, sshare, sprio, ssh, rsync"
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
    if let Some(value) = &overrides.apptainer {
        entries.push(format!("apptainer={value}"));
    }
    if let Some(value) = &overrides.singularity {
        entries.push(format!("singularity={value}"));
    }
    if let Some(value) = &overrides.salloc {
        entries.push(format!("salloc={value}"));
    }
    if let Some(value) = &overrides.sbatch {
        entries.push(format!("sbatch={value}"));
    }
    if let Some(value) = &overrides.srun {
        entries.push(format!("srun={value}"));
    }
    if let Some(value) = &overrides.scontrol {
        entries.push(format!("scontrol={value}"));
    }
    if let Some(value) = &overrides.sinfo {
        entries.push(format!("sinfo={value}"));
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
    if let Some(value) = &overrides.sshare {
        entries.push(format!("sshare={value}"));
    }
    if let Some(value) = &overrides.sprio {
        entries.push(format!("sprio={value}"));
    }
    if let Some(value) = &overrides.ssh {
        entries.push(format!("ssh={value}"));
    }
    if let Some(value) = &overrides.rsync {
        entries.push(format!("rsync={value}"));
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
            "salloc=/bin/salloc".to_string(),
            "sbatch=/bin/sbatch".to_string(),
            "srun=/bin/srun".to_string(),
            "squeue=/bin/squeue".to_string(),
            "sacct=/bin/sacct".to_string(),
            "sstat=/bin/sstat".to_string(),
            "scancel=/bin/scancel".to_string(),
            "sshare=/bin/sshare".to_string(),
            "sprio=/bin/sprio".to_string(),
        ])
        .expect("binaries");
        assert_eq!(binaries.enroot.as_deref(), Some("/bin/enroot"));
        assert_eq!(binaries.salloc.as_deref(), Some("/bin/salloc"));
        assert_eq!(binaries.scancel.as_deref(), Some("/bin/scancel"));
        assert_eq!(binaries.sshare.as_deref(), Some("/bin/sshare"));
        assert_eq!(binaries.sprio.as_deref(), Some("/bin/sprio"));
        let formatted = format_binary_entries(&binaries);
        assert!(formatted.contains("enroot=/bin/enroot"));
        assert!(formatted.contains("salloc=/bin/salloc"));
        assert!(formatted.contains("scancel=/bin/scancel"));
        assert!(formatted.contains("sshare=/bin/sshare"));
        assert!(formatted.contains("sprio=/bin/sprio"));
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
            crate::path_util::absolute_path(Path::new("compose.yaml"), cwd),
            cwd.join("compose.yaml")
        );
        assert_eq!(
            crate::path_util::absolute_path(Path::new("/tmp/abs"), cwd),
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
            None,
            None,
            None,
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
            None,
            None,
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
        let mut bash = Vec::new();
        completions_to_writer(Shell::Bash, &mut bash).expect("bash completions");
        let bash = String::from_utf8_lossy(&bash);
        assert!(bash.contains("hpc-compose"));
        assert!(bash.contains("_hpc-compose_static"));
        assert!(bash.contains("__complete-values --kind"));
        assert!(bash.contains("COMPREPLY=()"));
        assert!(!bash.contains("mapfile"));
        assert!(bash.contains(r#"experiment\ bundle*)"#));
        assert!(!bash.contains(" __complete-values completions"));

        let mut zsh = Vec::new();
        completions_to_writer(Shell::Zsh, &mut zsh).expect("zsh completions");
        let zsh = String::from_utf8_lossy(&zsh);
        assert!(zsh.contains("#compdef hpc-compose"));
        assert!(zsh.contains("_hpc-compose_static"));
        assert!(zsh.contains("__complete-values --kind"));
        assert!(!zsh.contains("'__complete-values:'"));

        let mut fish = Vec::new();
        completions_to_writer(Shell::Fish, &mut fish).expect("fish completions");
        let fish = String::from_utf8_lossy(&fish);
        assert!(fish.contains("__hpc_compose_complete_values"));
        assert!(fish.contains("__complete-values --kind"));
        assert!(
            fish.contains(r#"-l tag -f -n "__hpc_compose_dynamic_contains_command experiment""#)
        );
        assert!(fish.contains(
            r#"-l bundle -f -n "__hpc_compose_dynamic_contains_command experiment bundle""#
        ));
        assert!(!fish.contains(r#"-f -a "__complete-values""#));
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
            None,
            None,
            None,
            Some("json".to_string()),
            true,
            Some(OutputFormat::Json),
        )
        .expect("setup json");
        let settings = load_settings(&settings_path).expect("load settings json");
        assert_eq!(settings.default_profile.as_deref(), Some("json"));
    }
}
