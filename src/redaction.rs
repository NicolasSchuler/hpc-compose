//! Centralized redaction of sensitive interpolation and environment values.
//!
//! A value is treated as sensitive if it was resolved through the top-level
//! `secrets:` block ([`ValueSource::Secret`]) — structural, name-independent
//! redaction — or if its name matches a conservative sensitive-name heuristic.
//! Keeping this in one place ensures `config`, `context`, and inspect output
//! all agree on what is hidden.

use std::collections::{BTreeMap, BTreeSet};

use hpc_compose::context::ValueSource;
use hpc_compose::planner::ExecutionSpec;
use hpc_compose::prepare::RuntimePlan;
use serde::Serialize;

/// Substrings (case-insensitive) that mark an environment name as sensitive
/// when it has not been declared as a secret.
const SENSITIVE_NAME_NEEDLES: &[&str] = &[
    "SECRET",
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "API_KEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
    "CREDENTIAL",
    "AUTH",
    "COOKIE",
    "SESSION",
    "BEARER",
];

/// Returns `true` when *name* matches the sensitive-name heuristic.
#[must_use]
pub fn is_sensitive_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SENSITIVE_NAME_NEEDLES
        .iter()
        .any(|needle| upper.contains(needle))
}

/// Returns `true` when a value with *name* and optional *source* must be
/// redacted. A [`ValueSource::Secret`] is always sensitive regardless of name.
#[must_use]
pub fn is_sensitive(name: &str, source: Option<ValueSource>) -> bool {
    match source {
        Some(ValueSource::Secret) => true,
        _ => is_sensitive_name(name),
    }
}

/// Returns the redacted value for a single env entry unless values are shown.
#[must_use]
pub fn redact_value(
    name: &str,
    value: &str,
    source: Option<ValueSource>,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> String {
    if show_values {
        return value.to_string();
    }
    if is_sensitive(name, source) || secret_values.contains(value) {
        "<redacted>".to_string()
    } else {
        value.to_string()
    }
}

/// Redacts sensitive values in a service `environment` map. A value is
/// redacted when its key matches the sensitive-name heuristic, or when the
/// value exactly matches a known resolved secret value (so
/// `TOKEN: ${hf_token}` is hidden even when the key is benign).
#[must_use]
pub fn redact_env_map(
    map: &BTreeMap<String, String>,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> BTreeMap<String, String> {
    map.iter()
        .map(|(key, value)| {
            (
                key.clone(),
                redact_value(key, value, None, secret_values, show_values),
            )
        })
        .collect()
}

/// Redacts secret values from the runtime-plan strings that text renderers may
/// print directly, such as resolved argv, environment values, mounts, and
/// prepare commands.
#[must_use]
pub fn redacted_runtime_plan(
    plan: &RuntimePlan,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> RuntimePlan {
    if show_values {
        return plan.clone();
    }
    let mut plan = plan.clone();
    for service in &mut plan.ordered_services {
        service.execution = redact_execution(&service.execution, secret_values);
        service.environment = service
            .environment
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    redact_value(key, value, None, secret_values, false),
                )
            })
            .collect();
        service.volumes = service
            .volumes
            .iter()
            .map(|value| redact_freeform_string(value, secret_values, false))
            .collect();
        service.working_dir = service
            .working_dir
            .as_deref()
            .map(|value| redact_freeform_string(value, secret_values, false));
        if let Some(prepare) = &mut service.prepare {
            prepare.commands = prepare
                .commands
                .iter()
                .map(|value| redact_freeform_string(value, secret_values, false))
                .collect();
            prepare.mounts = prepare
                .mounts
                .iter()
                .map(|value| redact_freeform_string(value, secret_values, false))
                .collect();
            prepare.env = prepare
                .env
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        redact_value(key, value, None, secret_values, false),
                    )
                })
                .collect();
        }
    }
    plan
}

fn redact_execution(execution: &ExecutionSpec, secret_values: &BTreeSet<String>) -> ExecutionSpec {
    match execution {
        ExecutionSpec::ImageDefault => ExecutionSpec::ImageDefault,
        ExecutionSpec::Shell(value) => {
            ExecutionSpec::Shell(redact_freeform_string(value, secret_values, false))
        }
        ExecutionSpec::Exec(argv) => ExecutionSpec::Exec(
            argv.iter()
                .map(|value| redact_freeform_string(value, secret_values, false))
                .collect(),
        ),
    }
}

/// Redacts known secret substrings from a free-form string unless values are
/// explicitly shown.
#[must_use]
pub fn redact_freeform_string(
    value: &str,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> String {
    if show_values {
        value.to_string()
    } else {
        redact_secret_substrings(value, secret_values)
    }
}

/// Serializes a value to JSON and redacts sensitive strings anywhere in that
/// diagnostic surface.
pub fn redacted_json_value<T>(
    value: &T,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> serde_json::Result<serde_json::Value>
where
    T: Serialize,
{
    let mut value = serde_json::to_value(value)?;
    redact_json_value(&mut value, secret_values, show_values);
    Ok(value)
}

/// Serializes a value to YAML and redacts sensitive strings anywhere in that
/// diagnostic surface.
pub fn redacted_yaml_value<T>(
    value: &T,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) -> serde_norway::Result<serde_norway::Value>
where
    T: Serialize,
{
    let mut value = serde_norway::to_value(value)?;
    redact_yaml_value(&mut value, secret_values, show_values);
    Ok(value)
}

/// Redacts sensitive strings in a JSON value in place.
pub fn redact_json_value(
    value: &mut serde_json::Value,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) {
    if show_values {
        return;
    }
    redact_json_value_inner(value, secret_values, None);
}

/// Redacts sensitive strings in a YAML value in place.
pub fn redact_yaml_value(
    value: &mut serde_norway::Value,
    secret_values: &BTreeSet<String>,
    show_values: bool,
) {
    if show_values {
        return;
    }
    redact_yaml_value_inner(value, secret_values, None);
}

/// Collects the concrete values of interpolation variables whose source is
/// [`ValueSource::Secret`]. Used for value-equality redaction of service env
/// entries that reference a secret under a benign key.
#[must_use]
pub fn secret_value_set(
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

fn redact_json_value_inner(
    value: &mut serde_json::Value,
    secret_values: &BTreeSet<String>,
    parent_key: Option<&str>,
) {
    match value {
        serde_json::Value::String(current) => {
            *current = redact_string_for_key(parent_key, current, secret_values);
        }
        serde_json::Value::Array(items) => {
            redact_json_env_pairs(items, parent_key, secret_values);
            for item in items {
                redact_json_value_inner(item, secret_values, None);
            }
        }
        serde_json::Value::Object(entries) => {
            for (key, child) in entries {
                redact_json_value_inner(child, secret_values, Some(key));
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn redact_json_env_pairs(
    items: &mut [serde_json::Value],
    parent_key: Option<&str>,
    secret_values: &BTreeSet<String>,
) {
    if !matches!(parent_key, Some("environment" | "env")) {
        return;
    }
    for item in items {
        let serde_json::Value::Array(pair) = item else {
            continue;
        };
        let Some(env_key) = pair.first().and_then(serde_json::Value::as_str) else {
            continue;
        };
        let env_key = env_key.to_string();
        let Some(serde_json::Value::String(env_value)) = pair.get_mut(1) else {
            continue;
        };
        *env_value = redact_string_for_key(Some(&env_key), env_value, secret_values);
    }
}

fn redact_yaml_value_inner(
    value: &mut serde_norway::Value,
    secret_values: &BTreeSet<String>,
    parent_key: Option<&str>,
) {
    match value {
        serde_norway::Value::String(current) => {
            *current = redact_string_for_key(parent_key, current, secret_values);
        }
        serde_norway::Value::Sequence(items) => {
            redact_yaml_env_pairs(items, parent_key, secret_values);
            for item in items {
                redact_yaml_value_inner(item, secret_values, None);
            }
        }
        serde_norway::Value::Mapping(entries) => {
            for (key, child) in entries {
                redact_yaml_value_inner(child, secret_values, key.as_str());
            }
        }
        serde_norway::Value::Tagged(tagged) => {
            redact_yaml_value_inner(&mut tagged.value, secret_values, parent_key);
        }
        serde_norway::Value::Null
        | serde_norway::Value::Bool(_)
        | serde_norway::Value::Number(_) => {}
    }
}

fn redact_yaml_env_pairs(
    items: &mut [serde_norway::Value],
    parent_key: Option<&str>,
    secret_values: &BTreeSet<String>,
) {
    if !matches!(parent_key, Some("environment" | "env")) {
        return;
    }
    for item in items {
        let serde_norway::Value::Sequence(pair) = item else {
            continue;
        };
        let Some(env_key) = pair.first().and_then(serde_norway::Value::as_str) else {
            continue;
        };
        let env_key = env_key.to_string();
        let Some(serde_norway::Value::String(env_value)) = pair.get_mut(1) else {
            continue;
        };
        *env_value = redact_string_for_key(Some(&env_key), env_value, secret_values);
    }
}

fn redact_string_for_key(
    key: Option<&str>,
    value: &str,
    secret_values: &BTreeSet<String>,
) -> String {
    if key.is_some_and(is_sensitive_name) {
        "<redacted>".to_string()
    } else {
        redact_secret_substrings(value, secret_values)
    }
}

fn redact_secret_substrings(value: &str, secret_values: &BTreeSet<String>) -> String {
    let mut redacted = value.to_string();
    let mut secrets = secret_values
        .iter()
        .filter(|secret| !secret.is_empty())
        .collect::<Vec<_>>();
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    for secret in secrets {
        redacted = redacted.replace(secret, "<redacted>");
    }
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_heuristic_flags_common_sensitive_names() {
        assert!(is_sensitive_name("API_KEY"));
        assert!(is_sensitive_name("db_password"));
        assert!(is_sensitive_name("MY_TOKEN"));
        assert!(!is_sensitive_name("PATH"));
        assert!(!is_sensitive_name("home_dir"));
    }

    #[test]
    fn secret_source_overrides_benign_name() {
        assert!(is_sensitive("workspace", Some(ValueSource::Secret)));
        assert!(!is_sensitive("workspace", Some(ValueSource::ProcessEnv)));
        assert!(!is_sensitive("workspace", None));
    }

    #[test]
    fn redact_env_map_uses_name_and_value_match() {
        let mut map = BTreeMap::new();
        map.insert("API_KEY".to_string(), "hunter2".to_string());
        map.insert("MODEL".to_string(), "llama".to_string());
        map.insert("NOTEBOOK".to_string(), "super-secret-value".to_string());
        let mut secret_values = BTreeSet::new();
        secret_values.insert("super-secret-value".to_string());

        let redacted = redact_env_map(&map, &secret_values, false);
        assert_eq!(redacted["API_KEY"], "<redacted>");
        assert_eq!(redacted["MODEL"], "llama");
        assert_eq!(
            redacted["NOTEBOOK"], "<redacted>",
            "value matching a secret must redact regardless of key name"
        );
    }

    #[test]
    fn show_values_disables_redaction() {
        let mut map = BTreeMap::new();
        map.insert("API_KEY".to_string(), "hunter2".to_string());
        let redacted = redact_env_map(&map, &BTreeSet::new(), true);
        assert_eq!(redacted["API_KEY"], "hunter2");
    }

    #[test]
    fn secret_value_set_collects_only_secret_sourced_values() {
        let mut vars = BTreeMap::new();
        vars.insert("hf_token".to_string(), "abc".to_string());
        vars.insert("PATH".to_string(), "/bin".to_string());
        let mut sources = BTreeMap::new();
        sources.insert("hf_token".to_string(), ValueSource::Secret);
        sources.insert("PATH".to_string(), ValueSource::ProcessEnv);
        let set = secret_value_set(&vars, &sources);
        assert!(set.contains("abc"));
        assert!(!set.contains("/bin"));
    }

    #[test]
    fn redacted_json_value_replaces_secret_substrings_outside_env() {
        let mut secret_values = BTreeSet::new();
        secret_values.insert("hf-secret-value-123".to_string());
        let value = serde_json::json!({
            "services": {
                "app": {
                    "command": ["curl", "Authorization: Bearer hf-secret-value-123"]
                }
            }
        });

        let redacted = redacted_json_value(&value, &secret_values, false).expect("redact json");
        let text = serde_json::to_string(&redacted).expect("json");
        assert!(text.contains("Authorization: Bearer <redacted>"));
        assert!(!text.contains("hf-secret-value-123"));
    }

    #[test]
    fn redacted_yaml_value_redacts_env_pairs_by_name() {
        #[derive(Serialize)]
        struct RuntimeLike {
            environment: Vec<(String, String)>,
        }

        let value = RuntimeLike {
            environment: vec![
                ("API_TOKEN".to_string(), "token-value".to_string()),
                ("MODEL".to_string(), "llama".to_string()),
            ],
        };
        let redacted =
            redacted_yaml_value(&value, &BTreeSet::new(), false).expect("redact yaml value");

        let text = serde_norway::to_string(&redacted).expect("yaml");
        assert!(text.contains("<redacted>"));
        assert!(!text.contains("token-value"));
        assert!(text.contains("llama"));
    }
}
