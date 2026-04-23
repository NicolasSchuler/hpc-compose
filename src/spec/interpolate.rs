use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_norway::Value;

pub(super) type InterpolationVars = BTreeMap<String, String>;

pub(super) fn interpolation_vars(path: &Path) -> Result<InterpolationVars> {
    let mut vars = load_dotenv_vars(path.parent().unwrap_or_else(|| Path::new(".")))?;
    for (key, value) in env::vars() {
        vars.insert(key, value);
    }
    Ok(vars)
}

/// Returns variables that consumed `${VAR:-default}` or `${VAR-default}`
/// defaults because `VAR` was missing from `vars`.
///
/// # Errors
///
/// Returns an error when interpolation syntax is malformed.
pub fn missing_defaulted_variables(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    let raw =
        fs::read_to_string(path).context(format!("failed to read spec at {}", path.display()))?;
    let value: Value = serde_norway::from_str(&raw)
        .context(format!("failed to parse YAML at {}", path.display()))?;
    let mut missing = BTreeSet::new();
    collect_missing_defaulted_variables_from_value(&value, vars, &mut missing)?;
    Ok(missing)
}

fn collect_missing_defaulted_variables_from_value(
    value: &Value,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    match value {
        Value::String(current) => collect_missing_defaulted_variables_in_string(current, vars, out),
        Value::Sequence(items) => {
            for item in items {
                collect_missing_defaulted_variables_from_value(item, vars, out)?;
            }
            Ok(())
        }
        Value::Mapping(entries) => {
            for value in entries.values() {
                collect_missing_defaulted_variables_from_value(value, vars, out)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_missing_defaulted_variables_in_string(
    input: &str,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != '$' {
            index += 1;
            continue;
        }
        if matches!(chars.get(index + 1), Some('$')) {
            index += 2;
            continue;
        }
        if matches!(chars.get(index + 1), Some('{')) {
            let start = index;
            index += 2;
            let (expr, next_index) = read_braced_expression(&chars, index, input, start)?;
            index = next_index;
            collect_missing_from_braced_expr(&expr, vars, out, input, start)?;
            continue;
        }
        index += 1;
    }
    Ok(())
}

fn collect_missing_from_braced_expr(
    expr: &str,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
    input: &str,
    start: usize,
) -> Result<()> {
    let mut chars = expr.chars();
    let Some(first) = chars.next() else {
        bail!("invalid variable expression in '{}'", &input[start..]);
    };
    if !is_var_start(first) {
        bail!("invalid variable expression in '{}'", &input[start..]);
    }
    let name_len = 1 + chars.take_while(|ch| is_var_char(*ch)).count();
    let name = &expr[..name_len];
    let suffix = &expr[name_len..];

    match suffix {
        "" => {}
        _ if suffix.starts_with(":-") => {
            let default_used = match vars.get(name) {
                Some(value) => value.is_empty(),
                None => true,
            };
            if !vars.contains_key(name) {
                out.insert(name.to_string());
            }
            if default_used {
                collect_missing_defaulted_variables_in_string(&suffix[2..], vars, out)?;
            }
        }
        _ if suffix.starts_with('-') => {
            if !vars.contains_key(name) {
                out.insert(name.to_string());
                collect_missing_defaulted_variables_in_string(&suffix[1..], vars, out)?;
            }
        }
        _ => bail!("invalid variable expression '${{{expr}}}' in '{input}'"),
    }
    Ok(())
}

fn load_dotenv_vars(project_dir: &Path) -> Result<InterpolationVars> {
    let dotenv_path = project_dir.join(".env");
    if !dotenv_path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&dotenv_path)
        .context(format!("failed to read {}", dotenv_path.display()))?;
    let mut vars = BTreeMap::new();
    for (line_no, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = trimmed.split_once('=') else {
            bail!(
                "failed to parse {}: line {} must use KEY=VALUE syntax",
                dotenv_path.display(),
                line_no + 1
            );
        };
        let key = key.trim();
        if key.is_empty() {
            bail!(
                "failed to parse {}: line {} has an empty variable name",
                dotenv_path.display(),
                line_no + 1
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

pub(super) fn interpolate_optional_string(
    value: &mut Option<String>,
    vars: &InterpolationVars,
) -> Result<()> {
    if let Some(current) = value {
        *current = interpolate_string(current, vars)?;
    }
    Ok(())
}

pub(super) fn interpolate_vec_strings(
    values: &mut [String],
    vars: &InterpolationVars,
) -> Result<()> {
    for value in values {
        *value = interpolate_string(value, vars)?;
    }
    Ok(())
}

pub(super) fn interpolate_string(input: &str, vars: &InterpolationVars) -> Result<String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] != '$' {
            out.push(chars[index]);
            index += 1;
            continue;
        }

        if matches!(chars.get(index + 1), Some('$')) {
            out.push('$');
            index += 2;
            continue;
        }

        if matches!(chars.get(index + 1), Some('{')) {
            let start = index;
            index += 2;
            let (expr, next_index) = read_braced_expression(&chars, index, input, start)?;
            index = next_index;
            out.push_str(&resolve_braced_variable(&expr, vars, input, start)?);
            continue;
        }

        index += 1;
        if !matches!(chars.get(index), Some(ch) if is_var_start(*ch)) {
            out.push('$');
            continue;
        }

        let mut name = String::new();
        while let Some(ch) = chars.get(index) {
            if is_var_char(*ch) {
                name.push(*ch);
                index += 1;
            } else {
                break;
            }
        }

        let Some(value) = vars.get(&name) else {
            bail!("missing variable '{name}' referenced in '{input}'");
        };
        out.push_str(value);
    }

    Ok(out)
}

fn read_braced_expression(
    chars: &[char],
    mut index: usize,
    input: &str,
    start: usize,
) -> Result<(String, usize)> {
    let mut expr = String::new();
    let mut nested_braces = 0usize;

    while let Some(ch) = chars.get(index) {
        if *ch == '$' {
            if matches!(chars.get(index + 1), Some('$')) {
                expr.push('$');
                expr.push('$');
                index += 2;
                continue;
            }
            if matches!(chars.get(index + 1), Some('{')) {
                nested_braces += 1;
                expr.push('$');
                expr.push('{');
                index += 2;
                continue;
            }
        }

        if *ch == '}' {
            if nested_braces == 0 {
                return Ok((expr, index + 1));
            }
            nested_braces -= 1;
        }

        expr.push(*ch);
        index += 1;
    }

    bail!("unterminated variable expression in '{}'", &input[start..]);
}

fn resolve_braced_variable(
    expr: &str,
    vars: &InterpolationVars,
    input: &str,
    start: usize,
) -> Result<String> {
    let mut chars = expr.chars();
    let Some(first) = chars.next() else {
        bail!("invalid variable expression in '{}'", &input[start..]);
    };
    if !is_var_start(first) {
        bail!("invalid variable expression in '{}'", &input[start..]);
    }
    let name_len = 1 + chars.take_while(|ch| is_var_char(*ch)).count();
    let name = &expr[..name_len];
    let suffix = &expr[name_len..];

    match suffix {
        "" => resolve_required_variable(name, vars),
        _ if suffix.starts_with(":-") => {
            let default = &suffix[2..];
            match vars.get(name) {
                Some(value) if !value.is_empty() => Ok(value.clone()),
                _ => interpolate_string(default, vars),
            }
        }
        _ if suffix.starts_with('-') => match vars.get(name) {
            Some(value) => Ok(value.clone()),
            None => interpolate_string(&suffix[1..], vars),
        },
        _ => bail!("invalid variable expression '${{{expr}}}' in '{input}'"),
    }
}

fn resolve_required_variable(name: &str, vars: &InterpolationVars) -> Result<String> {
    vars.get(name)
        .cloned()
        .context(format!("missing variable '{name}'"))
}

fn is_var_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_var_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_loader_handles_quotes_exports_missing_and_parse_errors() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        assert!(
            load_dotenv_vars(tmpdir.path())
                .expect("missing dotenv")
                .is_empty()
        );

        fs::write(
            tmpdir.path().join(".env"),
            "\n# comment\nexport DOUBLE=\"two words\"\nSINGLE='one word'\nPLAIN=value\nEMPTY=\n",
        )
        .expect("dotenv");
        let vars = load_dotenv_vars(tmpdir.path()).expect("load dotenv");
        assert_eq!(vars.get("DOUBLE").map(String::as_str), Some("two words"));
        assert_eq!(vars.get("SINGLE").map(String::as_str), Some("one word"));
        assert_eq!(vars.get("PLAIN").map(String::as_str), Some("value"));
        assert_eq!(vars.get("EMPTY").map(String::as_str), Some(""));

        fs::write(tmpdir.path().join(".env"), "BROKEN\n").expect("broken dotenv");
        assert!(
            load_dotenv_vars(tmpdir.path())
                .expect_err("missing equals")
                .to_string()
                .contains("must use KEY=VALUE syntax")
        );

        fs::write(tmpdir.path().join(".env"), "=nope\n").expect("empty key dotenv");
        assert!(
            load_dotenv_vars(tmpdir.path())
                .expect_err("empty key")
                .to_string()
                .contains("empty variable name")
        );
    }

    #[test]
    fn interpolate_string_covers_required_defaults_escapes_and_errors() {
        let vars = BTreeMap::from([
            ("FOO".to_string(), "value".to_string()),
            ("EMPTY".to_string(), String::new()),
            ("INNER".to_string(), "inner".to_string()),
        ]);

        assert_eq!(
            interpolate_string("pre-$FOO-${FOO}-$$", &vars).expect("basic interpolation"),
            "pre-value-value-$"
        );
        assert_eq!(
            interpolate_string("${EMPTY:-fallback}", &vars).expect("colon default"),
            "fallback"
        );
        assert_eq!(
            interpolate_string("${EMPTY-fallback}", &vars).expect("dash default"),
            ""
        );
        assert_eq!(
            interpolate_string("${MISSING:-${INNER:-fallback}}", &vars).expect("nested default"),
            "inner"
        );
        assert_eq!(
            interpolate_string("literal $9 and $$FOO", &vars).expect("literal dollars"),
            "literal $9 and $FOO"
        );

        for input in [
            "$MISSING",
            "${MISSING}",
            "${}",
            "${1BAD}",
            "${FOO?bad}",
            "${FOO",
        ] {
            assert!(
                interpolate_string(input, &vars).is_err(),
                "{input} should be rejected"
            );
        }
    }

    #[test]
    fn missing_defaulted_variables_walks_values_and_nested_defaults() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("compose.yaml");
        fs::write(
            &path,
            r#"
"${KEY_IGNORED:-not-a-value}": mapping key is ignored
services:
  app:
    image: "${IMAGE:-redis:7}"
    command:
      - sh
      - -lc
      - "echo ${OUTER:-${INNER:-fallback}}"
    environment:
      PRESENT: "${PRESENT:-unused}"
      EMPTY: "${EMPTY:-empty-default}"
      BOOL: true
      NUMBER: 7
"#,
        )
        .expect("compose");

        let vars = BTreeMap::from([
            ("PRESENT".to_string(), "set".to_string()),
            ("EMPTY".to_string(), String::new()),
        ]);
        let missing = missing_defaulted_variables(&path, &vars).expect("scan");
        assert_eq!(
            missing,
            BTreeSet::from([
                "IMAGE".to_string(),
                "OUTER".to_string(),
                "INNER".to_string(),
            ])
        );
    }

    #[test]
    fn missing_defaulted_variables_reports_malformed_yaml_values() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("compose.yaml");
        fs::write(
            &path,
            "services:\n  app:\n    image: \"${BROKEN:-fallback\"\n",
        )
        .expect("compose");

        assert!(
            missing_defaulted_variables(&path, &BTreeMap::new())
                .expect_err("unterminated expression")
                .to_string()
                .contains("unterminated variable expression")
        );
    }
}
