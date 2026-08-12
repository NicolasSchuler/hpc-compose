use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_norway::Value;

use crate::domain::{ascii_identifier_continue, ascii_identifier_start};
use crate::dotenv::{DotenvLineError, parse_dotenv_lines};
use crate::spec_error::SpecError;

#[cfg(test)]
use crate::dotenv::DotenvLineErrorKind;

pub(super) type InterpolationVars = BTreeMap<String, String>;

#[derive(Default)]
struct DefaultUsageTracker {
    missing: BTreeSet<String>,
}

thread_local! {
    /// `strict-env` must follow the real typed interpolation traversal, but it
    /// is a diagnostic pass rather than a second load: missing required
    /// variables are handled by the normal loader and must not prevent this
    /// pass from reporting default fallbacks. The thread-local keeps that
    /// diagnostic mode scoped to one interpolation traversal without widening
    /// every interpolation helper's public contract.
    static DEFAULT_USAGE_TRACKER: RefCell<Option<DefaultUsageTracker>> = const { RefCell::new(None) };
}

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
    let mut spec = super::parse::load_raw_spec(path)?;
    track_missing_default_usage(|| spec.interpolate_with_vars(vars))
}

/// Returns variables that consumed `${VAR:-default}` or `${VAR-default}`
/// defaults in an already-read compose document because `VAR` was missing from
/// `vars`.
///
/// # Errors
///
/// Returns an error when the YAML or interpolation syntax is malformed.
pub fn missing_defaulted_variables_from_str(
    raw: &str,
    vars: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    missing_defaulted_variables_from_str_at_path(Path::new("compose.yaml"), raw, vars)
}

/// Path-aware in-memory variant used by authoring diagnostics so `extends`
/// files resolve exactly as they do during the real load.
pub(crate) fn missing_defaulted_variables_from_str_at_path(
    path: &Path,
    raw: &str,
    vars: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    let mut spec = super::parse::load_raw_spec_from_str(path, raw)?;
    track_missing_default_usage(|| spec.interpolate_with_vars(vars))
}

/// Returns interpolation variable names referenced by YAML scalar values in a
/// compose spec.
///
/// # Errors
///
/// Returns an error when the spec cannot be read, parsed, or contains malformed
/// interpolation syntax.
pub fn referenced_variables(
    path: &Path,
    vars: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    let raw =
        fs::read_to_string(path).context(format!("failed to read spec at {}", path.display()))?;
    let value: Value = serde_norway::from_str(&raw)
        .context(format!("failed to parse YAML at {}", path.display()))?;
    let mut referenced = BTreeSet::new();
    collect_referenced_variables_from_value(&value, vars, &mut referenced)?;
    Ok(referenced)
}

fn collect_referenced_variables_from_value(
    value: &Value,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    match value {
        Value::String(current) => collect_referenced_variables_in_string(current, vars, out),
        Value::Sequence(items) => {
            for item in items {
                collect_referenced_variables_from_value(item, vars, out)?;
            }
            Ok(())
        }
        Value::Mapping(entries) => {
            for value in entries.values() {
                collect_referenced_variables_from_value(value, vars, out)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_referenced_variables_in_string(
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
            collect_referenced_from_braced_expr(&expr, vars, out, input, start)?;
            continue;
        }

        index += 1;
        if !matches!(chars.get(index), Some(ch) if ascii_identifier_start(*ch)) {
            continue;
        }
        let mut name = String::new();
        while let Some(ch) = chars.get(index) {
            if ascii_identifier_continue(*ch) {
                name.push(*ch);
                index += 1;
            } else {
                break;
            }
        }
        out.insert(name);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum BracedExpressionOperator {
    Direct,
    RequiredNonEmpty,
    DefaultIfUnsetOrEmpty,
    RequiredIfUnset,
    DefaultIfUnset,
}

struct ParsedBracedExpression<'a> {
    name: &'a str,
    operator: BracedExpressionOperator,
    operand: &'a str,
}

enum BracedExpressionParseError {
    InvalidName,
    UnsupportedOperator,
}

fn parse_braced_expression(
    expr: &str,
) -> Result<ParsedBracedExpression<'_>, BracedExpressionParseError> {
    let mut chars = expr.char_indices();
    let Some((_, first)) = chars.next() else {
        return Err(BracedExpressionParseError::InvalidName);
    };
    if !ascii_identifier_start(first) {
        return Err(BracedExpressionParseError::InvalidName);
    }
    let name_end = chars
        .find_map(|(index, ch)| (!ascii_identifier_continue(ch)).then_some(index))
        .unwrap_or(expr.len());
    let name = &expr[..name_end];
    let suffix = &expr[name_end..];

    let (operator, operand) = if suffix.is_empty() {
        (BracedExpressionOperator::Direct, "")
    } else if let Some(operand) = suffix.strip_prefix(":?") {
        (BracedExpressionOperator::RequiredNonEmpty, operand)
    } else if let Some(operand) = suffix.strip_prefix(":-") {
        (BracedExpressionOperator::DefaultIfUnsetOrEmpty, operand)
    } else if let Some(operand) = suffix.strip_prefix('?') {
        (BracedExpressionOperator::RequiredIfUnset, operand)
    } else if let Some(operand) = suffix.strip_prefix('-') {
        (BracedExpressionOperator::DefaultIfUnset, operand)
    } else {
        return Err(BracedExpressionParseError::UnsupportedOperator);
    };

    Ok(ParsedBracedExpression {
        name,
        operator,
        operand,
    })
}

fn collect_referenced_from_braced_expr(
    expr: &str,
    vars: &BTreeMap<String, String>,
    out: &mut BTreeSet<String>,
    input: &str,
    start: usize,
) -> Result<()> {
    let parsed = parse_braced_expression(expr).map_err(|error| match error {
        BracedExpressionParseError::InvalidName => {
            anyhow::anyhow!("invalid variable expression in '{}'", &input[start..])
        }
        BracedExpressionParseError::UnsupportedOperator => {
            anyhow::anyhow!("invalid variable expression '${{{expr}}}' in '{input}'")
        }
    })?;
    out.insert(parsed.name.to_string());

    match parsed.operator {
        BracedExpressionOperator::Direct => {}
        BracedExpressionOperator::RequiredNonEmpty => {
            let required_but_missing = match vars.get(parsed.name) {
                Some(value) => value.is_empty(),
                None => true,
            };
            if required_but_missing {
                collect_referenced_variables_in_string(parsed.operand, vars, out)?;
            }
        }
        BracedExpressionOperator::DefaultIfUnsetOrEmpty => {
            let default_used = match vars.get(parsed.name) {
                Some(value) => value.is_empty(),
                None => true,
            };
            if default_used {
                collect_referenced_variables_in_string(parsed.operand, vars, out)?;
            }
        }
        BracedExpressionOperator::RequiredIfUnset => {
            if !vars.contains_key(parsed.name) {
                collect_referenced_variables_in_string(parsed.operand, vars, out)?;
            }
        }
        BracedExpressionOperator::DefaultIfUnset => {
            if !vars.contains_key(parsed.name) {
                collect_referenced_variables_in_string(parsed.operand, vars, out)?;
            }
        }
    }
    Ok(())
}

fn track_missing_default_usage(
    interpolate: impl FnOnce() -> Result<()>,
) -> Result<BTreeSet<String>> {
    DEFAULT_USAGE_TRACKER.with(|tracker| {
        let previous = tracker.replace(Some(DefaultUsageTracker::default()));
        debug_assert!(previous.is_none(), "default-usage tracking must not nest");

        let result = interpolate();
        let tracked = tracker
            .replace(previous)
            .expect("default-usage tracker was installed");
        result.map(|()| tracked.missing)
    })
}

fn record_missing_default(name: &str) {
    DEFAULT_USAGE_TRACKER.with(|tracker| {
        if let Some(active) = tracker.borrow_mut().as_mut() {
            active.missing.insert(name.to_string());
        }
    });
}

fn default_usage_tracking_active() -> bool {
    DEFAULT_USAGE_TRACKER.with(|tracker| tracker.borrow().is_some())
}

/// Failure modes of [`parse_env_file`]: either the file could not be read, or a
/// line violated the `KEY=VALUE` grammar. Keeping the two distinct lets the
/// caller map an I/O failure and a malformed line to different diagnostics.
pub(super) enum ParseEnvFileError {
    /// The file could not be read (missing, permissions, etc.).
    Io(std::io::Error),
    /// A line violated the `KEY=VALUE` grammar.
    Line(DotenvLineError),
}

/// Reads and parses a dotenv-style file at an explicit path. Existence is *not*
/// checked here -- the caller decides how to surface a missing file (e.g. the
/// `env_file:` loader raises [`SpecError::EnvFileNotFound`] first).
pub(super) fn parse_env_file(path: &Path) -> Result<InterpolationVars, ParseEnvFileError> {
    let raw = fs::read_to_string(path).map_err(ParseEnvFileError::Io)?;
    parse_dotenv_lines(&raw).map_err(ParseEnvFileError::Line)
}

fn load_dotenv_vars(project_dir: &Path) -> Result<InterpolationVars> {
    let dotenv_path = project_dir.join(".env");
    if !dotenv_path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&dotenv_path)
        .context(format!("failed to read {}", dotenv_path.display()))?;
    parse_dotenv_lines(&raw).map_err(|error| {
        anyhow::anyhow!(
            "failed to parse {}: line {} {}",
            dotenv_path.display(),
            error.line,
            error.kind.reason()
        )
    })
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
    // Interpolation only ever acts on `$` (`${VAR}`, `${VAR:-d}`, `$$`); the
    // overwhelmingly common `$`-free string can skip the char-vector build and
    // the char-by-char walk entirely.
    if !input.contains('$') {
        return Ok(input.to_string());
    }
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
        if !matches!(chars.get(index), Some(ch) if ascii_identifier_start(*ch)) {
            out.push('$');
            continue;
        }

        let mut name = String::new();
        while let Some(ch) = chars.get(index) {
            if ascii_identifier_continue(*ch) {
                name.push(*ch);
                index += 1;
            } else {
                break;
            }
        }

        match vars.get(&name) {
            Some(value) => out.push_str(value),
            None if default_usage_tracking_active() => {}
            None => bail!("missing variable '{name}' referenced in '{input}'"),
        }
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
    let parsed = parse_braced_expression(expr).map_err(|error| match error {
        BracedExpressionParseError::InvalidName => {
            anyhow::anyhow!("invalid variable expression in '{}'", &input[start..])
        }
        BracedExpressionParseError::UnsupportedOperator => {
            anyhow::anyhow!("invalid variable expression '${{{expr}}}' in '{input}'")
        }
    })?;

    match parsed.operator {
        BracedExpressionOperator::Direct => resolve_required_variable(parsed.name, vars),
        BracedExpressionOperator::RequiredNonEmpty => {
            resolve_required_variable_with_message(parsed.name, parsed.operand, vars, true)
        }
        BracedExpressionOperator::DefaultIfUnsetOrEmpty => match vars.get(parsed.name) {
            Some(value) if !value.is_empty() => Ok(value.clone()),
            Some(_) => interpolate_string(parsed.operand, vars),
            None => {
                record_missing_default(parsed.name);
                interpolate_string(parsed.operand, vars)
            }
        },
        BracedExpressionOperator::RequiredIfUnset => {
            resolve_required_variable_with_message(parsed.name, parsed.operand, vars, false)
        }
        BracedExpressionOperator::DefaultIfUnset => match vars.get(parsed.name) {
            Some(value) => Ok(value.clone()),
            None => {
                record_missing_default(parsed.name);
                interpolate_string(parsed.operand, vars)
            }
        },
    }
}

fn resolve_required_variable(name: &str, vars: &InterpolationVars) -> Result<String> {
    match vars.get(name) {
        Some(value) => Ok(value.clone()),
        None if default_usage_tracking_active() => Ok(String::new()),
        None => Err(anyhow::anyhow!("missing variable '{name}'")),
    }
}

/// Resolves a `${VAR:?message}` (`require_non_empty`) or `${VAR?message}`
/// (`!require_non_empty`) required-variable expression.
///
/// Returns the value when the variable satisfies the requirement, otherwise a
/// [`SpecError::RequiredVariableUnset`] miette diagnostic whose message echoes
/// the (interpolated) user message. The error is boxed through `anyhow` so the
/// diagnostic metadata survives the downcast in `cli_error_report`.
fn resolve_required_variable_with_message(
    name: &str,
    raw_message: &str,
    vars: &InterpolationVars,
    require_non_empty: bool,
) -> Result<String> {
    let value = vars.get(name);
    if let Some(value) = value.filter(|value| !(require_non_empty && value.is_empty())) {
        return Ok(value.clone());
    }

    if default_usage_tracking_active() {
        if !raw_message.is_empty() {
            let _ = interpolate_string(raw_message, vars)?;
        }
        return Ok(String::new());
    }

    let message = if raw_message.is_empty() {
        if value.is_some() {
            format!("'{name}' is required but empty")
        } else {
            format!("'{name}' is required but not set")
        }
    } else {
        let user_message = interpolate_string(raw_message, vars)?;
        format!("'{name}' is required: {user_message}")
    };

    let help_text = format!(
        "Set `{name}` before running this command, e.g. `export {name}=...`, add it to the `.env` file next to the compose file, or pass it however this command's caller supplies interpolation variables."
    );

    Err(SpecError::RequiredVariableUnset {
        name: name.to_string(),
        message,
        help_text,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Expected<'a, T> {
        Ok(T),
        Err(&'a str),
    }

    #[test]
    fn braced_expression_table_preserves_resolution_and_reference_scanning() {
        struct Case {
            label: &'static str,
            input: &'static str,
            vars: &'static [(&'static str, &'static str)],
            interpolation: Expected<'static, &'static str>,
            references: Expected<'static, &'static [&'static str]>,
        }

        const SET_EMPTY_NESTED: &[(&str, &str)] =
            &[("SET", "value"), ("EMPTY", ""), ("NESTED", "nested")];
        const SET_EMPTY: &[(&str, &str)] = &[("SET", "value"), ("EMPTY", "")];
        const NO_VARS: &[(&str, &str)] = &[];
        const SET_REF: &[&str] = &["SET"];
        const EMPTY_REF: &[&str] = &["EMPTY"];
        const UNSET_REF: &[&str] = &["UNSET"];
        const EMPTY_NESTED_REFS: &[&str] = &["EMPTY", "NESTED"];
        const UNSET_NESTED_REFS: &[&str] = &["UNSET", "NESTED"];

        let cases = [
            Case {
                label: "direct set",
                input: "${SET}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("value"),
                references: Expected::Ok(SET_REF),
            },
            Case {
                label: "direct empty",
                input: "${EMPTY}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok(""),
                references: Expected::Ok(EMPTY_REF),
            },
            Case {
                label: "direct unset",
                input: "${UNSET}",
                vars: NO_VARS,
                interpolation: Expected::Err("missing variable 'UNSET'"),
                references: Expected::Ok(UNSET_REF),
            },
            Case {
                label: "colon question set skips malformed message",
                input: "${SET:?${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("value"),
                references: Expected::Ok(SET_REF),
            },
            Case {
                label: "colon question empty traverses nested message",
                input: "${EMPTY:?empty ${NESTED:-fallback}}",
                vars: SET_EMPTY_NESTED,
                interpolation: Expected::Err("'EMPTY' is required: empty nested"),
                references: Expected::Ok(EMPTY_NESTED_REFS),
            },
            Case {
                label: "colon question unset traverses nested message default",
                input: "${UNSET:?unset ${NESTED:-fallback}}",
                vars: SET_EMPTY,
                interpolation: Expected::Err("'UNSET' is required: unset fallback"),
                references: Expected::Ok(UNSET_NESTED_REFS),
            },
            Case {
                label: "colon dash set skips malformed default",
                input: "${SET:-${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("value"),
                references: Expected::Ok(SET_REF),
            },
            Case {
                label: "colon dash empty traverses nested default",
                input: "${EMPTY:-empty ${NESTED:-fallback}}",
                vars: SET_EMPTY_NESTED,
                interpolation: Expected::Ok("empty nested"),
                references: Expected::Ok(EMPTY_NESTED_REFS),
            },
            Case {
                label: "colon dash unset traverses nested default",
                input: "${UNSET:-unset ${NESTED:-fallback}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("unset fallback"),
                references: Expected::Ok(UNSET_NESTED_REFS),
            },
            Case {
                label: "question set skips malformed message",
                input: "${SET?${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("value"),
                references: Expected::Ok(SET_REF),
            },
            Case {
                label: "question empty skips malformed message",
                input: "${EMPTY?${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok(""),
                references: Expected::Ok(EMPTY_REF),
            },
            Case {
                label: "question unset traverses nested message default",
                input: "${UNSET?unset ${NESTED:-fallback}}",
                vars: SET_EMPTY,
                interpolation: Expected::Err("'UNSET' is required: unset fallback"),
                references: Expected::Ok(UNSET_NESTED_REFS),
            },
            Case {
                label: "dash set skips malformed default",
                input: "${SET-${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("value"),
                references: Expected::Ok(SET_REF),
            },
            Case {
                label: "dash empty skips malformed default",
                input: "${EMPTY-${}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok(""),
                references: Expected::Ok(EMPTY_REF),
            },
            Case {
                label: "dash unset traverses nested default",
                input: "${UNSET-unset ${NESTED:-fallback}}",
                vars: SET_EMPTY,
                interpolation: Expected::Ok("unset fallback"),
                references: Expected::Ok(UNSET_NESTED_REFS),
            },
            Case {
                label: "empty expression",
                input: "${}",
                vars: NO_VARS,
                interpolation: Expected::Err("invalid variable expression in '${}'"),
                references: Expected::Err("invalid variable expression in '${}'"),
            },
            Case {
                label: "invalid name",
                input: "${1BAD}",
                vars: NO_VARS,
                interpolation: Expected::Err("invalid variable expression in '${1BAD}'"),
                references: Expected::Err("invalid variable expression in '${1BAD}'"),
            },
            Case {
                label: "unsupported operator",
                input: "${SET:+alternate}",
                vars: SET_EMPTY,
                interpolation: Expected::Err(
                    "invalid variable expression '${SET:+alternate}' in '${SET:+alternate}'",
                ),
                references: Expected::Err(
                    "invalid variable expression '${SET:+alternate}' in '${SET:+alternate}'",
                ),
            },
            Case {
                label: "unterminated expression",
                input: "${SET",
                vars: SET_EMPTY,
                interpolation: Expected::Err("unterminated variable expression in '${SET'"),
                references: Expected::Err("unterminated variable expression in '${SET'"),
            },
            Case {
                label: "unterminated nested expression",
                input: "${SET:-${NESTED}",
                vars: SET_EMPTY_NESTED,
                interpolation: Expected::Err(
                    "unterminated variable expression in '${SET:-${NESTED}'",
                ),
                references: Expected::Err("unterminated variable expression in '${SET:-${NESTED}'"),
            },
            Case {
                label: "taken malformed nested default",
                input: "${UNSET:-${}}",
                vars: NO_VARS,
                interpolation: Expected::Err("invalid variable expression in '${}'"),
                references: Expected::Err("invalid variable expression in '${}'"),
            },
        ];

        for case in cases {
            let vars = case
                .vars
                .iter()
                .map(|&(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>();

            match case.interpolation {
                Expected::Ok(expected) => assert_eq!(
                    interpolate_string(case.input, &vars)
                        .unwrap_or_else(|error| panic!("{}: {error}", case.label)),
                    expected,
                    "{} interpolation",
                    case.label
                ),
                Expected::Err(expected) => assert_eq!(
                    interpolate_string(case.input, &vars)
                        .expect_err(case.label)
                        .to_string(),
                    expected,
                    "{} interpolation error",
                    case.label
                ),
            }

            let mut references = BTreeSet::new();
            let result = collect_referenced_variables_in_string(case.input, &vars, &mut references);
            match case.references {
                Expected::Ok(expected) => {
                    result.unwrap_or_else(|error| panic!("{}: {error}", case.label));
                    assert_eq!(
                        references,
                        expected
                            .iter()
                            .map(|name| (*name).to_string())
                            .collect::<BTreeSet<_>>(),
                        "{} references",
                        case.label
                    );
                }
                Expected::Err(expected) => assert_eq!(
                    result.expect_err(case.label).to_string(),
                    expected,
                    "{} reference error",
                    case.label
                ),
            }
        }
    }

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
    fn parse_env_file_reuses_dotenv_line_grammar() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("service.env");

        fs::write(&path, "# comment\nexport A=one\nB='two words'\n").expect("write env file");
        let vars = match parse_env_file(&path) {
            Ok(vars) => vars,
            Err(_) => panic!("expected a parseable env file"),
        };
        assert_eq!(vars.get("A").map(String::as_str), Some("one"));
        assert_eq!(vars.get("B").map(String::as_str), Some("two words"));

        fs::write(&path, "GOOD=1\nBROKEN\n").expect("write malformed env file");
        match parse_env_file(&path) {
            Err(ParseEnvFileError::Line(error)) => {
                assert_eq!(error.line, 2);
                assert_eq!(error.kind, DotenvLineErrorKind::MissingEquals);
            }
            other => panic!("expected a malformed-line error, got {:?}", other.is_ok()),
        }
    }

    #[test]
    fn dotenv_parser_preserves_edge_grammar_and_last_value_wins() {
        let vars = parse_dotenv_lines(concat!(
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
        ))
        .expect("dotenv grammar");

        assert_eq!(vars.get("EMPTY").map(String::as_str), Some(""));
        assert_eq!(vars.get("DUPLICATE").map(String::as_str), Some("second"));
        assert_eq!(vars.get("DOUBLE").map(String::as_str), Some("two words"));
        assert_eq!(vars.get("SINGLE").map(String::as_str), Some("one word"));
        assert_eq!(vars.get("EMPTY_DOUBLE").map(String::as_str), Some(""));
        assert_eq!(vars.get("EMPTY_SINGLE").map(String::as_str), Some(""));
        assert_eq!(
            vars.get("UNMATCHED_SINGLE").map(String::as_str),
            Some("'left")
        );
        assert_eq!(
            vars.get("UNMATCHED_DOUBLE").map(String::as_str),
            Some("\"right")
        );
        assert_eq!(
            vars.get("MISMATCHED_SINGLE").map(String::as_str),
            Some("'left\"")
        );
        assert_eq!(
            vars.get("MISMATCHED_DOUBLE").map(String::as_str),
            Some("\"right'")
        );
    }

    #[test]
    fn dotenv_loader_reports_exact_line_and_reason() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join(".env");

        fs::write(&path, "GOOD=1\n# comment\nBROKEN\n").expect("dotenv");
        assert_eq!(
            load_dotenv_vars(tmpdir.path())
                .expect_err("missing equals")
                .to_string(),
            format!(
                "failed to parse {}: line 3 must use KEY=VALUE syntax",
                path.display()
            )
        );

        fs::write(&path, "GOOD=1\n\n = value\n").expect("dotenv");
        assert_eq!(
            load_dotenv_vars(tmpdir.path())
                .expect_err("empty key")
                .to_string(),
            format!(
                "failed to parse {}: line 3 has an empty variable name",
                path.display()
            )
        );
    }

    #[test]
    fn parse_env_file_missing_file_is_a_plain_io_error() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("does-not-exist.env");
        match parse_env_file(&missing) {
            Err(ParseEnvFileError::Io(error)) => {
                assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected an I/O error, got {:?}", other.is_ok()),
        }
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
        assert_eq!(
            interpolate_string("${FOO?bad}", &vars).expect("required var satisfied"),
            "value"
        );

        for input in ["$MISSING", "${MISSING}", "${}", "${1BAD}", "${FOO"] {
            assert!(
                interpolate_string(input, &vars).is_err(),
                "{input} should be rejected"
            );
        }
    }

    #[test]
    fn ascii_identifier_policy_preserves_interpolation_scanning() {
        for &(name, expected) in crate::domain::ASCII_IDENTIFIER_NAME_CASES {
            let mut chars = name.chars();
            let actual = chars.next().is_some_and(|first| {
                ascii_identifier_start(first) && chars.all(ascii_identifier_continue)
            });
            assert_eq!(actual, expected, "name {name:?}");
        }

        let vars = BTreeMap::from([
            ("_".to_string(), "under".to_string()),
            ("A".to_string(), "a".to_string()),
            ("A0".to_string(), "a0".to_string()),
            ("A_B9".to_string(), "token".to_string()),
        ]);
        let input = "$_/$A.$A0:$A_B9 $9 $é $- $";
        assert_eq!(
            interpolate_string(input, &vars).expect("bare interpolation"),
            "under/a.a0:token $9 $é $- $"
        );

        let mut referenced = BTreeSet::new();
        collect_referenced_variables_in_string(input, &vars, &mut referenced)
            .expect("bare reference scan");
        assert_eq!(
            referenced,
            BTreeSet::from([
                "_".to_string(),
                "A".to_string(),
                "A0".to_string(),
                "A_B9".to_string(),
            ])
        );
    }

    #[test]
    fn interpolate_string_required_colon_question_errors_on_unset_or_empty() {
        let vars = BTreeMap::from([
            ("FOO".to_string(), "value".to_string()),
            ("EMPTY".to_string(), String::new()),
            ("INNER".to_string(), "inner".to_string()),
        ]);

        assert_eq!(
            interpolate_string("${FOO:?bad}", &vars).expect("set variable passes"),
            "value"
        );
        assert_eq!(
            interpolate_string("${EMPTY:?bad}", &vars)
                .expect_err("empty rejected")
                .to_string(),
            "'EMPTY' is required: bad"
        );
        assert!(interpolate_string("${MISSING:?bad}", &vars).is_err());
        assert_eq!(
            interpolate_string("${MISSING:?}", &vars)
                .expect_err("unset rejected")
                .to_string(),
            "'MISSING' is required but not set"
        );
        assert_eq!(
            interpolate_string("${EMPTY:?}", &vars)
                .expect_err("empty rejected")
                .to_string(),
            "'EMPTY' is required but empty"
        );
        assert_eq!(
            interpolate_string("${MISSING:?need ${INNER}}", &vars)
                .expect_err("message is interpolated")
                .to_string(),
            "'MISSING' is required: need inner"
        );
        assert_eq!(
            interpolate_string("${MISSING:?need ${ALSO_MISSING:-fallback-msg}}", &vars)
                .expect_err("message default is interpolated")
                .to_string(),
            "'MISSING' is required: need fallback-msg"
        );
    }

    #[test]
    fn interpolate_string_required_bare_question_allows_empty_but_not_unset() {
        let vars = BTreeMap::from([
            ("FOO".to_string(), "value".to_string()),
            ("EMPTY".to_string(), String::new()),
        ]);

        assert_eq!(
            interpolate_string("${FOO?bad}", &vars).expect("set variable passes"),
            "value"
        );
        assert_eq!(
            interpolate_string("${EMPTY?bad}", &vars).expect("empty passes for bare ?"),
            ""
        );
        assert!(interpolate_string("${MISSING?bad}", &vars).is_err());
        assert_eq!(
            interpolate_string("${MISSING?}", &vars)
                .expect_err("unset rejected")
                .to_string(),
            "'MISSING' is required but not set"
        );
    }

    #[test]
    fn interpolate_string_required_operators_do_not_collide_with_default_operators() {
        let vars = BTreeMap::from([("EMPTY".to_string(), String::new())]);

        assert_eq!(
            interpolate_string("${EMPTY:-?}", &vars).expect("colon-dash default keeps '?'"),
            "?"
        );
        assert_eq!(
            interpolate_string("${MISSING:?-}", &vars)
                .expect_err("colon-question rejected")
                .to_string(),
            "'MISSING' is required: -"
        );
        assert_eq!(
            interpolate_string("${MISSING?-}", &vars)
                .expect_err("bare question rejected")
                .to_string(),
            "'MISSING' is required: -"
        );
    }

    #[test]
    fn missing_defaulted_variables_walks_values_and_nested_defaults() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("compose.yaml");
        fs::write(
            &path,
            r#"
services:
  "${KEY_IGNORED:-not-a-value}":
    image: "${IMAGE:-redis:7}"
    command:
      - sh
      - -lc
      - "echo ${OUTER:-${INNER:-fallback}}"
    environment:
      PRESENT: "${PRESENT:-unused}"
      EMPTY: "${EMPTY:-empty-default}"
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
