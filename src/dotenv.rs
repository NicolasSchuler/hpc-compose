//! Pure parsing for the dotenv line grammar shared by configuration loaders.

use std::collections::BTreeMap;

/// The reason a dotenv-style line failed the `KEY=VALUE` grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DotenvLineErrorKind {
    /// The line had no `=` separator.
    MissingEquals,
    /// The key to the left of `=` was empty.
    EmptyKey,
}

impl DotenvLineErrorKind {
    /// Returns the established user-facing reason for this grammar error.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::MissingEquals => "must use KEY=VALUE syntax",
            Self::EmptyKey => "has an empty variable name",
        }
    }
}

/// A path-free failure for one dotenv-style line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DotenvLineError {
    /// 1-based line number of the offending line.
    pub(crate) line: usize,
    /// Why the line was rejected.
    pub(crate) kind: DotenvLineErrorKind,
}

/// Parses dotenv-style `KEY=VALUE` lines from an already-read buffer.
///
/// Blank lines and comments are ignored, `export ` is an optional prefix,
/// surrounding key/value whitespace is trimmed, and matching single or double
/// quotes are removed. Duplicate keys use the last value.
pub(crate) fn parse_dotenv_lines(raw: &str) -> Result<BTreeMap<String, String>, DotenvLineError> {
    let mut vars = BTreeMap::new();
    for (line_no, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(DotenvLineError {
                line: line_no + 1,
                kind: DotenvLineErrorKind::MissingEquals,
            });
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(DotenvLineError {
                line: line_no + 1,
                kind: DotenvLineErrorKind::EmptyKey,
            });
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
