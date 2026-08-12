//! POSIX shell single-quoting policies with byte-stable output.
//!
//! [`quote`] is the canonical general-purpose policy and represents embedded
//! apostrophes with a double-quoted segment between single-quoted runs. Some
//! established staging, runtime, and presentation paths instead use a
//! backslash-escaped apostrophe between single-quoted runs;
//! [`quote_always_with_backslash_apostrophe`] preserves those exact bytes.
//! Callers that quote conditionally retain their existing allowlists and
//! delegate only the always-quoted branch.

/// Wraps `value` in single quotes, escaping embedded apostrophes with a
/// double-quoted apostrophe segment between single-quoted runs, so the result
/// is one shell word that expands to exactly `value` with no further
/// interpretation.
#[must_use]
pub fn quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

/// Always wraps `value` in single quotes, escaping embedded apostrophes with
/// a backslash-escaped apostrophe between single-quoted runs.
///
/// This preserves exact output bytes for paths that historically used that
/// representation. New executable render paths should continue using
/// [`quote`] unless their output contract requires these bytes.
#[must_use]
pub(crate) fn quote_always_with_backslash_apostrophe(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Quotes a token only when needed for display in a suggested shell command.
///
/// This deliberately preserves an empty string as empty for existing hint
/// rendering, so it must not be used to construct commands that will actually
/// be executed. Tokens containing only ASCII alphanumerics or `/._-:` remain
/// unquoted; every other token is single-quoted with embedded apostrophes
/// escaped using the existing backslash-apostrophe display sequence.
#[must_use]
pub(crate) fn quote_if_needed_for_display(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        quote_always_with_backslash_apostrophe(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{quote, quote_always_with_backslash_apostrophe, quote_if_needed_for_display};
    use proptest::prelude::*;

    /// Interprets the POSIX quoting that [`quote`] produces, concatenating the
    /// literal contents of single- and double-quoted runs. This lets the
    /// property test assert a round-trip without spawning a shell.
    fn unquote(quoted: &str) -> String {
        let mut out = String::new();
        let mut chars = quoted.chars();
        while let Some(c) = chars.next() {
            match c {
                '\'' => {
                    for d in chars.by_ref() {
                        if d == '\'' {
                            break;
                        }
                        out.push(d);
                    }
                }
                '"' => {
                    for d in chars.by_ref() {
                        if d == '"' {
                            break;
                        }
                        out.push(d);
                    }
                }
                other => out.push(other),
            }
        }
        out
    }

    #[test]
    fn quotes_empty_and_simple_values() {
        assert_eq!(quote(""), "''");
        assert_eq!(quote("abc"), "'abc'");
        assert_eq!(quote("a b"), "'a b'");
    }

    #[test]
    fn escapes_embedded_single_quote() {
        assert_eq!(quote("a'b"), "'a'\"'\"'b'");
        assert_eq!(unquote(&quote("a'b")), "a'b");
    }

    #[test]
    fn backslash_apostrophe_quoting_preserves_exact_always_quoted_bytes() {
        let cases = [
            ("", "''"),
            ("safe-token", "'safe-token'"),
            ("two words", "'two words'"),
            ("demo'spec", "'demo'\\''spec'"),
        ];

        for (value, expected) in cases {
            assert_eq!(
                quote_always_with_backslash_apostrophe(value),
                expected,
                "value {value:?}"
            );
        }
    }

    #[test]
    fn display_quoting_preserves_the_existing_conditional_matrix() {
        let cases = [
            ("safe-token_1/path.yaml", "safe-token_1/path.yaml"),
            ("job:17", "job:17"),
            ("two words", "'two words'"),
            ("demo'spec", "'demo'\\''spec'"),
            ("", ""),
        ];

        for (value, expected) in cases {
            assert_eq!(
                quote_if_needed_for_display(value),
                expected,
                "value {value:?}"
            );
        }
    }

    proptest! {
        /// For any input, the quoted form must reverse back to the original,
        /// which would have caught every historical allowlist divergence.
        #[test]
        fn quote_round_trips(s in ".*") {
            prop_assert_eq!(unquote(&quote(&s)), s);
        }
    }
}
