//! The single canonical POSIX shell single-quoting routine.
//!
//! Every user-controlled value emitted into the generated bash sbatch script —
//! and any command string later handed to a shell — must pass through
//! [`quote`]. Historically several near-identical copies existed across the
//! render, output, and runtime modules; consolidating here gives one tested,
//! audited implementation guarding the injection surface.

/// Wraps `value` in single quotes, escaping embedded single quotes via the
/// canonical `'\''` sequence, so the result is a single shell word that
/// expands to exactly `value` with no further interpretation.
#[must_use]
pub fn quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::quote;
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

    proptest! {
        /// For any input, the quoted form must reverse back to the original,
        /// which would have caught every historical allowlist divergence.
        #[test]
        fn quote_round_trips(s in ".*") {
            prop_assert_eq!(unquote(&quote(&s)), s);
        }
    }
}
