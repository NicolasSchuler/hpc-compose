/// One gibibyte in bytes, shared by every memory-size parser and formatter.
pub(crate) const GIB: u64 = 1_024 * 1_024 * 1_024;

/// Formats a byte count with binary units, scaling through TiB at one decimal.
#[must_use]
pub(crate) fn format_binary_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Parses a memory-size string (`512M`, `1.5G`, `2GiB`, `1048576`, …) into a
/// byte count.
///
/// This is the single shared implementation used by the linter and the
/// `job` accounting/rightsize/scoring code so they all agree on units and edge
/// cases. It accepts an optional decimal magnitude, the `B`/`K`/`M`/`G`/`T`/`P`
/// suffixes (with `B`/`iB` variants), and a bare byte count. The Slurm `sacct`
/// literal `unknown` (any case) and the empty string map to `None`. All
/// arithmetic saturates, so the function is total and never panics or overflows.
#[must_use]
pub(crate) fn parse_memory_bytes(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    let number_end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_digit() && ch != '.').then_some(index))
        .unwrap_or(trimmed.len());
    let number = &trimmed[..number_end];
    if number.is_empty() {
        return None;
    }
    let magnitude = number.parse::<f64>().ok()?;
    if !magnitude.is_finite() || magnitude < 0.0 {
        return None;
    }
    let multiplier = match trimmed[number_end..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1_u64,
        "K" | "KB" | "KIB" => 1_024,
        "M" | "MB" | "MIB" => 1_024_u64.pow(2),
        "G" | "GB" | "GIB" => GIB,
        "T" | "TB" | "TIB" => 1_024_u64.pow(4),
        "P" | "PB" | "PIB" => 1_024_u64.pow(5),
        _ => return None,
    };
    // Multiply in f64 to honor decimals, then clamp into u64 saturatingly.
    let bytes = magnitude * multiplier as f64;
    if bytes >= u64::MAX as f64 {
        Some(u64::MAX)
    } else {
        Some(bytes as u64)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::string::string_regex;

    use super::*;

    fn prop_config() -> ProptestConfig {
        ProptestConfig {
            cases: 64,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    #[test]
    fn format_binary_bytes_preserves_shared_edge_matrix() {
        let cases = [
            (0, "0 B"),
            (1_023, "1023 B"),
            (1_024, "1.0 KiB"),
            (1_536, "1.5 KiB"),
            (1_024_u64.pow(2) - 1, "1024.0 KiB"),
            (1_024_u64.pow(2), "1.0 MiB"),
            (1_024_u64.pow(3), "1.0 GiB"),
            (1_024_u64.pow(4), "1.0 TiB"),
            (u64::MAX, "16777216.0 TiB"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(format_binary_bytes(bytes), expected, "bytes={bytes}");
        }
    }

    #[test]
    fn memory_bytes_parser_handles_units_decimals_and_sentinels() {
        assert_eq!(parse_memory_bytes("1048576"), Some(1_048_576));
        assert_eq!(parse_memory_bytes("512M"), Some(512 * 1_024 * 1_024));
        assert_eq!(parse_memory_bytes("2GiB"), Some(2 * GIB));
        assert_eq!(parse_memory_bytes("1.5G"), Some(1_610_612_736));
        // sacct sentinels and empty values map to None.
        assert_eq!(parse_memory_bytes("unknown"), None);
        assert_eq!(parse_memory_bytes("UNKNOWN"), None);
        assert_eq!(parse_memory_bytes("   "), None);
        // Unsupported units and missing magnitudes are rejected.
        assert_eq!(parse_memory_bytes("4Gc"), None);
        assert_eq!(parse_memory_bytes("G"), None);
        // Integer and decimal forms of the same size round-trip to the same bytes.
        assert_eq!(parse_memory_bytes("2G"), parse_memory_bytes("2.0G"));
        // Saturates instead of overflowing.
        assert_eq!(parse_memory_bytes("99999999999P"), Some(u64::MAX));
    }

    #[test]
    fn memory_bytes_parser_preserves_alias_trimming_and_truncation_contract() {
        let unit_cases = [
            ("B", 1),
            ("K", 1_024),
            ("KB", 1_024),
            ("KiB", 1_024),
            ("M", 1_024_u64.pow(2)),
            ("MB", 1_024_u64.pow(2)),
            ("MiB", 1_024_u64.pow(2)),
            ("G", GIB),
            ("GB", GIB),
            ("GiB", GIB),
            ("T", 1_024_u64.pow(4)),
            ("TB", 1_024_u64.pow(4)),
            ("TiB", 1_024_u64.pow(4)),
            ("P", 1_024_u64.pow(5)),
            ("PB", 1_024_u64.pow(5)),
            ("PiB", 1_024_u64.pow(5)),
        ];
        for (unit, expected) in unit_cases {
            assert_eq!(parse_memory_bytes(&format!("1{unit}")), Some(expected));
        }

        assert_eq!(parse_memory_bytes("  1.5 GiB\t"), Some(GIB + GIB / 2));
        assert_eq!(parse_memory_bytes("1.9B"), Some(1));
    }

    proptest! {
        #![proptest_config(prop_config())]

        #[test]
        fn property_memory_bytes_parser_is_total(
            value in string_regex("[0-9]{0,6}(\\.[0-9]{0,3})?\\s*[KMGTPkmgtpiIbB]{0,3}")
                .expect("memory regex")
        ) {
            // The parser must be total: never panic, regardless of the input shape.
            let parsed = parse_memory_bytes(&value);
            // Re-parsing a successfully parsed integer byte count is idempotent.
            if let Some(bytes) = parsed {
                prop_assert_eq!(parse_memory_bytes(&bytes.to_string()), Some(bytes));
            }
        }
    }
}
