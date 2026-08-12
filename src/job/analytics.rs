//! Pure primitives shared by tracked-job analytics consumers.

use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::memory::{GIB, parse_memory_bytes};

pub(super) fn parse_tres_map(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for segment in raw.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (key, value) = segment
            .split_once('=')
            .context(format!("invalid TRES entry '{segment}'"))?;
        values.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(values)
}

pub(super) fn find_tres_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    if let Some(value) = values.get(key) {
        return Some(value.clone());
    }
    let prefix = format!("{key}:");
    for (candidate, value) in values {
        if candidate.starts_with(&prefix) {
            return Some(value.clone());
        }
    }
    None
}

pub(super) fn tres_gpu_count(values: &BTreeMap<String, String>) -> Option<u64> {
    find_tres_value(values, "gres/gpu")
        .or_else(|| find_tres_value(values, "gpu"))
        .and_then(|value| parse_u64(Some(&value)))
}

pub(super) fn tres_memory_bytes(values: &BTreeMap<String, String>) -> Option<u64> {
    values
        .get("mem")
        .or_else(|| values.get("memory"))
        .and_then(|value| parse_memory_bytes(value))
}

pub(super) fn estimated_step_memory_bytes(
    max_rss: &str,
    ave_rss: &str,
    ntasks: &str,
) -> Option<u64> {
    let max_rss = parse_memory_bytes(max_rss);
    let ave_rss_total = parse_memory_bytes(ave_rss)
        .map(|value| value.saturating_mul(ntasks.trim().parse::<u64>().unwrap_or(1).max(1)));
    max_option(max_rss, ave_rss_total)
}

pub(super) fn gpu_device_key(
    uuid: Option<&str>,
    node: Option<&str>,
    index: Option<&str>,
) -> String {
    uuid.filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}:{}",
                node.unwrap_or("unknown-node"),
                index.unwrap_or("unknown-index")
            )
        })
}

pub(super) fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw?.trim().parse::<f64>().ok()
}

pub(super) fn parse_u64(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse::<u64>().ok()
}

pub(super) fn format_bytes_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tres_grammar_and_lookup_preserve_ordering_and_errors() {
        let mut values =
            parse_tres_map(" , cpu=1, gres/gpu:zeta=7, cpu=4, gres/gpu:alpha=2, ").expect("tres");
        assert_eq!(values.get("cpu").map(String::as_str), Some("4"));
        assert_eq!(find_tres_value(&values, "gres/gpu").as_deref(), Some("2"));
        values.insert("gres/gpu".into(), "3".into());
        assert_eq!(find_tres_value(&values, "gres/gpu").as_deref(), Some("3"));
        assert_eq!(
            parse_tres_map(" cpu=1, broken-entry ")
                .expect_err("invalid tres")
                .to_string(),
            "invalid TRES entry 'broken-entry'"
        );
    }

    #[test]
    fn tres_resolution_preserves_precedence_and_invalid_exact_blocking() {
        let mut values = BTreeMap::from([
            ("gpu".to_string(), "8".to_string()),
            ("gres/gpu:zeta".to_string(), "7".to_string()),
            ("gres/gpu:alpha".to_string(), "2".to_string()),
        ]);
        assert_eq!(tres_gpu_count(&values), Some(2));
        values.insert("gres/gpu".into(), "3".into());
        assert_eq!(tres_gpu_count(&values), Some(3));
        values.insert("gres/gpu".into(), "unknown".into());
        assert_eq!(tres_gpu_count(&values), None);

        let memory = BTreeMap::from([
            ("mem".to_string(), "unknown".to_string()),
            ("memory".to_string(), "2G".to_string()),
        ]);
        assert_eq!(tres_memory_bytes(&memory), None);
        assert_eq!(
            tres_memory_bytes(&BTreeMap::from([(
                "memory".to_string(),
                " 1.5 GiB ".to_string(),
            )])),
            Some(GIB + GIB / 2)
        );
        assert_eq!(
            tres_memory_bytes(&BTreeMap::from([("mem:node".into(), "4G".into())])),
            None
        );
    }

    #[test]
    fn primitive_parsing_memory_and_device_keys_preserve_edge_semantics() {
        assert_eq!(parse_u64(Some(" 42 \t")), Some(42));
        assert_eq!(parse_u64(Some("")), None);
        assert_eq!(parse_u64(Some("UNKNOWN")), None);
        assert_eq!(parse_u64(None), None);
        assert_eq!(parse_f64(Some(" 3.5 \t")), Some(3.5));
        assert_eq!(parse_f64(Some("unknown")), None);

        assert_eq!(estimated_step_memory_bytes("3G", "2G", "4"), Some(8 * GIB));
        assert_eq!(
            estimated_step_memory_bytes("", "99999999999P", "2"),
            Some(u64::MAX)
        );
        assert_eq!(estimated_step_memory_bytes("", "2G", "0"), Some(2 * GIB));
        assert_eq!(
            estimated_step_memory_bytes("", "2G", "not-a-number"),
            Some(2 * GIB)
        );

        assert_eq!(
            gpu_device_key(Some("  GPU-0  "), None, Some("0")),
            "  GPU-0  "
        );
        assert_eq!(
            gpu_device_key(Some("   "), Some(" node "), Some(" 7 ")),
            " node : 7 "
        );
        assert_eq!(
            gpu_device_key(None, None, None),
            "unknown-node:unknown-index"
        );
        assert_eq!(format_bytes_gib(GIB + GIB / 2), "1.5 GiB");
    }
}
