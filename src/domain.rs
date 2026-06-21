use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

pub(crate) enum MountParts<'a> {
    HostContainer {
        host: &'a str,
        container: &'a str,
        mode: Option<&'a str>,
    },
    UnsupportedMode(&'a str),
    InvalidShape,
}

pub(crate) fn split_mount_parts(value: &str) -> MountParts<'_> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [host, container] => MountParts::HostContainer {
            host,
            container,
            mode: None,
        },
        [host, container, mode @ ("ro" | "rw")] => MountParts::HostContainer {
            host,
            container,
            mode: Some(mode),
        },
        [_, _, mode] => MountParts::UnsupportedMode(mode),
        _ => MountParts::InvalidShape,
    }
}

pub(crate) fn parse_node_index_ranges(value: &str, label: &str) -> Result<Vec<(u32, u32)>> {
    if value.trim().is_empty() {
        bail!("{label} must not be empty");
    }

    let mut ranges = Vec::new();
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            bail!("{label} contains an empty range segment");
        }
        let (start, end) = match part.split_once('-') {
            Some((start, end)) => (start.trim(), end.trim()),
            None => (part, part),
        };
        if start.is_empty() || end.is_empty() {
            bail!("{label} contains an incomplete range '{part}'");
        }
        let start = start
            .parse::<u32>()
            .with_context(|| format!("{label} contains invalid node index '{start}'"))?;
        let end = end
            .parse::<u32>()
            .with_context(|| format!("{label} contains invalid node index '{end}'"))?;
        if end < start {
            bail!("{label} contains descending range '{part}'");
        }
        ranges.push((start, end));
    }
    Ok(ranges)
}

pub(crate) fn resolve_node_index_expr(
    value: &str,
    allocation_nodes: u32,
    label: &str,
) -> Result<Vec<u32>> {
    let mut indices = BTreeSet::new();
    for (start, end) in parse_node_index_ranges(value, label)? {
        if end >= allocation_nodes {
            bail!(
                "{label} references node index {end}, but the allocation only has {} node(s)",
                allocation_nodes
            );
        }
        for index in start..=end {
            indices.insert(index);
        }
    }
    Ok(indices.into_iter().collect())
}

pub(crate) fn artifact_cache_key(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

pub(crate) fn short_digest_prefix(hash: &str) -> &str {
    &hash[..16]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_mount_parts_classifies_shapes() {
        assert!(matches!(
            split_mount_parts("/h:/c"),
            MountParts::HostContainer { mode: None, .. }
        ));
        assert!(matches!(
            split_mount_parts("/h:/c:ro"),
            MountParts::HostContainer {
                mode: Some("ro"),
                ..
            }
        ));
        assert!(matches!(
            split_mount_parts("/h:/c:rw"),
            MountParts::HostContainer {
                mode: Some("rw"),
                ..
            }
        ));
        assert!(matches!(
            split_mount_parts("/h:/c:rx"),
            MountParts::UnsupportedMode("rx")
        ));
        assert!(matches!(
            split_mount_parts("/host-only"),
            MountParts::InvalidShape
        ));
        assert!(matches!(
            split_mount_parts("a:b:c:d"),
            MountParts::InvalidShape
        ));
    }

    #[test]
    fn parse_node_index_ranges_parses_and_rejects() {
        assert_eq!(
            parse_node_index_ranges("0,2-3", "nodes").unwrap(),
            vec![(0, 0), (2, 3)]
        );
        for (input, needle) in [
            ("  ", "must not be empty"),
            ("0,,1", "empty range segment"),
            ("1-", "incomplete range"),
            ("a", "invalid node index"),
            ("3-1", "descending range"),
        ] {
            let err = parse_node_index_ranges(input, "nodes")
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "for {input:?} got: {err}");
        }
    }

    #[test]
    fn resolve_node_index_expr_bounds_and_dedups() {
        assert_eq!(
            resolve_node_index_expr("0-2,1", 4, "nodes").unwrap(),
            vec![0, 1, 2]
        );
        let err = resolve_node_index_expr("0-4", 4, "nodes")
            .unwrap_err()
            .to_string();
        assert!(err.contains("only has 4 node(s)"), "got: {err}");
    }

    #[test]
    fn cache_key_separates_parts_and_digest_prefix_is_16() {
        assert_ne!(
            artifact_cache_key(&["x", "y"]),
            artifact_cache_key(&["x", "z"])
        );
        // The NUL separator prevents the classic concatenation collision.
        assert_ne!(
            artifact_cache_key(&["ab", "c"]),
            artifact_cache_key(&["a", "bc"])
        );
        assert_eq!(short_digest_prefix(&artifact_cache_key(&["x"])).len(), 16);
    }
}
