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
