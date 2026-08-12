use std::collections::{BTreeMap, BTreeSet};

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

pub(crate) fn extract_human_sbatch_job_id(text: &str) -> Option<&str> {
    const MARKER: &str = "Submitted batch job ";
    let rest = &text[text.find(MARKER)? + MARKER.len()..];
    let len = rest
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .map(char::len_utf8)
        .sum::<usize>();
    (len > 0).then(|| &rest[..len])
}

pub(crate) fn registry_host_for_remote(remote: &str) -> String {
    let without_scheme = remote.split("://").nth(1).unwrap_or(remote);
    if let Some((host, _)) = without_scheme.split_once('#') {
        return host.to_string();
    }

    let has_path_component = without_scheme.contains('/');
    if !has_path_component {
        return "registry-1.docker.io".to_string();
    }

    let first = without_scheme.split('/').next().unwrap_or(without_scheme);
    if first == "localhost" || first.contains('.') || (first.contains(':') && has_path_component) {
        first.to_string()
    } else {
        "registry-1.docker.io".to_string()
    }
}

pub(crate) fn service_token(value: &str) -> String {
    let mut token = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            token.push(byte as char);
        } else {
            token.push_str(&format!("_x{byte:02x}_"));
        }
    }
    token
}

pub(crate) fn service_step_name(value: &str) -> String {
    format!("hpc-compose:{}", service_token(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RendezvousNameIssue {
    Empty,
    ReservedPathComponent,
    UnsupportedCharacter,
}

pub(crate) fn rendezvous_name_issue(value: &str) -> Option<RendezvousNameIssue> {
    if value.trim().is_empty() {
        return Some(RendezvousNameIssue::Empty);
    }
    if matches!(value, "." | "..") {
        return Some(RendezvousNameIssue::ReservedPathComponent);
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        None
    } else {
        Some(RendezvousNameIssue::UnsupportedCharacter)
    }
}

pub(crate) fn rendezvous_env_token(name: &str) -> String {
    let mut token = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() {
            token.push((byte as char).to_ascii_uppercase());
        } else {
            token.push('_');
        }
    }
    if token.is_empty() {
        "_".to_string()
    } else {
        token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RendezvousEnvTokenCollision<'a> {
    pub(crate) first_name: &'a str,
    pub(crate) second_name: &'a str,
    pub(crate) token: String,
}

pub(crate) fn rendezvous_env_token_collision<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Option<RendezvousEnvTokenCollision<'a>> {
    let mut names_by_token = BTreeMap::new();
    for name in names {
        let token = rendezvous_env_token(name);
        if let Some(&first_name) = names_by_token.get(&token) {
            return Some(RendezvousEnvTokenCollision {
                first_name,
                second_name: name,
                token,
            });
        }
        names_by_token.insert(token, name);
    }
    None
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

    #[test]
    fn human_sbatch_job_id_preserves_exact_marker_policy() {
        let cases = [
            ("Submitted batch job 12345\n", Some("12345")),
            (" \tSubmitted batch job 42 \t", Some("42")),
            ("Submitted batch job  42", None),
            ("submitted batch job 7", None),
            ("banner\nSubmitted batch job 81\nfooter", Some("81")),
            (
                "Reservation 7 active\nSubmitted batch job 13579",
                Some("13579"),
            ),
            ("Submitted batch job 42 (cluster=gpu)", Some("42")),
            ("Submitted batch job nope\nSubmitted batch job 99", None),
            ("", None),
            ("   \n  ", None),
            ("préface 🙂\nSubmitted batch job 314終", Some("314")),
            ("Submitted batch job １２３", None),
            ("no submission marker 123", None),
            ("Submitted batch job 12345\nnodes 8", Some("12345")),
        ];

        for (input, expected) in cases {
            assert_eq!(
                extract_human_sbatch_job_id(input),
                expected,
                "input {input:?}"
            );
        }
    }

    #[test]
    fn registry_host_policy_preserves_defaults_separators_and_explicit_hosts() {
        let cases = [
            ("docker://redis:7", "registry-1.docker.io"),
            ("docker://library/redis:7", "registry-1.docker.io"),
            ("redis:7", "registry-1.docker.io"),
            ("docker://ghcr.io/acme/app:1", "ghcr.io"),
            (
                "docker://registry.scc.kit.edu#proj/app:latest",
                "registry.scc.kit.edu",
            ),
            ("docker://localhost:5000/app:latest", "localhost:5000"),
            ("podman://myhost:5000/app:latest", "myhost:5000"),
        ];

        for (remote, expected) in cases {
            assert_eq!(
                registry_host_for_remote(remote),
                expected,
                "remote {remote:?}"
            );
        }
    }

    #[test]
    fn service_identity_encodes_non_alphanumeric_utf8_bytes() {
        assert_eq!(service_token("api.worker-1"), "api_x2e_worker_x2d_1");
        assert_eq!(service_token("café"), "caf_xc3__xa9_");
        assert_eq!(service_token("_"), "_x5f_");
        assert_eq!(service_token(""), "");
        assert_eq!(
            service_step_name("api.worker-1"),
            "hpc-compose:api_x2e_worker_x2d_1"
        );
    }

    #[test]
    fn rendezvous_name_classifier_preserves_ascii_grammar_and_empty_split() {
        for byte in 0_u8..=127 {
            let value = format!("a{}b", char::from(byte));
            let expected = if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.') {
                None
            } else {
                Some(RendezvousNameIssue::UnsupportedCharacter)
            };
            assert_eq!(
                rendezvous_name_issue(&value),
                expected,
                "ASCII byte 0x{byte:02x} in {value:?}"
            );
        }

        for value in ["-", "_", "A0.z-_"] {
            assert_eq!(rendezvous_name_issue(value), None, "value {value:?}");
        }
        for value in [".", ".."] {
            assert_eq!(
                rendezvous_name_issue(value),
                Some(RendezvousNameIssue::ReservedPathComponent),
                "value {value:?}"
            );
        }
        for value in ["", " \t\n", "\u{00a0}"] {
            assert_eq!(
                rendezvous_name_issue(value),
                Some(RendezvousNameIssue::Empty),
                "value {value:?}"
            );
        }
        for value in [" a", "a b", "a/b", "a\\b", "a\0b", "é", "🙂"] {
            assert_eq!(
                rendezvous_name_issue(value),
                Some(RendezvousNameIssue::UnsupportedCharacter),
                "value {value:?}"
            );
        }
    }

    #[test]
    fn rendezvous_env_token_preserves_byte_mapping_and_collisions() {
        assert_eq!(rendezvous_env_token(""), "_");
        assert_eq!(rendezvous_env_token("model-server.v1"), "MODEL_SERVER_V1");
        assert_eq!(rendezvous_env_token("é"), "__");
        assert_eq!(rendezvous_env_token("🙂"), "____");
        for name in ["a-b", "a.b", "a_b"] {
            assert_eq!(rendezvous_env_token(name), "A_B", "name {name:?}");
        }

        let collision = rendezvous_env_token_collision(["model.server-v1", "model_server_v1"])
            .expect("colliding discovery names");
        assert_eq!(collision.first_name, "model.server-v1");
        assert_eq!(collision.second_name, "model_server_v1");
        assert_eq!(collision.token, "MODEL_SERVER_V1");
        assert!(rendezvous_env_token_collision(["model.server-v1", "trainer"]).is_none());
    }
}
