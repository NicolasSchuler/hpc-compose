//! Shared MPI planning and diagnostic helpers.

use crate::runtime_plan::RuntimeService;
use crate::spec::MpiProfile;

pub(crate) fn preferred_mpi_type_description(profile: MpiProfile) -> &'static str {
    match profile {
        MpiProfile::Openmpi => "pmix/pmix_v* or pmi2",
        MpiProfile::Mpich => "pmi2 or pmix/pmix_v*",
        MpiProfile::IntelMpi => "pmi2",
    }
}

pub(crate) fn advertised_mpi_types(output: &str) -> Vec<String> {
    let mut values = output
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '+')))
        .filter(|token| mpi_advertised_token_looks_useful(token))
        .map(str::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn mpi_advertised_token_looks_useful(token: &str) -> bool {
    if token.is_empty() || token.starts_with('-') {
        return false;
    }
    let lower = token.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "mpi"
            | "plugin"
            | "plugins"
            | "type"
            | "types"
            | "are"
            | "available"
            | "specific"
            | "version"
            | "versions"
    ) {
        return false;
    }
    token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
}

pub(crate) fn resolved_rank_count(service: &RuntimeService) -> u32 {
    service
        .placement
        .ntasks
        .or_else(|| {
            service
                .placement
                .ntasks_per_node
                // Planning rejects an overflowing geometry. Saturation keeps
                // diagnostics total for manually constructed RuntimePlans.
                .map(|per_node| per_node.saturating_mul(service.placement.nodes))
        })
        .unwrap_or(1)
}
