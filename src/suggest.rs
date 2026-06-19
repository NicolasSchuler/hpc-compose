//! Tiny "did you mean" suggestion helper for diagnostics.
//!
//! Returns the closest candidate to a target string by Levenshtein edit
//! distance, bounded so that unrelated typos do not produce misleading
//! suggestions. Kept dependency-free and small on purpose.

/// Default per-target edit-distance ceiling.
///
/// Grows with target length so that long identifiers (for example
/// `service_completed_successfully`) still match on reasonable typos, while
/// short garbage tokens rarely produce false suggestions.
fn default_max_distance(target: &str) -> usize {
    (target.len() / 3).max(2)
}

/// Returns the closest candidate to `target` within `max_distance` edits.
///
/// Comparison is case-insensitive; the returned string keeps the candidate's
/// original casing. When two candidates tie, the first one wins.
#[must_use]
pub fn nearest<'a>(target: &str, candidates: &[&'a str], max_distance: usize) -> Option<&'a str> {
    let target_lower = target.to_ascii_lowercase();
    let mut best: Option<(&'a str, usize)> = None;
    for candidate in candidates {
        let dist = levenshtein(&target_lower, &candidate.to_ascii_lowercase());
        if dist > max_distance {
            continue;
        }
        match best {
            Some((_, best_dist)) if dist >= best_dist => {}
            _ => best = Some((*candidate, dist)),
        }
    }
    best.map(|(candidate, _)| candidate)
}

/// Returns the closest candidate to `target` using the default length-based
/// distance ceiling.
#[must_use]
pub fn nearest_default<'a>(target: &str, candidates: &[&'a str]) -> Option<&'a str> {
    nearest(target, candidates, default_max_distance(target))
}

/// Classic two-row Levenshtein edit distance over `char`s.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev = (0..=b.len()).collect::<Vec<_>>();
    let mut curr = vec![0_usize; b.len() + 1];
    for i in 1..=a.len() {
        curr[0] = i;
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_handles_basic_edits() {
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("flaw", "lawn"), 2);
        assert_eq!(levenshtein("same", "same"), 0);
    }

    #[test]
    fn nearest_picks_closest_within_bound() {
        let candidates = [
            "service_started",
            "service_healthy",
            "service_completed_successfully",
        ];
        assert_eq!(
            nearest_default("service_start", &candidates),
            Some("service_started")
        );
        assert_eq!(
            nearest_default("SERVICE_HEALTHY", &candidates),
            Some("service_healthy")
        );
        assert_eq!(
            nearest_default("service_completed_succesfully", &candidates),
            Some("service_completed_successfully")
        );
    }

    #[test]
    fn nearest_returns_none_when_far() {
        let candidates = ["image", "command", "environment"];
        assert_eq!(
            nearest_default("completely-unrelated-token", &candidates),
            None
        );
    }

    #[test]
    fn nearest_matches_short_typos() {
        let candidates = ["image", "command", "environment", "volumes", "depends_on"];
        assert_eq!(nearest_default("comand", &candidates), Some("command"));
        assert_eq!(
            nearest_default("depend_on", &candidates),
            Some("depends_on")
        );
        assert_eq!(nearest_default("volums", &candidates), Some("volumes"));
    }

    #[test]
    fn nearest_respects_explicit_max_distance() {
        let candidates = ["abc", "xyz"];
        assert_eq!(nearest("abc", &candidates, 0), Some("abc"));
        assert_eq!(nearest("abd", &candidates, 0), None);
        assert_eq!(nearest("abd", &candidates, 1), Some("abc"));
    }
}
