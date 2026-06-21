//! Pure, IO-free statistics rollup over replicate trial values.
//!
//! Used by the sweep status/observe/results flows to summarize the objective
//! values of replicate trials that share a parameter config into a single
//! mean±std(n) row.

use std::collections::BTreeMap;

/// Mean and population standard deviation over a group of replicate values.
///
/// `std` uses the population convention (divide by `n`), so a single value
/// (`n == 1`) yields `std == 0.0` cleanly. `n` is the number of values that
/// contributed to the rollup.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct ReplicateStats {
    /// Arithmetic mean of the values.
    pub mean: f64,
    /// Population standard deviation (divide by `n`); `0.0` when `n <= 1`.
    pub std: f64,
    /// Number of values that contributed to the rollup.
    pub n: usize,
}

/// Computes the mean and population standard deviation of `values`.
///
/// Returns `None` when `values` is empty (no defined mean). For a single value
/// the standard deviation is `0.0`.
#[must_use]
pub fn replicate_rollup(values: &[f64]) -> Option<ReplicateStats> {
    let n = values.len();
    if n == 0 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / n as f64;
    Some(ReplicateStats {
        mean,
        std: variance.sqrt(),
        n,
    })
}

/// Groups `(config_key, value)` pairs by `config_key`, preserving first-seen
/// order is not guaranteed; the returned map is sorted by key.
///
/// Pairs whose value is `None` still register the group (so empty groups are
/// visible) but contribute no value to the rollup. Callers that only want
/// groups with at least one observed value can filter on the returned vector.
#[must_use]
pub fn group_by_config<'a, I>(pairs: I) -> BTreeMap<String, Vec<f64>>
where
    I: IntoIterator<Item = (&'a str, Option<f64>)>,
{
    let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for (config_key, value) in pairs {
        let entry = groups.entry(config_key.to_string()).or_default();
        if let Some(value) = value {
            entry.push(value);
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_of_empty_is_none() {
        assert_eq!(replicate_rollup(&[]), None);
    }

    #[test]
    fn rollup_single_value_has_zero_std() {
        let stats = replicate_rollup(&[1.5]).expect("rollup");
        assert_eq!(stats.mean, 1.5);
        assert_eq!(stats.std, 0.0);
        assert_eq!(stats.n, 1);
    }

    #[test]
    fn rollup_known_population_std() {
        // values [2, 4, 4, 4, 5, 5, 7, 9]: mean = 5, population std = 2.0.
        let stats = replicate_rollup(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]).expect("rollup");
        assert_eq!(stats.mean, 5.0);
        assert!((stats.std - 2.0).abs() < 1e-12, "std was {}", stats.std);
        assert_eq!(stats.n, 8);
    }

    #[test]
    fn rollup_three_values() {
        // [1, 2, 3]: mean 2, variance ((1)+(0)+(1))/3 = 2/3, std = sqrt(2/3).
        let stats = replicate_rollup(&[1.0, 2.0, 3.0]).expect("rollup");
        assert_eq!(stats.mean, 2.0);
        assert!((stats.std - (2.0_f64 / 3.0).sqrt()).abs() < 1e-12);
        assert_eq!(stats.n, 3);
    }

    #[test]
    fn group_by_config_buckets_values_and_skips_none() {
        let pairs = [
            ("a", Some(1.0)),
            ("b", Some(10.0)),
            ("a", Some(3.0)),
            ("b", None),
            ("c", None),
        ];
        let groups = group_by_config(pairs);
        assert_eq!(groups.get("a"), Some(&vec![1.0, 3.0]));
        assert_eq!(groups.get("b"), Some(&vec![10.0]));
        // A group with only None values is still registered, but empty.
        assert_eq!(groups.get("c"), Some(&Vec::new()));
    }
}
