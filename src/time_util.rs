//! Shared time helpers and duration constants.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const SECONDS_PER_HOUR: u64 = 60 * 60;
pub(crate) const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;

pub(crate) fn system_time_to_unix(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

pub(crate) fn unix_timestamp_now() -> u64 {
    system_time_to_unix(SystemTime::now()).unwrap_or(0)
}

pub(crate) fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
