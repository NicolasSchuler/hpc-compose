pub(super) fn bash_array_literal(items: &[String]) -> String {
    let body = items
        .iter()
        .map(|item| shell_quote(item))
        .collect::<Vec<_>>()
        .join(" ");
    format!("({body})")
}

/// Converts a service name into the tracked log file name used on disk.
pub fn log_file_name_for_service(value: &str) -> String {
    crate::tracked_paths::log_file_name_for_service(value)
}

pub(super) fn shell_quote(value: &str) -> String {
    // Delegate to the single canonical, property-tested quoter so the
    // security-critical render path cannot drift from it.
    crate::shell_quote::quote(value)
}

pub(super) fn flag(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}
