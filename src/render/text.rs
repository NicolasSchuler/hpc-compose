pub(super) fn bash_array_literal(items: &[String]) -> String {
    let body = items
        .iter()
        .map(|item| shell_quote(item))
        .collect::<Vec<_>>()
        .join(" ");
    format!("({body})")
}

pub(super) fn service_token(value: &str) -> String {
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

pub(super) fn service_step_name(value: &str) -> String {
    format!("hpc-compose:{}", service_token(value))
}

/// Converts a service name into the tracked log file name used on disk.
pub fn log_file_name_for_service(value: &str) -> String {
    format!("{}.log", service_token(value))
}

pub(super) fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

pub(super) fn flag(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}
