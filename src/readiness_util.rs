//! Shared readiness-check utilities used by planner, preflight, and render.

use crate::spec::ReadinessSpec;

/// Returns `true` when the host string matches a localhost address.
pub fn is_localhost_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Extracts the hostname from an HTTP/HTTPS URL.
///
/// Handles IPv6 bracket notation, userinfo (`user@host`), and port suffixes.
/// Returns `None` for malformed or empty authorities.
pub fn extract_http_host(url: &str) -> Option<&str> {
    let (_, after_scheme) = url.split_once("://")?;
    let authority = after_scheme.split('/').next()?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return None;
    }
    if authority.starts_with('[') {
        let end = authority.find(']')?;
        return Some(&authority[1..end]);
    }
    Some(authority.split(':').next().unwrap_or(authority))
}

/// Returns `true` when the readiness check relies on implicit localhost
/// semantics (TCP with no explicit host, or HTTP with a localhost URL).
pub fn readiness_uses_implicit_localhost(readiness: Option<&ReadinessSpec>) -> bool {
    match readiness {
        None | Some(ReadinessSpec::Sleep { .. } | ReadinessSpec::Log { .. }) => false,
        Some(ReadinessSpec::Tcp { host, .. }) => host.as_deref().is_none_or(is_localhost_host),
        Some(ReadinessSpec::Http { url, .. }) => {
            extract_http_host(url).is_none_or(is_localhost_host)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ReadinessSpec;

    #[test]
    fn is_localhost_host_matches_known_addresses() {
        assert!(is_localhost_host("localhost"));
        assert!(is_localhost_host("127.0.0.1"));
        assert!(is_localhost_host("::1"));
        assert!(!is_localhost_host("192.168.1.1"));
        assert!(!is_localhost_host("example.com"));
        assert!(!is_localhost_host("0.0.0.0"));
    }

    #[test]
    fn extract_http_host_handles_standard_urls() {
        assert_eq!(
            extract_http_host("http://example.com/path"),
            Some("example.com")
        );
        assert_eq!(
            extract_http_host("https://example.com:8080/health"),
            Some("example.com")
        );
        assert_eq!(
            extract_http_host("http://127.0.0.1:3000/api"),
            Some("127.0.0.1")
        );
    }

    #[test]
    fn extract_http_host_handles_ipv6_brackets() {
        assert_eq!(extract_http_host("http://[::1]:8080/health"), Some("::1"));
        assert_eq!(
            extract_http_host("https://[2001:db8::1]/path"),
            Some("2001:db8::1")
        );
    }

    #[test]
    fn extract_http_host_handles_userinfo() {
        assert_eq!(
            extract_http_host("https://user@host.example.com/path"),
            Some("host.example.com")
        );
        assert_eq!(
            extract_http_host("https://user:pass@host.example.com/path"),
            Some("host.example.com")
        );
    }

    #[test]
    fn extract_http_host_returns_none_for_invalid() {
        assert_eq!(extract_http_host("not-a-url"), None);
        assert_eq!(extract_http_host(""), None);
    }

    #[test]
    fn readiness_uses_implicit_localhost_covers_all_variants() {
        assert!(!readiness_uses_implicit_localhost(None));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Sleep { seconds: 5 }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: None,
            }
        )));

        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: None,
                port: 8080,
                timeout_seconds: None,
            }
        )));
        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: Some("localhost".into()),
                port: 8080,
                timeout_seconds: None,
            }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: Some("10.0.0.1".into()),
                port: 8080,
                timeout_seconds: None,
            }
        )));

        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Http {
                url: "http://127.0.0.1:8080/health".into(),
                status_code: 200,
                timeout_seconds: None,
            }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Http {
                url: "http://10.0.0.1:8080/health".into(),
                status_code: 200,
                timeout_seconds: None,
            }
        )));
    }
}
