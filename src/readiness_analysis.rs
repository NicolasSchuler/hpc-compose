//! Pure readiness-spec analysis shared by planning, preflight, and command
//! presentation adapters.

use crate::spec::ReadinessSpec;

/// A statically derived TCP/HTTP endpoint.
///
/// This is a neutral internal value: it is neither serialized nor associated
/// with a service name. Presentation adapters add their own service identity
/// and output contract where needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadinessEndpoint {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) url: Option<String>,
}

/// Readiness behavior after applying the shared host and timeout defaults.
///
/// Values borrow directly from the source spec where possible. Presentation
/// adapters remain responsible for their own labels, paths, quoting, and
/// serialization contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectiveReadiness<'a> {
    Sleep {
        configured_seconds: u64,
        effective_seconds: u64,
    },
    Tcp {
        host: &'a str,
        port: u16,
        timeout_seconds: u64,
    },
    Log {
        pattern: &'a str,
        timeout_seconds: u64,
    },
    Http {
        url: &'a str,
        status_code: u16,
        timeout_seconds: u64,
    },
}

/// Applies the established readiness defaults and an optional command-level
/// timeout override without adding presentation or execution policy.
pub(crate) fn effective_readiness(
    readiness: &ReadinessSpec,
    timeout_override: Option<u64>,
) -> EffectiveReadiness<'_> {
    match readiness {
        ReadinessSpec::Sleep { seconds } => EffectiveReadiness::Sleep {
            configured_seconds: *seconds,
            effective_seconds: timeout_override.unwrap_or(*seconds),
        },
        ReadinessSpec::Tcp {
            host,
            port,
            timeout_seconds,
        } => EffectiveReadiness::Tcp {
            host: host.as_deref().unwrap_or("127.0.0.1"),
            port: *port,
            timeout_seconds: timeout_override.or(*timeout_seconds).unwrap_or(60),
        },
        ReadinessSpec::Log {
            pattern,
            timeout_seconds,
        } => EffectiveReadiness::Log {
            pattern,
            timeout_seconds: timeout_override.or(*timeout_seconds).unwrap_or(60),
        },
        ReadinessSpec::Http {
            url,
            status_code,
            timeout_seconds,
        } => EffectiveReadiness::Http {
            url,
            status_code: *status_code,
            timeout_seconds: timeout_override.or(*timeout_seconds).unwrap_or(60),
        },
    }
}

/// Returns `true` when the host string matches a localhost address.
pub(crate) fn is_localhost_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Extracts the hostname from an HTTP/HTTPS URL.
///
/// Handles IPv6 bracket notation, userinfo (`user@host`), and port suffixes.
/// Returns `None` for malformed or empty authorities.
pub(crate) fn extract_http_host(url: &str) -> Option<&str> {
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
pub(crate) fn readiness_uses_implicit_localhost(readiness: Option<&ReadinessSpec>) -> bool {
    match readiness {
        None | Some(ReadinessSpec::Sleep { .. } | ReadinessSpec::Log { .. }) => false,
        Some(ReadinessSpec::Tcp { host, .. }) => host.as_deref().is_none_or(is_localhost_host),
        Some(ReadinessSpec::Http { url, .. }) => {
            extract_http_host(url).is_none_or(is_localhost_host)
        }
    }
}

/// Derives the descriptive endpoint represented by one readiness spec.
///
/// Sleep and log readiness have no port and return `None`. An implicit-host TCP
/// readiness uses the established `<host>` placeholder. HTTP URL bytes are
/// retained exactly for presentation consumers.
pub(crate) fn readiness_endpoint(readiness: &ReadinessSpec) -> Option<ReadinessEndpoint> {
    match readiness {
        ReadinessSpec::Tcp { port, host, .. } => Some(ReadinessEndpoint {
            host: host.clone().unwrap_or_else(|| "<host>".to_string()),
            port: *port,
            url: None,
        }),
        ReadinessSpec::Http { url, .. } => {
            let (host, port) = http_host_port(url);
            Some(ReadinessEndpoint {
                host,
                port,
                url: Some(url.clone()),
            })
        }
        ReadinessSpec::Sleep { .. } | ReadinessSpec::Log { .. } => None,
    }
}

/// Best-effort host+port from an HTTP(S) URL authority. This parser is kept
/// separate from [`extract_http_host`]: endpoint presentation and locality
/// classification intentionally have different malformed-input policies.
fn http_host_port(url: &str) -> (String, u16) {
    let default_port = if url.starts_with("https://") { 443 } else { 80 };
    let placeholder = || ("<host>".to_string(), default_port);
    let Some((_, after_scheme)) = url.split_once("://") else {
        return placeholder();
    };
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return placeholder();
    }
    if let Some(rest) = authority.strip_prefix('[') {
        // IPv6 literal: [::1]:8000
        let Some(end) = rest.find(']') else {
            return placeholder();
        };
        let host = rest[..end].to_string();
        let port = rest[end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port);
        return (host, port);
    }
    match authority.split_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(default_port)),
        None => (authority.to_string(), default_port),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_readiness(url: &str) -> ReadinessSpec {
        ReadinessSpec::Http {
            url: url.into(),
            status_code: 200,
            timeout_seconds: None,
        }
    }

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
    fn extract_http_host_handles_standard_ipv6_and_userinfo_urls() {
        let cases = [
            ("http://example.com/path", Some("example.com")),
            ("https://example.com:8080/health", Some("example.com")),
            ("http://127.0.0.1:3000/api", Some("127.0.0.1")),
            ("http://[::1]:8080/health", Some("::1")),
            ("https://[2001:db8::1]/path", Some("2001:db8::1")),
            (
                "https://user@host.example.com/path",
                Some("host.example.com"),
            ),
            (
                "https://user:pass@host.example.com/path",
                Some("host.example.com"),
            ),
        ];
        for (url, expected) in cases {
            assert_eq!(extract_http_host(url), expected, "host case {url:?}");
        }
    }

    #[test]
    fn extract_http_host_preserves_malformed_authority_classification() {
        let cases = [
            ("http:///health", None),
            ("http://user@/health", None),
            ("http://[::1", None),
            ("http://[]:8080/health", Some("")),
            ("http://:8080/health", Some("")),
            ("http://[::1]trailing:8080/health", Some("::1")),
            ("not-a-url", None),
            ("", None),
        ];
        for (url, expected) in cases {
            assert_eq!(extract_http_host(url), expected, "host case {url:?}");
        }
    }

    #[test]
    fn readiness_uses_implicit_localhost_covers_every_variant() {
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

        for (host, expected) in [
            (None, true),
            (Some("localhost"), true),
            (Some("127.0.0.1"), true),
            (Some("::1"), true),
            (Some("10.0.0.1"), false),
        ] {
            assert_eq!(
                readiness_uses_implicit_localhost(Some(&ReadinessSpec::Tcp {
                    host: host.map(str::to_string),
                    port: 8080,
                    timeout_seconds: None,
                })),
                expected,
                "TCP host {host:?}"
            );
        }

        let cases = [
            ("http://127.0.0.1:8080/health", true),
            ("http://10.0.0.1:8080/health", false),
            ("http://[::1]:8080/health", true),
            ("http://[2001:db8::1]:8080/health", false),
            ("http:///health", true),
            ("http://user@/health", true),
            ("http://[::1", true),
            ("http://[]:8080/health", false),
            ("http://:8080/health", false),
            ("http://[::1]trailing:8080/health", true),
            ("HTTPS://localhost/health", true),
            ("https://user:pass@localhost/health", true),
        ];
        for (url, expected) in cases {
            assert_eq!(
                readiness_uses_implicit_localhost(Some(&http_readiness(url))),
                expected,
                "HTTP locality case {url:?}"
            );
        }
    }

    #[test]
    fn endpoint_authority_parser_preserves_exact_policy() {
        let cases = [
            ("http://node02:9000/health", ("node02".to_string(), 9000)),
            ("https://x/", ("x".to_string(), 443)),
            ("http://y/", ("y".to_string(), 80)),
            (
                "https://user:pass@host.example:8443/p",
                ("host.example".to_string(), 8443),
            ),
            (
                "http://[2001:db8::7]:8080/",
                ("2001:db8::7".to_string(), 8080),
            ),
            ("https://[2001:db8::7]/", ("2001:db8::7".to_string(), 443)),
            ("http:///health", ("<host>".to_string(), 80)),
            ("http://user@/health", ("<host>".to_string(), 80)),
            ("http://:8080/health", (String::new(), 8080)),
            ("http://[]:8081/health", (String::new(), 8081)),
            ("http://[::1]trailing:8083/", ("::1".to_string(), 80)),
            (
                "https://node.invalid:nope/",
                ("node.invalid".to_string(), 443),
            ),
            (
                "HTTPS://secure.example/",
                ("secure.example".to_string(), 80),
            ),
            ("garbage", ("<host>".to_string(), 80)),
            ("https://[2001:db8::7", ("<host>".to_string(), 443)),
        ];
        for (url, expected) in cases {
            assert_eq!(http_host_port(url), expected, "authority case {url:?}");
        }
    }

    #[test]
    fn readiness_endpoint_preserves_variants_placeholder_and_url_bytes() {
        assert_eq!(
            readiness_endpoint(&ReadinessSpec::Sleep { seconds: 1 }),
            None
        );
        assert_eq!(
            readiness_endpoint(&ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: None,
            }),
            None
        );
        assert_eq!(
            readiness_endpoint(&ReadinessSpec::Tcp {
                host: None,
                port: 7001,
                timeout_seconds: Some(2),
            }),
            Some(ReadinessEndpoint {
                host: "<host>".into(),
                port: 7001,
                url: None,
            })
        );
        assert_eq!(
            readiness_endpoint(&ReadinessSpec::Tcp {
                host: Some("node02".into()),
                port: 7002,
                timeout_seconds: None,
            }),
            Some(ReadinessEndpoint {
                host: "node02".into(),
                port: 7002,
                url: None,
            })
        );

        let original_url = "HTTPS://user:pass@[2001:db8::7]:nope/health?x=%2F";
        assert_eq!(
            readiness_endpoint(&http_readiness(original_url)),
            Some(ReadinessEndpoint {
                host: "2001:db8::7".into(),
                port: 80,
                url: Some(original_url.into()),
            })
        );
    }
}
