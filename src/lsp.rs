//! Diagnostics-only stdio Language Server for hpc-compose YAML files.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use lsp_server::{Connection, Message, Notification as ServerNotification, Request, Response};
use lsp_types::notification::{
    DidChangeTextDocument, DidCloseTextDocument, DidOpenTextDocument, DidSaveTextDocument,
    Notification as LspNotification,
};
use lsp_types::{
    Diagnostic, DiagnosticSeverity, DidChangeTextDocumentParams, DidCloseTextDocumentParams,
    DidOpenTextDocumentParams, DidSaveTextDocumentParams, NumberOrString, Position,
    PublishDiagnosticsParams, Range, SaveOptions, ServerCapabilities, TextDocumentSyncCapability,
    TextDocumentSyncKind, TextDocumentSyncOptions, TextDocumentSyncSaveOptions, Uri,
};
use serde_json::json;

use crate::authoring_diagnostics::{
    AuthoringDiagnostic, AuthoringDiagnosticOptions, AuthoringRange, AuthoringSeverity,
    diagnose_document,
};

/// Runtime options for the diagnostics-only LSP server.
#[derive(Debug, Clone)]
pub(crate) struct LspOptions {
    pub(crate) cwd: PathBuf,
    pub(crate) profile: Option<String>,
    pub(crate) settings_file: Option<PathBuf>,
    pub(crate) strict_env: bool,
}

/// Runs the diagnostics-only LSP over stdio.
pub(crate) fn run_stdio(options: LspOptions) -> Result<()> {
    let (connection, io_threads) = Connection::stdio();
    connection
        .initialize(serde_json::to_value(server_capabilities())?)
        .context("failed to initialize LSP connection")?;

    let mut state = LspState::new(options);
    for message in &connection.receiver {
        match message {
            Message::Request(request) => {
                if connection.handle_shutdown(&request)? {
                    break;
                }
                respond_method_not_found(&connection, request)?;
            }
            Message::Response(_) => {}
            Message::Notification(notification) => {
                if notification.method == "exit" {
                    break;
                }
                if let Some(published) = state.handle_notification(&notification)? {
                    connection
                        .sender
                        .send(Message::Notification(publish_notification(published)?))
                        .context("failed to publish diagnostics")?;
                }
            }
        }
    }

    drop(connection);
    io_threads.join()?;
    Ok(())
}

/// Capabilities advertised by the MVP LSP server.
pub(crate) fn server_capabilities() -> ServerCapabilities {
    ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Options(
            TextDocumentSyncOptions {
                open_close: Some(true),
                change: Some(TextDocumentSyncKind::FULL),
                save: Some(TextDocumentSyncSaveOptions::SaveOptions(SaveOptions {
                    include_text: Some(true),
                })),
                ..TextDocumentSyncOptions::default()
            },
        )),
        ..ServerCapabilities::default()
    }
}

#[derive(Debug, Clone)]
struct OpenDocument {
    text: String,
    version: Option<i32>,
}

/// Stateful handler for text-document LSP notifications.
#[derive(Debug)]
pub(crate) struct LspState {
    options: LspOptions,
    documents: BTreeMap<String, OpenDocument>,
}

/// Diagnostics publication requested by an LSP notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedDiagnostics {
    pub(crate) uri: String,
    pub(crate) version: Option<i32>,
    pub(crate) diagnostics: Vec<AuthoringDiagnostic>,
}

impl LspState {
    /// Creates an empty LSP handler state.
    pub(crate) fn new(options: LspOptions) -> Self {
        Self {
            options,
            documents: BTreeMap::new(),
        }
    }

    /// Handles one text-document notification.
    pub(crate) fn handle_notification(
        &mut self,
        notification: &ServerNotification,
    ) -> Result<Option<PublishedDiagnostics>> {
        if notification.method == DidOpenTextDocument::METHOD {
            let params: DidOpenTextDocumentParams =
                serde_json::from_value(notification.params.clone())?;
            return Ok(Some(self.did_open(
                &params.text_document.uri.to_string(),
                params.text_document.text,
                Some(params.text_document.version),
            )));
        }
        if notification.method == DidChangeTextDocument::METHOD {
            let params: DidChangeTextDocumentParams =
                serde_json::from_value(notification.params.clone())?;
            let text = params
                .content_changes
                .into_iter()
                .last()
                .map(|change| change.text)
                .unwrap_or_default();
            return Ok(Some(self.did_change(
                &params.text_document.uri.to_string(),
                text,
                Some(params.text_document.version),
            )));
        }
        if notification.method == DidSaveTextDocument::METHOD {
            let params: DidSaveTextDocumentParams =
                serde_json::from_value(notification.params.clone())?;
            return Ok(self.did_save(&params.text_document.uri.to_string(), params.text));
        }
        if notification.method == DidCloseTextDocument::METHOD {
            let params: DidCloseTextDocumentParams =
                serde_json::from_value(notification.params.clone())?;
            return Ok(Some(self.did_close(&params.text_document.uri.to_string())));
        }
        Ok(None)
    }

    fn did_open(&mut self, uri: &str, text: String, version: Option<i32>) -> PublishedDiagnostics {
        self.documents.insert(
            uri.to_string(),
            OpenDocument {
                text: text.clone(),
                version,
            },
        );
        self.diagnose(uri, &text, version)
    }

    fn did_change(
        &mut self,
        uri: &str,
        text: String,
        version: Option<i32>,
    ) -> PublishedDiagnostics {
        self.documents.insert(
            uri.to_string(),
            OpenDocument {
                text: text.clone(),
                version,
            },
        );
        self.diagnose(uri, &text, version)
    }

    fn did_save(&mut self, uri: &str, text: Option<String>) -> Option<PublishedDiagnostics> {
        if let Some(text) = text {
            let version = self
                .documents
                .get(uri)
                .and_then(|document| document.version);
            self.documents.insert(
                uri.to_string(),
                OpenDocument {
                    text: text.clone(),
                    version,
                },
            );
            return Some(self.diagnose(uri, &text, version));
        }
        let document = self.documents.get(uri)?;
        Some(self.diagnose(uri, &document.text, document.version))
    }

    fn did_close(&mut self, uri: &str) -> PublishedDiagnostics {
        self.documents.remove(uri);
        PublishedDiagnostics {
            uri: uri.to_string(),
            version: None,
            diagnostics: Vec::new(),
        }
    }

    fn diagnose(&self, uri: &str, text: &str, version: Option<i32>) -> PublishedDiagnostics {
        let diagnostics = match file_path_from_uri(uri) {
            Some(path) => diagnose_document(
                &path,
                text,
                &AuthoringDiagnosticOptions {
                    cwd: self.options.cwd.clone(),
                    profile: self.options.profile.clone(),
                    settings_file: self.options.settings_file.clone(),
                    strict_env: self.options.strict_env,
                },
            ),
            None => vec![unsupported_uri_diagnostic(text)],
        };
        PublishedDiagnostics {
            uri: uri.to_string(),
            version,
            diagnostics,
        }
    }
}

fn respond_method_not_found(connection: &Connection, request: Request) -> Result<()> {
    let response = Response::new_err(
        request.id,
        lsp_server::ErrorCode::MethodNotFound as i32,
        "hpc-compose lsp only implements initialize, shutdown, and text document diagnostics"
            .to_string(),
    );
    connection
        .sender
        .send(Message::Response(response))
        .context("failed to send unsupported-request response")
}

fn publish_notification(published: PublishedDiagnostics) -> Result<ServerNotification> {
    let uri = Uri::from_str(&published.uri)
        .with_context(|| format!("failed to parse document URI '{}'", published.uri))?;
    Ok(ServerNotification::new(
        "textDocument/publishDiagnostics".to_string(),
        PublishDiagnosticsParams {
            uri,
            diagnostics: published
                .diagnostics
                .iter()
                .map(to_lsp_diagnostic)
                .collect(),
            version: published.version,
        },
    ))
}

fn to_lsp_diagnostic(diagnostic: &AuthoringDiagnostic) -> Diagnostic {
    Diagnostic {
        range: to_lsp_range(diagnostic.range),
        severity: Some(match diagnostic.severity {
            AuthoringSeverity::Error => DiagnosticSeverity::ERROR,
            AuthoringSeverity::Warning => DiagnosticSeverity::WARNING,
        }),
        code: diagnostic.code.clone().map(NumberOrString::String),
        code_description: None,
        source: Some("hpc-compose".to_string()),
        message: diagnostic.message.clone(),
        related_information: None,
        tags: None,
        data: Some(json!({
            "field": diagnostic.field,
            "recommendation": diagnostic.recommendation,
        })),
    }
}

fn to_lsp_range(range: AuthoringRange) -> Range {
    Range {
        start: Position {
            line: range.start_line,
            character: range.start_character,
        },
        end: Position {
            line: range.end_line,
            character: range.end_character,
        },
    }
}

fn unsupported_uri_diagnostic(text: &str) -> AuthoringDiagnostic {
    AuthoringDiagnostic {
        severity: AuthoringSeverity::Error,
        message: "hpc-compose lsp only diagnoses file:// documents".to_string(),
        code: Some("hpc_compose::authoring::unsupported_uri".to_string()),
        field: None,
        recommendation: Some("Open the compose YAML as a local file URI.".to_string()),
        range: whole_document_range(text),
    }
}

fn whole_document_range(text: &str) -> AuthoringRange {
    let mut line = 0_u32;
    let mut character = 1_u32;
    for (index, current) in text.lines().enumerate() {
        line = index as u32;
        character = current.chars().count().max(1) as u32;
    }
    AuthoringRange {
        start_line: 0,
        start_character: 0,
        end_line: line,
        end_character: character,
    }
}

fn file_path_from_uri(uri: &str) -> Option<PathBuf> {
    let raw = uri.strip_prefix("file://")?;
    let raw = raw.strip_prefix("localhost").unwrap_or(raw);
    if raw.is_empty() {
        return None;
    }
    Some(Path::new(&percent_decode(raw)?).to_path_buf())
}

fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = *bytes.get(index + 1)?;
            let lo = *bytes.get(index + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsp_types::{
        DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
        TextDocumentContentChangeEvent, TextDocumentIdentifier, TextDocumentItem,
        VersionedTextDocumentIdentifier,
    };

    const VALID_SPEC: &str = "\
services:
  app:
    image: alpine:3.20
";

    fn options(cwd: &Path) -> LspOptions {
        LspOptions {
            cwd: cwd.to_path_buf(),
            profile: None,
            settings_file: None,
            strict_env: false,
        }
    }

    fn uri(path: &Path) -> Uri {
        Uri::from_str(&format!("file://{}", path.display())).expect("uri")
    }

    fn notification<N: LspNotification>(params: N::Params) -> ServerNotification {
        ServerNotification::new(N::METHOD.to_string(), params)
    }

    #[test]
    fn initialize_advertises_diagnostics_only_capabilities() {
        let capabilities = server_capabilities();
        assert!(capabilities.completion_provider.is_none());
        assert!(capabilities.hover_provider.is_none());
        assert!(capabilities.code_action_provider.is_none());
        assert!(matches!(
            capabilities.text_document_sync,
            Some(TextDocumentSyncCapability::Options(_))
        ));
    }

    #[test]
    fn did_open_publishes_diagnostics() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let mut state = LspState::new(options(tmp.path()));
        let notification = notification::<DidOpenTextDocument>(DidOpenTextDocumentParams {
            text_document: TextDocumentItem {
                uri: uri(&path),
                language_id: "yaml".to_string(),
                version: 1,
                text: "services:\n  app:\n    image: alpine\n    ports: []\n".to_string(),
            },
        });

        let published = state
            .handle_notification(&notification)
            .expect("handle")
            .expect("publish");
        assert_eq!(published.version, Some(1));
        assert_eq!(published.diagnostics.len(), 1);
    }

    #[test]
    fn did_change_replaces_diagnostics() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let document_uri = uri(&path);
        let mut state = LspState::new(options(tmp.path()));
        state.did_open(
            &document_uri.to_string(),
            "services:\n  app:\n    image: alpine\n    ports: []\n".to_string(),
            Some(1),
        );

        let notification = notification::<DidChangeTextDocument>(DidChangeTextDocumentParams {
            text_document: VersionedTextDocumentIdentifier {
                uri: document_uri,
                version: 2,
            },
            content_changes: vec![TextDocumentContentChangeEvent {
                range: None,
                range_length: None,
                text: VALID_SPEC.to_string(),
            }],
        });
        let published = state
            .handle_notification(&notification)
            .expect("handle")
            .expect("publish");

        assert_eq!(published.version, Some(2));
        assert_eq!(published.diagnostics, Vec::new());
    }

    #[test]
    fn did_close_clears_diagnostics() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("compose.yaml");
        let document_uri = uri(&path);
        let mut state = LspState::new(options(tmp.path()));
        state.did_open(&document_uri.to_string(), VALID_SPEC.to_string(), Some(1));

        let notification = notification::<DidCloseTextDocument>(DidCloseTextDocumentParams {
            text_document: TextDocumentIdentifier { uri: document_uri },
        });
        let published = state
            .handle_notification(&notification)
            .expect("handle")
            .expect("publish");

        assert_eq!(published.version, None);
        assert_eq!(published.diagnostics, Vec::new());
    }

    #[test]
    fn shutdown_request_is_identified_by_lsp_server() {
        let request = Request::new(
            lsp_server::RequestId::from(1),
            "shutdown".to_string(),
            serde_json::Value::Null,
        );
        assert_eq!(request.method, "shutdown");
    }
}
