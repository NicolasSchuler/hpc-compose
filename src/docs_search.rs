//! Pure search over the mdBook documentation embedded at build time.
//!
//! The runtime path is intentionally static: search reads only the generated
//! in-binary index and never consults settings, the filesystem, the network, or
//! Slurm. `build.rs` owns the mdBook ingestion step.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DOCS_SEARCH_SCHEMA_VERSION: u32 = 1;
const DEFAULT_LIMIT: usize = 8;
const SNIPPET_MAX_BYTES: usize = 220;
const SNIPPET_CONTEXT_BYTES: usize = 90;

#[derive(Debug, Clone, Copy)]
struct EmbeddedDoc {
    path: &'static str,
    title: &'static str,
    sections: &'static [EmbeddedSection],
}

#[derive(Debug, Clone, Copy)]
struct EmbeddedSection {
    heading: &'static str,
    body: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/docs_search_index.rs"));

/// Search response consumed by text and JSON CLI renderers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DocsSearchOutput {
    /// Additive schema version for machine-readable output.
    pub schema_version: u32,
    /// Query string after CLI-level argument joining.
    pub query: String,
    /// Ranked matches from the embedded documentation.
    pub matches: Vec<DocsSearchHit>,
}

/// One ranked documentation match.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DocsSearchHit {
    /// Stable docs location, relative to the mdBook root.
    pub location: String,
    /// Page title.
    pub title: String,
    /// Best matching section heading, when it is more specific than the page title.
    pub heading: Option<String>,
    /// Compact query-centered excerpt.
    pub snippet: String,
    /// Internal relevance score, useful for debugging ranking changes.
    #[serde(skip)]
    score: u32,
}

impl DocsSearchHit {
    /// Returns a stable human-readable docs location.
    pub fn location(&self) -> String {
        self.location.clone()
    }
}

/// Searches the embedded docs index and returns ranked matches.
///
/// `limit == 0` returns no matches. Empty or whitespace-only queries also return
/// no matches. The function is pure with respect to runtime state: it only reads
/// compile-time embedded strings.
pub fn search_docs(query: &str, limit: usize) -> DocsSearchOutput {
    let parsed = ParsedQuery::new(query);
    let effective_limit = limit;
    let matches = if parsed.is_empty() || effective_limit == 0 {
        Vec::new()
    } else {
        ranked_matches(&parsed, effective_limit)
    };

    DocsSearchOutput {
        schema_version: DOCS_SEARCH_SCHEMA_VERSION,
        query: query.trim().to_string(),
        matches,
    }
}

/// Searches with the default CLI result limit.
pub fn search_docs_default(query: &str) -> DocsSearchOutput {
    search_docs(query, DEFAULT_LIMIT)
}

#[derive(Debug, Clone)]
struct ParsedQuery {
    phrase: String,
    tokens: Vec<String>,
}

impl ParsedQuery {
    fn new(query: &str) -> Self {
        let phrase = normalize_for_match(query);
        let mut tokens = Vec::new();
        for token in tokenize(query) {
            if !tokens.contains(&token) {
                tokens.push(token);
            }
        }
        Self { phrase, tokens }
    }

    fn is_empty(&self) -> bool {
        self.phrase.is_empty() && self.tokens.is_empty()
    }
}

#[derive(Debug)]
struct Candidate<'a> {
    doc: &'a EmbeddedDoc,
    section: &'a EmbeddedSection,
    doc_index: usize,
    section_index: usize,
    score: u32,
}

fn ranked_matches(query: &ParsedQuery, limit: usize) -> Vec<DocsSearchHit> {
    let mut candidates = Vec::new();
    for (doc_index, doc) in EMBEDDED_DOCS.iter().enumerate() {
        let mut best: Option<Candidate<'_>> = None;
        for (section_index, section) in doc.sections.iter().enumerate() {
            let score = score_section(query, doc, section);
            if score == 0 {
                continue;
            }
            let candidate = Candidate {
                doc,
                section,
                doc_index,
                section_index,
                score,
            };
            let replace = best.as_ref().is_none_or(|current| {
                candidate.score > current.score
                    || (candidate.score == current.score
                        && candidate.section_index < current.section_index)
            });
            if replace {
                best = Some(candidate);
            }
        }
        if let Some(best) = best {
            candidates.push(best);
        }
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.doc_index.cmp(&right.doc_index))
            .then_with(|| left.section_index.cmp(&right.section_index))
    });

    candidates
        .into_iter()
        .take(limit)
        .map(|candidate| candidate.into_hit(query))
        .collect()
}

impl Candidate<'_> {
    fn into_hit(self, query: &ParsedQuery) -> DocsSearchHit {
        let heading = best_heading(self.doc, self.section);
        let location = match &heading {
            Some(heading) => format!("{}#{}", self.doc.path, anchor_for_heading(heading)),
            None => self.doc.path.to_string(),
        };
        DocsSearchHit {
            location,
            title: self.doc.title.to_string(),
            heading,
            snippet: snippet_for(self.doc, self.section, query),
            score: self.score,
        }
    }
}

fn best_heading(doc: &EmbeddedDoc, section: &EmbeddedSection) -> Option<String> {
    let heading = section.heading.trim();
    if heading.is_empty() || heading == doc.title {
        None
    } else {
        Some(heading.to_string())
    }
}

fn score_section(query: &ParsedQuery, doc: &EmbeddedDoc, section: &EmbeddedSection) -> u32 {
    let title = normalize_for_match(doc.title);
    let heading = normalize_for_match(section.heading);
    let body = normalize_for_match(section.body);

    let mut score = 0;
    if !query.phrase.is_empty() {
        score += count_occurrences(&title, &query.phrase) * 4_000;
        score += count_occurrences(&heading, &query.phrase) * 2_500;
        score += count_occurrences(&body, &query.phrase) * 1_200;
    }

    for token in &query.tokens {
        score += count_occurrences(&title, token) * 160;
        score += count_occurrences(&heading, token) * 80;
        score += count_occurrences(&body, token) * 8;

        if !is_technical_token(token) && token.chars().count() >= 4 {
            score += fuzzy_token_score(token, &tokenize(doc.title)) * 40;
            score += fuzzy_token_score(token, &tokenize(section.heading)) * 24;
            score += fuzzy_token_score(token, &tokenize(section.body)) * 3;
        }
    }

    if score > 0
        && query
            .tokens
            .iter()
            .all(|token| title.contains(token) || heading.contains(token) || body.contains(token))
    {
        score += 100;
    }

    score
}

fn tokenize(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter_map(|part| {
            let token = part
                .trim_matches(|ch: char| !is_query_token_char(ch))
                .to_ascii_lowercase();
            if token.is_empty() { None } else { Some(token) }
        })
        .collect()
}

fn is_query_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric()
        || matches!(
            ch,
            '_' | '-' | '.' | '/' | ':' | '=' | '+' | '@' | '#' | '~'
        )
}

fn is_technical_token(token: &str) -> bool {
    token.starts_with("--")
        || token.chars().any(|ch| {
            matches!(
                ch,
                '_' | '-' | '.' | '/' | ':' | '=' | '+' | '@' | '#' | '~'
            )
        })
}

fn fuzzy_token_score(needle: &str, haystack_tokens: &[String]) -> u32 {
    if haystack_tokens.iter().any(|token| token == needle) {
        return 0;
    }
    let max_distance = if needle.chars().count() >= 8 { 2 } else { 1 };
    haystack_tokens
        .iter()
        .filter(|candidate| {
            !is_technical_token(candidate) && edit_distance_at_most(needle, candidate, max_distance)
        })
        .count()
        .min(3) as u32
}

fn edit_distance_at_most(left: &str, right: &str, max_distance: usize) -> bool {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    if left.len().abs_diff(right.len()) > max_distance {
        return false;
    }

    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];

    for (left_index, left_ch) in left.iter().enumerate() {
        current[0] = left_index + 1;
        let mut row_min = current[0];
        for (right_index, right_ch) in right.iter().enumerate() {
            let substitution = previous[right_index] + usize::from(left_ch != right_ch);
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let value = substitution.min(insertion).min(deletion);
            current[right_index + 1] = value;
            row_min = row_min.min(value);
        }
        if row_min > max_distance {
            return false;
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right.len()] <= max_distance
}

fn normalize_for_match(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut previous_was_space = false;
    for ch in input.chars() {
        if ch.is_whitespace() {
            if !previous_was_space {
                output.push(' ');
                previous_was_space = true;
            }
        } else {
            output.push(ch.to_ascii_lowercase());
            previous_was_space = false;
        }
    }
    output.trim().to_string()
}

fn count_occurrences(haystack: &str, needle: &str) -> u32 {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut remaining = haystack;
    while let Some(index) = remaining.find(needle) {
        count += 1;
        remaining = &remaining[index + needle.len()..];
    }
    count
}

fn snippet_for(doc: &EmbeddedDoc, section: &EmbeddedSection, query: &ParsedQuery) -> String {
    let source = format!("{} {}", section.heading, section.body);
    let compact = compact_whitespace(if source.trim().is_empty() {
        doc.title
    } else {
        &source
    });
    if compact.is_empty() {
        return doc.title.to_string();
    }

    let lower = compact.to_ascii_lowercase();
    let match_index = find_snippet_match(&lower, query).unwrap_or(0);
    excerpt(&compact, match_index)
}

fn find_snippet_match(haystack: &str, query: &ParsedQuery) -> Option<usize> {
    if !query.phrase.is_empty()
        && let Some(index) = haystack.find(&query.phrase)
    {
        return Some(index);
    }

    query
        .tokens
        .iter()
        .filter_map(|token| haystack.find(token))
        .min()
}

fn compact_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn excerpt(text: &str, match_index: usize) -> String {
    if text.len() <= SNIPPET_MAX_BYTES {
        return text.to_string();
    }

    let mut start = match_index.saturating_sub(SNIPPET_CONTEXT_BYTES);
    start = floor_char_boundary(text, start);
    if start > 0
        && let Some(offset) = text[start..].find(' ')
    {
        start = floor_char_boundary(text, start + offset + 1);
    }

    let mut end = (start + SNIPPET_MAX_BYTES).min(text.len());
    end = floor_char_boundary(text, end);
    if end < text.len()
        && let Some(offset) = text[..end].rfind(' ')
    {
        end = floor_char_boundary(text, offset);
    }

    let mut snippet = String::new();
    if start > 0 {
        snippet.push_str("...");
    }
    snippet.push_str(text[start..end].trim());
    if end < text.len() {
        snippet.push_str("...");
    }
    snippet
}

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn anchor_for_heading(heading: &str) -> String {
    let mut anchor = String::new();
    let mut previous_dash = false;
    for ch in heading.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            anchor.push(ch);
            previous_dash = false;
        } else if (ch.is_whitespace() || matches!(ch, '-' | '_' | '/' | ':'))
            && !anchor.is_empty()
            && !previous_dash
        {
            anchor.push('-');
            previous_dash = true;
        }
    }
    while anchor.ends_with('-') {
        anchor.pop();
    }
    anchor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_cache_dir_technical_token() {
        let report = search_docs("x-slurm.cache_dir", 5);

        assert!(!report.matches.is_empty());
        assert!(report.matches.iter().any(|hit| {
            hit.snippet.contains("x-slurm.cache_dir")
                || hit.location().contains("x-slurm.cache_dir")
        }));
    }

    #[test]
    fn matches_phrase_like_readiness_query() {
        let report = search_docs("readiness never passes", 3);

        let first = report.matches.first().expect("match");
        assert!(first.location().starts_with("troubleshooting.md"));
        assert!(
            first.snippet.contains("Readiness never passes"),
            "snippet was: {}",
            first.snippet
        );
    }

    #[test]
    fn snippet_extracts_query_context() {
        let report = search_docs("--offline", 1);

        let first = report.matches.first().expect("match");
        assert!(
            first.snippet.contains("--offline"),
            "snippet was: {}",
            first.snippet
        );
        assert!(first.snippet.len() <= SNIPPET_MAX_BYTES + 6);
    }

    #[test]
    fn fuzzy_fallback_handles_plain_word_typos() {
        let report = search_docs("readines", 3);

        assert!(
            report
                .matches
                .iter()
                .any(|hit| hit.location().starts_with("troubleshooting.md")
                    || hit.snippet.contains("readiness")),
            "matches were: {:#?}",
            report.matches
        );
    }

    #[test]
    fn limit_behavior_caps_hits() {
        assert!(search_docs("readiness", 0).matches.is_empty());

        let limited = search_docs("readiness", 2);
        assert_eq!(limited.matches.len(), 2);

        let broader = search_docs("readiness", 20);
        assert!(broader.matches.len() >= limited.matches.len());
    }
}
