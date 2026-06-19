//! Surgical, comment-preserving rewrites for `lint --fix`.
//!
//! The editor works line-by-line so that every byte outside the rewritten
//! `depends_on` block is preserved verbatim, including comments, blank lines,
//! and author formatting. A safety gate re-parses the result before any file
//! is written, so a botched rewrite never reaches disk.
//!
//! Only `depends_on` edges are auto-fixable today. Path-related findings are
//! advisory (see `src/lint.rs`).

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::lint::SuggestedFix;
use crate::spec::{DependencyCondition, DependsOnSpec};

/// A fix that was successfully applied to the source text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedFix {
    /// Lint rule code that produced the fix.
    pub code: &'static str,
    /// Service owning the rewritten field.
    pub service: String,
    /// Dotted spec field that was rewritten.
    pub field: String,
    /// One-line human description of the change.
    pub description: String,
}

/// Applies every fixable [`SuggestedFix`] to *text* sequentially, returning the
/// rewritten text and a description of each applied fix.
///
/// Each individual rewrite is idempotent, so overlapping fixes (for example
/// multiple implicit edges inside one list-form `depends_on`) collapse
/// cleanly. This function only edits the in-memory text; the caller is
/// responsible for writing the result and running the post-write safety gate.
///
/// # Errors
///
/// Returns an error if any individual fix cannot locate its target or refuses
/// to produce a valid rewrite.
pub fn apply_fixes(text: &str, fixes: &[SuggestedFix]) -> Result<(String, Vec<AppliedFix>)> {
    let mut current = text.to_string();
    let mut applied = Vec::new();
    for fix in fixes {
        let SuggestedFix::DependsOnCondition {
            service,
            dependency,
            condition,
        } = fix;
        match rewrite_depends_on_condition(&current, service, dependency, condition)? {
            Some(outcome) => {
                current = outcome.new_text;
                applied.push(AppliedFix {
                    code: "HPC006",
                    service: service.clone(),
                    field: format!("services.{service}.depends_on.{dependency}"),
                    description: format!(
                        "made depends_on condition explicit: {dependency} -> {condition}"
                    ),
                });
            }
            None => { /* already explicit; idempotent no-op */ }
        }
    }
    Ok((current, applied))
}

#[derive(Debug)]
struct RewriteOutcome {
    new_text: String,
}

/// Locates `services.<service>.depends_on` and makes the `<dependency>` edge's
/// condition explicit. Returns `Ok(None)` when the edge is already explicit
/// (idempotent).
///
/// # Errors
///
/// Returns an error if the `depends_on` block cannot be located or has a shape
/// the editor does not understand.
fn rewrite_depends_on_condition(
    text: &str,
    service: &str,
    dependency: &str,
    condition: &str,
) -> Result<Option<RewriteOutcome>> {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    if lines.is_empty() {
        bail!("compose source is empty");
    }
    let block = locate_depends_on(&lines, service)?;

    let inline = inline_value_after_key(lines[block.depends_on_idx], "depends_on");
    if let Some(value) = inline {
        return rewrite_inline(&lines, &block, &value, dependency, condition);
    }

    rewrite_block(&lines, &block, dependency, condition)
}

/// Located positions for a service's `depends_on:` block.
struct DependsOnBlock {
    depends_on_idx: usize,
    depends_on_indent: usize,
}

fn locate_depends_on(lines: &[&str], service: &str) -> Result<DependsOnBlock> {
    let services_idx = find_top_key(lines, &["services", "steps"])
        .context("could not find a top-level 'services' (or 'steps') mapping")?;
    let services_indent = indent_of(lines[services_idx]);

    let service_idx = find_child_mapping_entry(lines, services_idx, services_indent, service)
        .with_context(|| format!("could not locate service '{service}'"))?;
    let service_indent = indent_of(lines[service_idx]);

    let depends_on_idx = find_child_key_entry(lines, service_idx, service_indent, "depends_on")
        .with_context(|| format!("service '{service}' has no depends_on block to rewrite"))?;
    let depends_on_indent = indent_of(lines[depends_on_idx]);

    Ok(DependsOnBlock {
        depends_on_idx,
        depends_on_indent,
    })
}

/// Returns the value text following `key:` on the same line, when present.
///
/// For `depends_on: [a, b]` this returns `"[a, b]"`; for `depends_on:` (empty)
/// it returns `None`.
fn inline_value_after_key(line: &str, key: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let prefix = format!("{key}:");
    let after = trimmed.strip_prefix(&prefix)?;
    let rest = after.trim_start();
    // Strip a trailing comment so `depends_on: [a] # note` -> "[a]".
    let rest = strip_trailing_comment(rest);
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

fn strip_trailing_comment(value: &str) -> &str {
    // Naive but conservative: a `#` preceded by whitespace starts a comment.
    // Flow collections rarely contain ` # ` inside a single line, so this is
    // safe for the inline forms we emit/parse here.
    let bytes = value.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut prev = b' ';
    for (idx, &byte) in bytes.iter().enumerate() {
        match byte {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'#' if !in_single && !in_double && (prev == b' ' || prev == b'\t') => {
                return value[..idx].trim_end();
            }
            _ => {}
        }
        prev = byte;
    }
    value.trim_end()
}

fn rewrite_inline(
    lines: &[&str],
    block: &DependsOnBlock,
    value: &str,
    dependency: &str,
    condition: &str,
) -> Result<Option<RewriteOutcome>> {
    let parsed = parse_depends_on_value(value)?;
    if edge_already_explicit(&parsed, dependency, condition) {
        return Ok(None);
    }
    let new_value = rebuild_as_explicit_mapping(&parsed, dependency, condition);
    let new_text = emit_inline_replacement(lines, block, &new_value)?;
    Ok(Some(RewriteOutcome { new_text }))
}

fn rewrite_block(
    lines: &[&str],
    block: &DependsOnBlock,
    dependency: &str,
    condition: &str,
) -> Result<Option<RewriteOutcome>> {
    let parent_indent = block.depends_on_indent;
    let child_indent = match first_child_indent(lines, block.depends_on_idx, parent_indent) {
        Some(indent) => indent,
        None => bail!("depends_on block has no indented children"),
    };

    let block_end = block_end_line(lines, block.depends_on_idx, parent_indent);
    let child_lines = &lines[block.depends_on_idx + 1..block_end];

    if child_lines
        .iter()
        .any(|line| line.trim_start().starts_with("- "))
    {
        // Sequence (list) form: must convert the whole block to mapping form.
        let parsed = parse_depends_on_children(child_lines)?;
        if edge_already_explicit(&parsed, dependency, condition) {
            return Ok(None);
        }
        let rebuilt = render_mapping_block(&parsed, dependency, condition, child_indent);
        let new_text = splice(lines, block.depends_on_idx + 1, block_end, &rebuilt)?;
        return Ok(Some(RewriteOutcome { new_text }));
    }

    // Mapping form: insert a condition under the target entry if missing.
    rewrite_block_mapping(lines, block, child_indent, block_end, dependency, condition)
}

fn rewrite_block_mapping(
    lines: &[&str],
    block: &DependsOnBlock,
    child_indent: usize,
    block_end: usize,
    dependency: &str,
    condition: &str,
) -> Result<Option<RewriteOutcome>> {
    let entry_idx = (block.depends_on_idx + 1..block_end)
        .find(|&idx| is_mapping_entry(lines[idx], child_indent, dependency))
        .with_context(|| format!("could not find depends_on entry '{dependency}' to rewrite"))?;

    // Is there already a `condition:` child directly under this entry?
    let entry_indent = indent_of(lines[entry_idx]);
    let entry_block_end = block_end_line(lines, entry_idx, entry_indent);
    for &line in &lines[entry_idx + 1..entry_block_end] {
        if is_blank_or_comment(line) {
            continue;
        }
        if indent_of(line) <= entry_indent {
            break;
        }
        if line.trim_start().starts_with("condition:") {
            return Ok(None); // already explicit
        }
    }

    // Determine whether the entry line has an inline value (e.g. `redis: {}`
    // or `redis: {condition: x}`). If so, regenerate via parse to keep things
    // valid; otherwise insert a `condition:` child line.
    let entry_line = lines[entry_idx];
    let inline = inline_value_after_key(entry_line, dependency);
    if let Some(value) = inline {
        let parsed = parse_depends_on_value(&format!("{{{value}}}"))
            .or_else(|_| parse_depends_on_value(&value))
            .context("failed to parse inline depends_on entry")?;
        if edge_already_explicit(&parsed, dependency, condition) {
            return Ok(None);
        }
        let new_entry = render_single_entry(dependency, condition, entry_indent);
        let new_text = splice(lines, entry_idx, entry_idx + 1, &new_entry)?;
        return Ok(Some(RewriteOutcome { new_text }));
    }

    let indent_str = " ".repeat(entry_indent + 2);
    let new_child_line = format!("{indent_str}condition: {condition}\n");
    let new_text = insert_lines(lines, entry_idx + 1, &new_child_line);
    Ok(Some(RewriteOutcome { new_text }))
}

/// Parses a standalone depends_on value (inline or reconstructed children).
fn parse_depends_on_value(value: &str) -> Result<DependsOnSpec> {
    let wrapped = format!("depends_on: {value}\n");
    let doc: serde_norway::Value = serde_norway::from_str(&wrapped)
        .with_context(|| format!("failed to parse depends_on value '{value}'"))?;
    let mapping_value = doc
        .get("depends_on")
        .cloned()
        .unwrap_or(serde_norway::Value::Null);
    let spec: DependsOnSpec = serde_norway::from_value(mapping_value)
        .context("failed to deserialize depends_on value")?;
    Ok(spec)
}

fn parse_depends_on_children(child_lines: &[&str]) -> Result<DependsOnSpec> {
    let mut joined = String::from("depends_on:\n");
    for line in child_lines {
        joined.push_str(line);
    }
    let doc: serde_norway::Value =
        serde_norway::from_str(&joined).context("failed to parse depends_on children")?;
    let value = doc
        .get("depends_on")
        .cloned()
        .unwrap_or(serde_norway::Value::Null);
    let spec: DependsOnSpec =
        serde_norway::from_value(value).context("failed to deserialize depends_on children")?;
    Ok(spec)
}

/// Returns true when *dependency* already carries *condition* explicitly.
fn edge_already_explicit(parsed: &DependsOnSpec, dependency: &str, condition: &str) -> bool {
    let Ok(entries) = parsed.entries() else {
        return false;
    };
    entries.iter().any(|edge| {
        edge.name == dependency && !edge.implicit && {
            let want = match edge.condition {
                DependencyCondition::ServiceStarted => "service_started",
                DependencyCondition::ServiceHealthy => "service_healthy",
                DependencyCondition::ServiceCompletedSuccessfully => {
                    "service_completed_successfully"
                }
            };
            want == condition
        }
    })
}

/// Renders the whole depends_on mapping in explicit block form. The target
/// edge gets *condition*; other edges keep their existing condition or become
/// explicit `service_started` when they were implicit (unavoidable when
/// converting from list form).
fn rebuild_as_explicit_mapping(
    parsed: &DependsOnSpec,
    target: &str,
    target_condition: &str,
) -> String {
    let entries = parsed.entries().unwrap_or_default();
    let mut out = String::from("{");
    let mut first = true;
    for edge in entries {
        if !first {
            out.push_str(", ");
        }
        first = false;
        let condition = if edge.name == target {
            target_condition.to_string()
        } else {
            condition_label(edge.condition).to_string()
        };
        out.push_str(&format!("{}: {{condition: {condition}}}", edge.name));
    }
    out.push('}');
    out
}

fn render_mapping_block(
    parsed: &DependsOnSpec,
    target: &str,
    target_condition: &str,
    child_indent: usize,
) -> String {
    let entries = parsed.entries().unwrap_or_default();
    let indent = " ".repeat(child_indent);
    let inner = " ".repeat(child_indent + 2);
    let mut out = String::new();
    for edge in entries {
        let condition = if edge.name == target {
            target_condition.to_string()
        } else {
            condition_label(edge.condition).to_string()
        };
        out.push_str(&format!("{indent}{}:\n", edge.name));
        out.push_str(&format!("{inner}condition: {condition}\n"));
    }
    out
}

fn render_single_entry(dependency: &str, condition: &str, entry_indent: usize) -> String {
    let indent = " ".repeat(entry_indent);
    let inner = " ".repeat(entry_indent + 2);
    format!("{indent}{dependency}:\n{inner}condition: {condition}\n")
}

fn condition_label(condition: DependencyCondition) -> &'static str {
    match condition {
        DependencyCondition::ServiceStarted => "service_started",
        DependencyCondition::ServiceHealthy => "service_healthy",
        DependencyCondition::ServiceCompletedSuccessfully => "service_completed_successfully",
    }
}

/// Replace `lines[start..end)` with the given replacement text (which may span
/// multiple lines), preserving the rest of the buffer byte-for-byte.
fn splice(lines: &[&str], start: usize, end: usize, replacement: &str) -> Result<String> {
    let mut out = String::new();
    for line in &lines[..start] {
        out.push_str(line);
    }
    out.push_str(replacement);
    for line in &lines[end..] {
        out.push_str(line);
    }
    Ok(out)
}

fn insert_lines(lines: &[&str], at: usize, new_line: &str) -> String {
    let mut out = String::new();
    for line in &lines[..at] {
        out.push_str(line);
    }
    out.push_str(new_line);
    for line in &lines[at..] {
        out.push_str(line);
    }
    out
}

fn emit_inline_replacement(
    lines: &[&str],
    block: &DependsOnBlock,
    new_value: &str,
) -> Result<String> {
    let indent = " ".repeat(block.depends_on_indent);
    let new_line = format!("{indent}depends_on: {new_value}\n");
    splice(
        lines,
        block.depends_on_idx,
        block.depends_on_idx + 1,
        &new_line,
    )
}

// --- line-scanning helpers ------------------------------------------------

fn indent_of(line: &str) -> usize {
    line.bytes().take_while(|byte| *byte == b' ').count()
}

fn is_blank_or_comment(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.is_empty() || trimmed.starts_with('#')
}

fn find_top_key(lines: &[&str], keys: &[&str]) -> Option<usize> {
    let mut min_indent = usize::MAX;
    let mut found = None;
    for (idx, line) in lines.iter().enumerate() {
        if is_blank_or_comment(line) {
            continue;
        }
        let indent = indent_of(line);
        if indent > min_indent {
            continue;
        }
        let trimmed = line.trim_start();
        let matches_key = keys.iter().any(|key| {
            trimmed == *key
                || trimmed.starts_with(&format!("{key}:"))
                || trimmed.starts_with(&format!("{key} :"))
        });
        if matches_key && indent < min_indent {
            min_indent = indent;
            found = Some(idx);
        }
    }
    found
}

/// Finds a child mapping entry named *key* whose key sits at an indent greater
/// than `parent_indent`, scanning the parent's block.
fn find_child_mapping_entry(
    lines: &[&str],
    parent_idx: usize,
    parent_indent: usize,
    key: &str,
) -> Option<usize> {
    let child_indent = first_child_indent(lines, parent_idx, parent_indent)?;
    for (idx, line) in lines.iter().enumerate() {
        if idx <= parent_idx || is_blank_or_comment(line) {
            continue;
        }
        let indent = indent_of(line);
        if indent <= parent_indent {
            break;
        }
        if indent == child_indent && is_key_line(line, key) {
            return Some(idx);
        }
    }
    None
}

/// Same as [`find_child_mapping_entry`] but returns the index of the key line.
/// (Kept as a separate name for readability at call sites.)
fn find_child_key_entry(
    lines: &[&str],
    parent_idx: usize,
    parent_indent: usize,
    key: &str,
) -> Option<usize> {
    find_child_mapping_entry(lines, parent_idx, parent_indent, key)
}

fn is_key_line(line: &str, key: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed == format!("{key}:")
        || trimmed.starts_with(&format!("{key}:"))
        || trimmed == format!("{key} :")
        || trimmed.starts_with(&format!("{key} :"))
}

fn is_mapping_entry(line: &str, expected_indent: usize, key: &str) -> bool {
    if indent_of(line) != expected_indent {
        return false;
    }
    let trimmed = line.trim_start();
    trimmed == format!("{key}:")
        || trimmed.starts_with(&format!("{key}:"))
        || trimmed.starts_with(&format!("{key} :"))
}

fn first_child_indent(lines: &[&str], parent_idx: usize, parent_indent: usize) -> Option<usize> {
    for (idx, line) in lines.iter().enumerate() {
        if idx <= parent_idx || is_blank_or_comment(line) {
            continue;
        }
        let indent = indent_of(line);
        if indent <= parent_indent {
            return None;
        }
        return Some(indent);
    }
    None
}

/// Returns the exclusive end line index of the block that starts at
/// `start_idx` (whose key indent is `parent_indent`).
fn block_end_line(lines: &[&str], start_idx: usize, parent_indent: usize) -> usize {
    for (idx, line) in lines.iter().enumerate() {
        if idx <= start_idx || is_blank_or_comment(line) {
            continue;
        }
        let indent = indent_of(line);
        if indent <= parent_indent {
            return idx;
        }
    }
    lines.len()
}

// --- unified diff for --dry-run -------------------------------------------

/// Renders a minimal unified diff between *original* and *updated*.
///
/// Uses an LCS-over-lines pass. Unchanged ("equal") lines are emitted
/// verbatim (no leading space) so the surrounding context of each change is
/// readable without the noise of a full unified-diff context window.
pub fn unified_diff(original: &str, updated: &str) -> String {
    let a: Vec<&str> = original.split_inclusive('\n').collect();
    let b: Vec<&str> = updated.split_inclusive('\n').collect();
    let hunks = diff_hunks(&a, &b);
    let mut out = String::new();
    out.push_str("--- lint --fix (proposed)\n+++ lint --fix (proposed)\n");
    for (tag, line) in hunks {
        match tag {
            DiffTag::Equal => out.push_str(line),
            DiffTag::Delete => {
                out.push('-');
                out.push_str(line);
            }
            DiffTag::Insert => {
                out.push('+');
                out.push_str(line);
            }
        }
    }
    out
}

#[derive(Clone, Copy)]
enum DiffTag {
    Equal,
    Delete,
    Insert,
}

fn diff_hunks<'a>(a: &[&'a str], b: &[&'a str]) -> Vec<(DiffTag, &'a str)> {
    let lcs = lcs_table(a, b);
    let mut i = a.len();
    let mut j = b.len();
    let mut out = Vec::new();
    while i > 0 || j > 0 {
        if i > 0 && j > 0 && a[i - 1] == b[j - 1] {
            out.push((DiffTag::Equal, a[i - 1]));
            i -= 1;
            j -= 1;
        } else if j > 0 && (i == 0 || lcs[i][j - 1] >= lcs[i - 1][j]) {
            out.push((DiffTag::Insert, b[j - 1]));
            j -= 1;
        } else {
            out.push((DiffTag::Delete, a[i - 1]));
            i -= 1;
        }
    }
    out.reverse();
    out
}

fn lcs_table(a: &[&str], b: &[&str]) -> Vec<Vec<usize>> {
    let mut dp = vec![vec![0_usize; b.len() + 1]; a.len() + 1];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            dp[i][j] = if a[i - 1] == b[j - 1] {
                dp[i - 1][j - 1] + 1
            } else {
                dp[i - 1][j].max(dp[i][j - 1])
            };
        }
    }
    dp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fix(text: &str, service: &str, dependency: &str, condition: &str) -> String {
        rewrite_depends_on_condition(text, service, dependency, condition)
            .expect("rewrite")
            .expect("changed")
            .new_text
    }

    #[test]
    fn block_list_form_converts_to_explicit_mapping() {
        let input = "services:\n  app:\n    image: redis:7\n    depends_on:\n      - redis\n      - cache\n";
        let out = fix(input, "app", "redis", "service_started");
        assert!(out.contains("redis:\n        condition: service_started"));
        assert!(out.contains("cache:\n        condition: service_started"));
        // The list form is gone.
        assert!(!out.contains("- redis"));
        assert!(!out.contains("- cache"));
    }

    #[test]
    fn inline_list_form_converts_to_inline_mapping() {
        let input = "services:\n  app:\n    image: redis:7\n    depends_on: [redis, cache]\n";
        let out = fix(input, "app", "redis", "service_started");
        assert!(out.contains("depends_on: {redis: {condition: service_started},"));
    }

    #[test]
    fn block_mapping_without_condition_inserts_condition() {
        let input = "services:\n  app:\n    image: redis:7\n    depends_on:\n      redis: {}\n      cache:\n        condition: service_healthy\n";
        let out = fix(input, "app", "redis", "service_started");
        assert!(out.contains("redis:\n        condition: service_started"));
        // cache is left untouched.
        assert!(out.contains("cache:\n        condition: service_healthy"));
    }

    #[test]
    fn already_explicit_edge_is_noop() {
        let input = "services:\n  app:\n    image: redis:7\n    depends_on:\n      redis:\n        condition: service_started\n";
        let outcome =
            rewrite_depends_on_condition(input, "app", "redis", "service_started").expect("ok");
        assert!(outcome.is_none());
    }

    #[test]
    fn comments_outside_block_are_preserved_byte_for_byte() {
        let input = "# top comment\nservices:\n  # leading comment\n  app: # inline svc\n    image: redis:7\n    depends_on:\n      - redis\n  redis:\n    image: redis:7\n# trailing\n";
        let out = fix(input, "app", "redis", "service_started");
        assert!(out.contains("# top comment"));
        assert!(out.contains("# leading comment"));
        assert!(out.contains("# inline svc"));
        assert!(out.contains("# trailing"));
        assert!(out.contains("  redis:\n    image: redis:7"));
    }

    #[test]
    fn preserves_steps_alias() {
        let input = "steps:\n  app:\n    depends_on:\n      - redis\n";
        let out = fix(input, "app", "redis", "service_started");
        assert!(out.contains("redis:\n        condition: service_started"));
    }

    #[test]
    fn missing_service_bails() {
        let input = "services:\n  app:\n    depends_on:\n      - redis\n";
        let err = rewrite_depends_on_condition(input, "missing", "redis", "service_started")
            .expect_err("should fail");
        assert!(
            err.to_string()
                .contains("could not locate service 'missing'")
        );
    }

    #[test]
    fn missing_depends_on_bails() {
        let input = "services:\n  app:\n    image: redis:7\n";
        let err = rewrite_depends_on_condition(input, "app", "redis", "service_started")
            .expect_err("should fail");
        assert!(err.to_string().contains("no depends_on block"));
    }

    #[test]
    fn apply_fixes_dedups_overlapping_edges() {
        let input = "services:\n  app:\n    depends_on:\n      - redis\n      - cache\n";
        let fixes = vec![
            SuggestedFix::DependsOnCondition {
                service: "app".into(),
                dependency: "redis".into(),
                condition: "service_started".into(),
            },
            SuggestedFix::DependsOnCondition {
                service: "app".into(),
                dependency: "cache".into(),
                condition: "service_started".into(),
            },
        ];
        let (text, applied) = apply_fixes(input, &fixes).expect("apply");
        // The list->map conversion makes every edge explicit in a single edit,
        // so the second fix is an idempotent no-op.
        assert!(text.contains("redis:\n        condition: service_started"));
        assert!(text.contains("cache:\n        condition: service_started"));
        assert_eq!(applied.len(), 1);
    }

    #[test]
    fn idempotent_on_second_run() {
        let input = "services:\n  app:\n    depends_on:\n      - redis\n";
        let once = fix(input, "app", "redis", "service_started");
        let outcome =
            rewrite_depends_on_condition(&once, "app", "redis", "service_started").expect("ok");
        assert!(outcome.is_none(), "second run must be a no-op");
    }

    #[test]
    fn strip_trailing_comment_handles_quotes() {
        assert_eq!(strip_trailing_comment("[a, b]  # note"), "[a, b]");
        assert_eq!(strip_trailing_comment("[a, b]"), "[a, b]");
        assert_eq!(strip_trailing_comment("value # not yaml"), "value");
    }

    #[test]
    fn unified_diff_marks_insertions_and_deletions() {
        let diff = unified_diff("a\nb\nc\n", "a\nx\nc\n");
        assert!(diff.starts_with("--- lint --fix (proposed)\n+++ lint --fix (proposed)\n"));
        assert!(diff.contains("-b\n"));
        assert!(diff.contains("+x\n"));
        // Equal lines are emitted verbatim (no leading space).
        assert!(diff.contains("a\n"));
        assert!(diff.contains("c\n"));
    }

    #[test]
    fn unified_diff_no_change_is_empty_of_edits() {
        let diff = unified_diff("a\nb\n", "a\nb\n");
        // The only `-`/`+` characters should be the two header lines.
        let change_lines = diff
            .lines()
            .filter(|line| line.starts_with('-') || line.starts_with('+'))
            .filter(|line| !line.starts_with("---") && !line.starts_with("+++"))
            .count();
        assert_eq!(change_lines, 0, "no change lines expected:\n{diff}");
    }
}
