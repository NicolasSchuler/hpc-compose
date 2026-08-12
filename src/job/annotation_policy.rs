//! Pure validation and normalization policy for tracked-record annotations.

use std::collections::BTreeSet;

use anyhow::{Result, bail};

pub(super) const MAX_TAGS_PER_RECORD: usize = 32;
pub(super) const MAX_TAG_LEN: usize = 64;
pub(super) const MAX_NOTE_LEN: usize = 4096;

pub(super) fn validate_tag(tag: &str) -> Result<()> {
    if tag.is_empty() {
        bail!("tag must not be empty");
    }
    if tag.chars().count() > MAX_TAG_LEN {
        bail!("tag '{tag}' is longer than the maximum of {MAX_TAG_LEN} characters");
    }
    if !tag
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        bail!(
            "tag '{tag}' contains unsupported characters; use only letters, digits, '.', '_', and '-'"
        );
    }
    Ok(())
}

pub(super) fn apply_tag_changes(
    existing: &mut Vec<String>,
    add: &[String],
    remove: &[String],
) -> Result<()> {
    for tag in add.iter().chain(remove.iter()) {
        validate_tag(tag)?;
    }
    let mut set: BTreeSet<String> = existing.iter().cloned().collect();
    for tag in add {
        set.insert(tag.clone());
    }
    for tag in remove {
        set.remove(tag.as_str());
    }
    if set.len() > MAX_TAGS_PER_RECORD {
        bail!(
            "a tracked record can carry at most {MAX_TAGS_PER_RECORD} tags ({} after this change); remove tags with 'experiment tag --remove <TAG>' first",
            set.len()
        );
    }
    *existing = set.into_iter().collect();
    Ok(())
}

pub(super) fn validate_note_text(text: &str) -> Result<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        bail!("note text must not be empty");
    }
    if trimmed.chars().count() > MAX_NOTE_LEN {
        bail!("note text is longer than the maximum of {MAX_NOTE_LEN} characters");
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn tag_validation_preserves_exact_boundaries_and_errors() {
        assert_eq!(MAX_TAG_LEN, 64);
        validate_tag("lr-bug_v1.2").expect("allowed tag");
        validate_tag(&"a".repeat(MAX_TAG_LEN)).expect("maximum-length tag");
        assert_eq!(
            validate_tag("").expect_err("empty tag").to_string(),
            "tag must not be empty"
        );
        assert_eq!(
            validate_tag("café").expect_err("non-ASCII tag").to_string(),
            "tag 'café' contains unsupported characters; use only letters, digits, '.', '_', and '-'"
        );
        let too_long = "a".repeat(MAX_TAG_LEN + 1);
        assert_eq!(
            validate_tag(&too_long)
                .expect_err("overlong tag")
                .to_string(),
            format!("tag '{too_long}' is longer than the maximum of {MAX_TAG_LEN} characters")
        );
    }

    #[test]
    fn tag_changes_preserve_sorted_atomic_add_then_remove_semantics() {
        assert_eq!(MAX_TAGS_PER_RECORD, 32);
        let mut tags = strings(&["zeta", "baseline", "baseline"]);
        apply_tag_changes(&mut tags, &strings(&["alpha", "zeta"]), &strings(&["zeta"]))
            .expect("valid changes");
        assert_eq!(tags, strings(&["alpha", "baseline"]));

        let before_invalid = tags.clone();
        assert_eq!(
            apply_tag_changes(&mut tags, &strings(&["valid"]), &strings(&["bad tag"]))
                .expect_err("invalid removal")
                .to_string(),
            "tag 'bad tag' contains unsupported characters; use only letters, digits, '.', '_', and '-'"
        );
        assert_eq!(tags, before_invalid, "failed validation must be atomic");

        let mut full = (0..MAX_TAGS_PER_RECORD)
            .map(|index| format!("tag{index:03}"))
            .collect::<Vec<_>>();
        let before_overflow = full.clone();
        assert_eq!(
            apply_tag_changes(&mut full, &strings(&["one-more"]), &[])
                .expect_err("tag limit")
                .to_string(),
            "a tracked record can carry at most 32 tags (33 after this change); remove tags with 'experiment tag --remove <TAG>' first"
        );
        assert_eq!(full, before_overflow, "overflow must not mutate tags");

        let mut legacy = strings(&["legacy tag"]);
        apply_tag_changes(&mut legacy, &[], &[]).expect("existing tags are not revalidated");
        assert_eq!(legacy, strings(&["legacy tag"]));
    }

    #[test]
    fn note_validation_preserves_unicode_character_limits_and_exact_errors() {
        assert_eq!(MAX_NOTE_LEN, 4096);
        assert_eq!(
            validate_note_text(" caf\u{732b} ").expect("trimmed Unicode note"),
            "caf\u{732b}"
        );
        assert_eq!(
            validate_note_text(" \n\t")
                .expect_err("empty note")
                .to_string(),
            "note text must not be empty"
        );
        let maximum = "\u{732b}".repeat(MAX_NOTE_LEN);
        assert_eq!(validate_note_text(&maximum).expect("maximum note"), maximum);
        assert_eq!(
            validate_note_text(&"\u{732b}".repeat(MAX_NOTE_LEN + 1))
                .expect_err("overlong note")
                .to_string(),
            "note text is longer than the maximum of 4096 characters"
        );
    }
}
