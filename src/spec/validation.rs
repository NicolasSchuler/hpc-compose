use anyhow::{Result, bail};

pub(super) fn validate_positive_u32(value: Option<u32>, field: &str) -> Result<()> {
    if matches!(value, Some(0)) {
        bail!("{field} must be at least 1");
    }
    Ok(())
}

pub(super) fn validate_sbatch_safe_string(value: Option<&str>, field: &str) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    if value.contains('\n') || value.contains('\r') {
        bail!("{field} must not contain line breaks");
    }
    if value.contains('\0') {
        bail!("{field} must not contain null bytes");
    }
    Ok(())
}

pub(super) fn validate_sbatch_safe_strings<'a>(
    values: impl IntoIterator<Item = &'a str>,
    field: &str,
) -> Result<()> {
    for (index, value) in values.into_iter().enumerate() {
        validate_sbatch_safe_string(Some(value), &format!("{field}[{index}]"))?;
    }
    Ok(())
}
