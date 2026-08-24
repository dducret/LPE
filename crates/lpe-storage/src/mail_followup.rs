use anyhow::{bail, Result};

use crate::JmapEmailFollowupUpdate;

pub(crate) fn validate_followup_update(update: &JmapEmailFollowupUpdate) -> Result<()> {
    if let Some(status) = update.followup_flag_status.as_deref() {
        if !matches!(status, "none" | "flagged" | "complete") {
            bail!("invalid follow-up flag status");
        }
    }
    // [MS-OXOFLAG] section 2.2.1.2 defines follow-up icon colors 1..=6;
    // zero is LPE's cleared canonical projection.
    if update
        .followup_icon
        .is_some_and(|value| !(0..=6).contains(&value))
        || update.todo_item_flags.is_some_and(|value| value < 0)
    {
        bail!("invalid follow-up flag value");
    }
    Ok(())
}

pub(crate) fn normalize_mail_categories(categories: Vec<String>) -> Vec<String> {
    let mut categories = categories
        .into_iter()
        .map(|category| category.trim().to_string())
        .filter(|category| !category.is_empty())
        .collect::<Vec<_>>();
    categories.sort();
    categories.dedup();
    categories
}
