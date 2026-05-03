use anyhow::{anyhow, bail, Result};
use sha2::{Digest, Sha256};
use std::env;
use uuid::Uuid;

pub(crate) fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

pub(crate) fn normalize_admin_session_auth_method(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "oidc" => "oidc",
        // The persisted admin session tracks the broad login family so
        // password+totp continues to work against the 0.1.3 schema.
        _ => "password",
    }
}

pub(crate) fn normalize_subject(value: &str) -> String {
    value.trim().to_string()
}

pub(crate) fn normalize_task_status(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "needs-action" => Ok("needs-action"),
        "in-progress" => Ok("in-progress"),
        "completed" => Ok("completed"),
        "cancelled" => Ok("cancelled"),
        other => bail!("unsupported task status: {other}"),
    }
}

pub(crate) fn normalize_task_list_name(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("task list name is required");
    }
    Ok(trimmed.to_string())
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn domain_from_email(email: &str) -> Result<String> {
    email
        .split_once('@')
        .map(|(_, domain)| domain.trim().to_lowercase())
        .filter(|domain| !domain.is_empty())
        .ok_or_else(|| anyhow!("account email does not contain a domain"))
}

pub(crate) fn preview_text(body_text: &str) -> String {
    let preview = body_text
        .split_whitespace()
        .take(28)
        .collect::<Vec<_>>()
        .join(" ");

    if preview.is_empty() {
        "(no preview)".to_string()
    } else {
        preview
    }
}

pub(crate) fn permissions_from_storage(
    role: &str,
    rights_summary: Option<&str>,
    permissions_json: Option<&str>,
) -> Vec<String> {
    let explicit = permissions_json
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    normalize_admin_permissions(role, rights_summary.unwrap_or_default(), &explicit)
}

pub(crate) fn system_mailbox_aliases(role: &str, display_name: &str) -> Vec<String> {
    let mut aliases = match role {
        "inbox" => vec!["inbox".to_string()],
        "drafts" => vec!["draft".to_string(), "drafts".to_string()],
        "sent" => vec![
            "sent".to_string(),
            "sent items".to_string(),
            "sent messages".to_string(),
        ],
        "trash" => vec![
            "deleted".to_string(),
            "deleted items".to_string(),
            "trash".to_string(),
        ],
        _ => Vec::new(),
    };
    let normalized_display_name = display_name.trim().to_lowercase();
    if !normalized_display_name.is_empty()
        && !aliases
            .iter()
            .any(|alias| alias == &normalized_display_name)
    {
        aliases.push(normalized_display_name);
    }
    aliases
}

pub(crate) fn system_mailbox_role_for_display_name(display_name: &str) -> Option<&'static str> {
    match display_name.trim().to_ascii_lowercase().as_str() {
        "inbox" => Some("inbox"),
        "draft" | "drafts" => Some("drafts"),
        "sent" | "sent items" | "sent messages" => Some("sent"),
        "deleted" | "deleted items" | "trash" => Some("trash"),
        _ => None,
    }
}

pub(crate) fn canonical_system_mailbox_display_name(role: &str) -> Option<&'static str> {
    match role {
        "inbox" => Some("Inbox"),
        "drafts" => Some("Drafts"),
        "sent" => Some("Sent"),
        "trash" => Some("Deleted"),
        _ => None,
    }
}

pub(crate) fn normalize_admin_permissions(
    role: &str,
    rights_summary: &str,
    explicit: &[String],
) -> Vec<String> {
    let mut permissions = default_permissions_for_role(role);
    permissions.extend(split_permissions(rights_summary));
    permissions.extend(
        explicit
            .iter()
            .map(|permission| permission.trim().to_lowercase())
            .filter(|permission| !permission.is_empty()),
    );
    if !permissions.is_empty() {
        permissions.push("dashboard".to_string());
    }
    permissions.sort();
    permissions.dedup();
    permissions
}

pub(crate) fn permission_summary(permissions: &[String]) -> String {
    permissions.join(", ")
}

pub(crate) fn default_permissions_for_role(role: &str) -> Vec<String> {
    match role.trim().to_lowercase().as_str() {
        "server-admin" | "super-admin" => vec!["*".to_string()],
        "tenant-admin" => vec![
            "dashboard",
            "domains",
            "accounts",
            "aliases",
            "admins",
            "policies",
            "security",
            "ai",
            "antispam",
            "pst",
            "audit",
            "mail",
            "operations",
            "protocols",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect(),
        "domain-admin" => vec![
            "dashboard",
            "domains",
            "accounts",
            "aliases",
            "admins",
            "mail",
            "pst",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect(),
        "compliance-admin" => vec!["dashboard", "audit", "policies"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "helpdesk" | "support" => vec!["dashboard", "accounts", "mail"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "transport-operator" => vec!["dashboard", "antispam", "operations", "protocols"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn parse_activesync_file_reference(value: &str) -> Option<(Uuid, Uuid)> {
    let mut parts = value.trim().split(':');
    if parts.next()? != "attachment" {
        return None;
    }
    let message_id = Uuid::parse_str(parts.next()?).ok()?;
    let attachment_id = Uuid::parse_str(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((message_id, attachment_id))
}

pub(crate) fn trim_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub(crate) fn normalize_gal_visibility(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "tenant" | "" => Ok("tenant".to_string()),
        "hidden" => Ok("hidden".to_string()),
        other => bail!("unsupported GAL visibility: {other}"),
    }
}

pub(crate) fn normalize_directory_kind(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "person" | "" => Ok("person".to_string()),
        "room" => Ok("room".to_string()),
        "equipment" => Ok("equipment".to_string()),
        other => bail!("unsupported directory kind: {other}"),
    }
}

pub(crate) fn validate_sieve_script_name(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("sieve script name is required");
    }
    if trimmed.len() > 128 {
        bail!("sieve script name is too long");
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains('\0') {
        bail!("sieve script name contains unsupported characters");
    }
    Ok(trimmed.to_string())
}

pub(crate) fn validate_sieve_script_content(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("sieve script content is required");
    }
    if trimmed.len() > crate::MAX_SIEVE_SCRIPT_BYTES {
        bail!("sieve script exceeds the MVP size limit");
    }
    Ok(trimmed.to_string())
}

pub(crate) fn env_hostname(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().trim_matches('_').to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn env_bind_address(name: &str, fallback: &str) -> String {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn split_permissions(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|entry| entry.trim().to_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}
