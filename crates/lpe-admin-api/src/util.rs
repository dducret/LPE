use axum::http::StatusCode;
use lpe_storage::{
    AdminDashboard, AuthenticatedAdmin, CollaborationResourceKind, SenderDelegationRight,
};
use uuid::Uuid;

pub(crate) fn parse_collaboration_kind(value: &str) -> Result<CollaborationResourceKind, String> {
    match value.trim().to_lowercase().as_str() {
        "contacts" | "contact" => Ok(CollaborationResourceKind::Contacts),
        "calendar" | "calendars" => Ok(CollaborationResourceKind::Calendar),
        _ => Err("unsupported collaboration kind".to_string()),
    }
}

pub(crate) fn parse_sender_delegation_right(value: &str) -> Result<SenderDelegationRight, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "send_as" | "send-as" => Ok(SenderDelegationRight::SendAs),
        "send_on_behalf" | "send-on-behalf" => Ok(SenderDelegationRight::SendOnBehalf),
        _ => Err("unsupported sender delegation right".to_string()),
    }
}

pub(crate) fn ensure_admin_can_manage_email(
    admin: &AuthenticatedAdmin,
    email: &str,
) -> std::result::Result<(), (StatusCode, String)> {
    if admin.role == "server-admin" || admin.role == "super-admin" || admin.domain_id.is_none() {
        return Ok(());
    }

    let suffix = format!("@{}", admin.domain_name.to_lowercase());
    if email.trim().to_lowercase().ends_with(&suffix) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "domain admin cannot manage this domain".to_string(),
        ))
    }
}

pub(crate) fn mailbox_account_email(
    dashboard: &AdminDashboard,
    mailbox_id: Uuid,
) -> Option<String> {
    dashboard
        .accounts
        .iter()
        .find(|account| {
            account
                .mailboxes
                .iter()
                .any(|mailbox| mailbox.id == mailbox_id)
        })
        .map(|account| account.email.clone())
}
