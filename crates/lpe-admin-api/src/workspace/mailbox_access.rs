use axum::http::StatusCode;
use lpe_storage::{AuthenticatedAccount, MailboxAccountAccess};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientWorkspaceQuery {
    pub(crate) account_id: Option<Uuid>,
}

pub(crate) fn ensure_client_mailbox_read_access(
    mailbox_access: &MailboxAccountAccess,
) -> std::result::Result<(), (StatusCode, String)> {
    if mailbox_access.is_owned || mailbox_access.may_read {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "authenticated account cannot read this mailbox".to_string(),
        ))
    }
}

pub(crate) fn classify_client_mailbox_access_error(error: anyhow::Error) -> (StatusCode, String) {
    if error.to_string() == "mailbox account is not accessible" {
        (
            StatusCode::FORBIDDEN,
            "authenticated account cannot access this mailbox".to_string(),
        )
    } else {
        crate::http::internal_error(error)
    }
}

pub(crate) async fn resolve_client_mailbox_access<S: super::ClientSubmissionStore>(
    storage: &S,
    account: &AuthenticatedAccount,
    requested_account_id: Uuid,
) -> std::result::Result<MailboxAccountAccess, (StatusCode, String)> {
    let accessible = storage
        .fetch_accessible_mailbox_accounts(account.account_id)
        .await
        .map_err(crate::http::internal_error)?;
    accessible
        .into_iter()
        .find(|entry| entry.account_id == requested_account_id)
        .ok_or((
            StatusCode::FORBIDDEN,
            "authenticated account cannot access this mailbox".to_string(),
        ))
}

pub(crate) fn ensure_client_mailbox_write_access(
    mailbox_access: &MailboxAccountAccess,
) -> std::result::Result<(), (StatusCode, String)> {
    if mailbox_access.is_owned || mailbox_access.may_write {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "authenticated account cannot write drafts in this mailbox".to_string(),
        ))
    }
}
