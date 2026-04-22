use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent, ClientTask,
    ClientTaskList, ClientWorkspace, HealthResponse, MailboxAccountAccess, SavedDraftMessage,
    Storage, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
};
use tracing::info;
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    observability, require_account,
    types::{
        ApiResult, SubmitMessageRequest, SubmitRecipientRequest, UpsertClientContactRequest,
        UpsertClientEventRequest, UpsertClientTaskRequest,
    },
};

pub(crate) async fn client_workspace(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<ClientWorkspace> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_client_workspace(account.account_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn submit_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> ApiResult<SubmittedMessage> {
    let account = require_account(&storage, &headers).await?;
    let mailbox_access = resolve_client_mailbox_access(&storage, &account, request.account_id).await?;
    let trace_id = observability::trace_id_from_headers(&headers);
    let subject_for_audit = request.subject.clone();
    let recipient_count = request.to.len()
        + request
            .cc
            .as_ref()
            .map(|entries| entries.len())
            .unwrap_or(0)
        + request
            .bcc
            .as_ref()
            .map(|entries| entries.len())
            .unwrap_or(0);
    let internet_message_id = request.internet_message_id.clone();
    let submitted = storage
        .submit_message(
            map_submit_message_request(&account, &mailbox_access, request),
            AuditEntryInput {
                actor: account.email,
                action: "submit-message".to_string(),
                subject: subject_for_audit,
            },
        )
        .await
        .map_err(internal_error)?;
    observability::record_mail_submission("api");
    info!(
        trace_id = %trace_id,
        account_id = %account.account_id,
        message_id = %submitted.message_id,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        recipient_count,
        "mail submission accepted"
    );

    Ok(Json(submitted))
}

pub(crate) async fn save_draft_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> ApiResult<SavedDraftMessage> {
    let account = require_account(&storage, &headers).await?;
    let mailbox_access = resolve_client_mailbox_access(&storage, &account, request.account_id).await?;
    let subject_for_audit = request.subject.clone();
    ensure_client_mailbox_write_access(&mailbox_access)?;
    let draft = storage
        .save_draft_message(
            map_submit_message_request(&account, &mailbox_access, request),
            AuditEntryInput {
                actor: account.email,
                action: "save-draft-message".to_string(),
                subject: subject_for_audit,
            },
        )
        .await
        .map_err(internal_error)?;

    Ok(Json(draft))
}

pub(crate) async fn delete_draft_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(message_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_draft_message(
            account.account_id,
            message_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-draft-message".to_string(),
                subject: message_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn upsert_client_contact(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientContactRequest>,
) -> ApiResult<ClientContact> {
    let account = require_account(&storage, &headers).await?;
    let input = UpsertClientContactInput {
        id: request.id,
        account_id: account.account_id,
        name: request.name,
        role: request.role,
        email: request.email,
        phone: request.phone,
        team: request.team,
        notes: request.notes,
    };
    let contact = if let Some(contact_id) = request.id {
        storage
            .update_accessible_contact(account.account_id, contact_id, input)
            .await
            .map_err(bad_request_error)?
    } else {
        storage
            .create_accessible_contact(account.account_id, request.collection_id.as_deref(), input)
            .await
            .map_err(bad_request_error)?
    };
    Ok(Json(ClientContact {
        id: contact.id,
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
    }))
}

pub(crate) async fn upsert_client_event(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientEventRequest>,
) -> ApiResult<ClientEvent> {
    let account = require_account(&storage, &headers).await?;
    let input = UpsertClientEventInput {
        id: request.id,
        account_id: account.account_id,
        date: request.date,
        time: request.time,
        time_zone: request.time_zone,
        duration_minutes: request.duration_minutes,
        recurrence_rule: request.recurrence_rule,
        title: request.title,
        location: request.location,
        attendees: request.attendees,
        attendees_json: request.attendees_json,
        notes: request.notes,
    };
    let event = if let Some(event_id) = request.id {
        storage
            .update_accessible_event(account.account_id, event_id, input)
            .await
            .map_err(bad_request_error)?
    } else {
        storage
            .create_accessible_event(account.account_id, request.collection_id.as_deref(), input)
            .await
            .map_err(bad_request_error)?
    };
    Ok(Json(ClientEvent {
        id: event.id,
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        recurrence_rule: event.recurrence_rule,
        title: event.title,
        location: event.location,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
    }))
}

pub(crate) async fn list_client_tasks(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<ClientTask>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_client_tasks(account.account_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn list_client_task_lists(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<ClientTaskList>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_task_lists(account.account_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn get_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<Uuid>,
) -> ApiResult<ClientTask> {
    let account = require_account(&storage, &headers).await?;
    let mut tasks = storage
        .fetch_client_tasks_by_ids(account.account_id, &[task_id])
        .await
        .map_err(internal_error)?;
    let task = tasks
        .pop()
        .ok_or((StatusCode::NOT_FOUND, "task not found".to_string()))?;
    Ok(Json(task))
}

pub(crate) async fn upsert_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientTaskRequest>,
) -> ApiResult<ClientTask> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_client_task(UpsertClientTaskInput {
                id: request.id,
                principal_account_id: account.account_id,
                account_id: account.account_id,
                task_list_id: request.task_list_id,
                title: request.title,
                description: request.description,
                status: request.status,
                due_at: request.due_at,
                completed_at: request.completed_at,
                sort_order: request.sort_order.unwrap_or(0),
            })
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_client_task(account.account_id, task_id)
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn resolve_client_mailbox_access(
    storage: &Storage,
    account: &AuthenticatedAccount,
    requested_account_id: Uuid,
) -> std::result::Result<MailboxAccountAccess, (StatusCode, String)> {
    let accessible = storage
        .fetch_accessible_mailbox_accounts(account.account_id)
        .await
        .map_err(internal_error)?;
    accessible
        .into_iter()
        .find(|entry| entry.account_id == requested_account_id)
        .ok_or((
            StatusCode::FORBIDDEN,
            "authenticated account cannot access this mailbox".to_string(),
        ))
}

fn ensure_client_mailbox_write_access(
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

fn map_submit_message_request(
    authenticated_account: &AuthenticatedAccount,
    mailbox_access: &MailboxAccountAccess,
    request: SubmitMessageRequest,
) -> SubmitMessageInput {
    let (sender_display, sender_address) =
        resolve_client_sender_fields(authenticated_account, mailbox_access, &request);
    SubmitMessageInput {
        draft_message_id: request.draft_message_id,
        account_id: request.account_id,
        submitted_by_account_id: authenticated_account.account_id,
        source: request.source.unwrap_or_else(|| "jmap".to_string()),
        from_display: request.from_display,
        from_address: request.from_address,
        sender_display,
        sender_address,
        to: map_recipients(request.to),
        cc: map_recipients(request.cc.unwrap_or_default()),
        bcc: map_recipients(request.bcc.unwrap_or_default()),
        subject: request.subject,
        body_text: request.body_text,
        body_html_sanitized: request.body_html_sanitized,
        internet_message_id: request.internet_message_id,
        mime_blob_ref: request.mime_blob_ref,
        size_octets: request.size_octets.unwrap_or(0),
        unread: None,
        flagged: None,
        attachments: Vec::new(),
    }
}

fn resolve_client_sender_fields(
    authenticated_account: &AuthenticatedAccount,
    mailbox_access: &MailboxAccountAccess,
    request: &SubmitMessageRequest,
) -> (Option<String>, Option<String>) {
    if request.sender_address.is_some() {
        return (
            request.sender_display.clone(),
            request.sender_address.clone(),
        );
    }

    if mailbox_access.account_id != authenticated_account.account_id
        && mailbox_access.may_send_on_behalf
        && !mailbox_access.may_send_as
    {
        return (
            Some(authenticated_account.display_name.clone()),
            Some(authenticated_account.email.clone()),
        );
    }

    (None, None)
}

fn map_recipients(input: Vec<SubmitRecipientRequest>) -> Vec<SubmittedRecipientInput> {
    input
        .into_iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address,
            display_name: recipient.display_name,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::resolve_client_sender_fields;
    use crate::types::SubmitMessageRequest;
    use lpe_storage::{AuthenticatedAccount, MailboxAccountAccess};
    use uuid::Uuid;

    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::new_v4(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
            expires_at: "2026-04-22T00:00:00Z".to_string(),
        }
    }

    fn submit_request() -> SubmitMessageRequest {
        SubmitMessageRequest {
            draft_message_id: None,
            account_id: Uuid::new_v4(),
            source: Some("web-client".to_string()),
            from_display: Some("Shared Mailbox".to_string()),
            from_address: "shared@example.test".to_string(),
            sender_display: None,
            sender_address: None,
            to: Vec::new(),
            cc: None,
            bcc: None,
            subject: "Subject".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            mime_blob_ref: None,
            size_octets: Some(32),
        }
    }

    #[test]
    fn delegated_send_on_behalf_defaults_sender_to_authenticated_account() {
        let authenticated = account();
        let mailbox_access = MailboxAccountAccess {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as: false,
            may_send_on_behalf: true,
        };

        let (sender_display, sender_address) =
            resolve_client_sender_fields(&authenticated, &mailbox_access, &submit_request());

        assert_eq!(sender_display.as_deref(), Some("Delegate"));
        assert_eq!(sender_address.as_deref(), Some("delegate@example.test"));
    }

    #[test]
    fn delegated_send_as_without_explicit_sender_keeps_sender_empty() {
        let authenticated = account();
        let mailbox_access = MailboxAccountAccess {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: true,
        };

        let (sender_display, sender_address) =
            resolve_client_sender_fields(&authenticated, &mailbox_access, &submit_request());

        assert_eq!(sender_display, None);
        assert_eq!(sender_address, None);
    }

    #[test]
    fn explicit_sender_fields_are_preserved() {
        let authenticated = account();
        let mailbox_access = MailboxAccountAccess {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as: false,
            may_send_on_behalf: true,
        };
        let mut request = submit_request();
        request.sender_display = Some("Delegate".to_string());
        request.sender_address = Some("delegate@example.test".to_string());

        let (sender_display, sender_address) =
            resolve_client_sender_fields(&authenticated, &mailbox_access, &request);

        assert_eq!(sender_display.as_deref(), Some("Delegate"));
        assert_eq!(sender_address.as_deref(), Some("delegate@example.test"));
    }
}
