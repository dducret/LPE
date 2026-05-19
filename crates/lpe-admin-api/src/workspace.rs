use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent, ClientNote, ClientReminder,
    ClientTask, ClientTaskList, ClientWorkspace, HealthResponse, JournalEntry,
    MailboxAccountAccess, ReminderQuery, SavedDraftMessage, Storage, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput,
};
use tracing::info;
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    observability, require_account,
    types::{
        ApiResult, ReminderQueryRequest, SubmitMessageRequest, SubmitRecipientRequest,
        UpsertClientContactRequest, UpsertClientEventRequest, UpsertClientNoteRequest,
        UpsertClientTaskRequest, UpsertJournalEntryRequest,
    },
};

#[allow(async_fn_in_trait)]
trait ClientSessionStore {
    async fn fetch_account_session(
        &self,
        token: &str,
    ) -> anyhow::Result<Option<AuthenticatedAccount>>;
}

#[allow(async_fn_in_trait)]
trait ClientSubmissionStore: ClientSessionStore {
    async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> anyhow::Result<Vec<MailboxAccountAccess>>;
    async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> anyhow::Result<SubmittedMessage>;
}

impl ClientSessionStore for Storage {
    async fn fetch_account_session(
        &self,
        token: &str,
    ) -> anyhow::Result<Option<AuthenticatedAccount>> {
        Storage::fetch_account_session(self, token).await
    }
}

#[allow(async_fn_in_trait)]
trait ClientOutlookStore: ClientSessionStore {
    async fn fetch_client_notes(&self, account_id: Uuid) -> anyhow::Result<Vec<ClientNote>>;
    async fn fetch_client_notes_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<ClientNote>>;
    async fn upsert_client_note(&self, input: UpsertClientNoteInput) -> anyhow::Result<ClientNote>;
    async fn delete_client_note(&self, account_id: Uuid, note_id: Uuid) -> anyhow::Result<()>;
    async fn fetch_journal_entries(&self, account_id: Uuid) -> anyhow::Result<Vec<JournalEntry>>;
    async fn fetch_journal_entries_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<JournalEntry>>;
    async fn upsert_journal_entry(
        &self,
        input: UpsertJournalEntryInput,
    ) -> anyhow::Result<JournalEntry>;
    async fn delete_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> anyhow::Result<()>;
    async fn query_client_reminders(
        &self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> anyhow::Result<Vec<ClientReminder>>;
}

impl ClientSubmissionStore for Storage {
    async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> anyhow::Result<Vec<MailboxAccountAccess>> {
        Storage::fetch_accessible_mailbox_accounts(self, principal_account_id).await
    }

    async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> anyhow::Result<SubmittedMessage> {
        Storage::submit_message(self, input, audit).await
    }
}

impl ClientOutlookStore for Storage {
    async fn fetch_client_notes(&self, account_id: Uuid) -> anyhow::Result<Vec<ClientNote>> {
        Storage::fetch_client_notes(self, account_id).await
    }

    async fn fetch_client_notes_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<ClientNote>> {
        Storage::fetch_client_notes_by_ids(self, account_id, ids).await
    }

    async fn upsert_client_note(&self, input: UpsertClientNoteInput) -> anyhow::Result<ClientNote> {
        Storage::upsert_client_note(self, input).await
    }

    async fn delete_client_note(&self, account_id: Uuid, note_id: Uuid) -> anyhow::Result<()> {
        Storage::delete_client_note(self, account_id, note_id).await
    }

    async fn fetch_journal_entries(&self, account_id: Uuid) -> anyhow::Result<Vec<JournalEntry>> {
        Storage::fetch_journal_entries(self, account_id).await
    }

    async fn fetch_journal_entries_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<JournalEntry>> {
        Storage::fetch_journal_entries_by_ids(self, account_id, ids).await
    }

    async fn upsert_journal_entry(
        &self,
        input: UpsertJournalEntryInput,
    ) -> anyhow::Result<JournalEntry> {
        Storage::upsert_journal_entry(self, input).await
    }

    async fn delete_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> anyhow::Result<()> {
        Storage::delete_journal_entry(self, account_id, entry_id).await
    }

    async fn query_client_reminders(
        &self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> anyhow::Result<Vec<ClientReminder>> {
        Storage::query_client_reminders(self, account_id, query).await
    }
}

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
    let submitted = submit_message_with_store(&storage, &headers, request).await?;

    Ok(Json(submitted))
}

async fn submit_message_with_store<S: ClientSubmissionStore>(
    storage: &S,
    headers: &HeaderMap,
    request: SubmitMessageRequest,
) -> std::result::Result<SubmittedMessage, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    let mailbox_access =
        resolve_client_mailbox_access(storage, &account, request.account_id).await?;
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
        .map_err(classify_client_submission_storage_error)?;
    observability::record_mail_submission("api");
    info!(
        trace_id = %trace_id,
        account_id = %account.account_id,
        message_id = %submitted.message_id,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        recipient_count,
        "mail submission accepted"
    );

    Ok(submitted)
}

pub(crate) async fn save_draft_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> ApiResult<SavedDraftMessage> {
    let account = require_account(&storage, &headers).await?;
    let mailbox_access =
        resolve_client_mailbox_access(&storage, &account, request.account_id).await?;
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

pub(crate) async fn delete_client_contact(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(contact_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_accessible_contact(account.account_id, contact_id)
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
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
        uid: request.uid,
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
        uid: event.uid,
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

pub(crate) async fn delete_client_event(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(event_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_accessible_event(account.account_id, event_id)
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
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

pub(crate) async fn list_client_notes(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<ClientNote>> {
    Ok(Json(
        list_client_notes_with_store(&storage, &headers).await?,
    ))
}

pub(crate) async fn get_client_note(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(note_id): AxumPath<Uuid>,
) -> ApiResult<ClientNote> {
    Ok(Json(
        get_client_note_with_store(&storage, &headers, note_id).await?,
    ))
}

pub(crate) async fn upsert_client_note(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientNoteRequest>,
) -> ApiResult<ClientNote> {
    Ok(Json(
        upsert_client_note_with_store(&storage, &headers, request).await?,
    ))
}

pub(crate) async fn delete_client_note(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(note_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    delete_client_note_with_store(&storage, &headers, note_id).await?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn list_journal_entries(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<JournalEntry>> {
    Ok(Json(
        list_journal_entries_with_store(&storage, &headers).await?,
    ))
}

pub(crate) async fn get_journal_entry(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(entry_id): AxumPath<Uuid>,
) -> ApiResult<JournalEntry> {
    Ok(Json(
        get_journal_entry_with_store(&storage, &headers, entry_id).await?,
    ))
}

pub(crate) async fn upsert_journal_entry(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertJournalEntryRequest>,
) -> ApiResult<JournalEntry> {
    Ok(Json(
        upsert_journal_entry_with_store(&storage, &headers, request).await?,
    ))
}

pub(crate) async fn delete_journal_entry(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(entry_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    delete_journal_entry_with_store(&storage, &headers, entry_id).await?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn query_client_reminders(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Query(request): Query<ReminderQueryRequest>,
) -> ApiResult<Vec<ClientReminder>> {
    Ok(Json(
        query_client_reminders_with_store(&storage, &headers, request).await?,
    ))
}

async fn list_client_notes_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
) -> std::result::Result<Vec<ClientNote>, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .fetch_client_notes(account.account_id)
        .await
        .map_err(internal_error)
}

async fn get_client_note_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    note_id: Uuid,
) -> std::result::Result<ClientNote, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    let mut notes = storage
        .fetch_client_notes_by_ids(account.account_id, &[note_id])
        .await
        .map_err(internal_error)?;
    notes
        .pop()
        .ok_or((StatusCode::NOT_FOUND, "note not found".to_string()))
}

async fn upsert_client_note_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    request: UpsertClientNoteRequest,
) -> std::result::Result<ClientNote, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .upsert_client_note(UpsertClientNoteInput {
            id: request.id,
            account_id: account.account_id,
            title: request.title,
            body_text: request.body_text,
            color: request.color,
            categories_json: request.categories_json.unwrap_or_else(|| "[]".to_string()),
        })
        .await
        .map_err(bad_request_error)
}

async fn delete_client_note_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    note_id: Uuid,
) -> std::result::Result<(), (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .delete_client_note(account.account_id, note_id)
        .await
        .map_err(bad_request_error)
}

async fn list_journal_entries_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
) -> std::result::Result<Vec<JournalEntry>, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .fetch_journal_entries(account.account_id)
        .await
        .map_err(internal_error)
}

async fn get_journal_entry_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    entry_id: Uuid,
) -> std::result::Result<JournalEntry, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    let mut entries = storage
        .fetch_journal_entries_by_ids(account.account_id, &[entry_id])
        .await
        .map_err(internal_error)?;
    entries
        .pop()
        .ok_or((StatusCode::NOT_FOUND, "journal entry not found".to_string()))
}

async fn upsert_journal_entry_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    request: UpsertJournalEntryRequest,
) -> std::result::Result<JournalEntry, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .upsert_journal_entry(UpsertJournalEntryInput {
            id: request.id,
            account_id: account.account_id,
            subject: request.subject,
            body_text: request.body_text,
            entry_type: request.entry_type,
            message_class: request
                .message_class
                .unwrap_or_else(|| "IPM.Activity".to_string()),
            starts_at: request.starts_at,
            ends_at: request.ends_at,
            occurred_at: request.occurred_at,
            companies_json: request.companies_json.unwrap_or_else(|| "[]".to_string()),
            contacts_json: request.contacts_json.unwrap_or_else(|| "[]".to_string()),
        })
        .await
        .map_err(bad_request_error)
}

async fn delete_journal_entry_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    entry_id: Uuid,
) -> std::result::Result<(), (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .delete_journal_entry(account.account_id, entry_id)
        .await
        .map_err(bad_request_error)
}

async fn query_client_reminders_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    request: ReminderQueryRequest,
) -> std::result::Result<Vec<ClientReminder>, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .query_client_reminders(
            account.account_id,
            ReminderQuery {
                include_inactive: request.include_inactive.unwrap_or(false),
            },
        )
        .await
        .map_err(internal_error)
}

async fn require_account_from_store<S: ClientSessionStore>(
    storage: &S,
    headers: &HeaderMap,
) -> std::result::Result<AuthenticatedAccount, (StatusCode, String)> {
    let token = crate::http::bearer_token(headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    storage
        .fetch_account_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid or expired session".to_string(),
        ))
}

async fn resolve_client_mailbox_access<S: ClientSubmissionStore>(
    storage: &S,
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

fn classify_client_submission_storage_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("send as is not granted")
        || lowered.contains("send on behalf is not granted")
        || lowered.contains("from email must match authenticated account")
        || lowered.contains("from email must match delegated mailbox")
        || lowered.contains("sender email must match authenticated account")
        || lowered.contains("account not found")
    {
        return (StatusCode::FORBIDDEN, message);
    }

    if lowered.contains("from_address is required")
        || lowered.contains("at least one recipient")
        || lowered.contains("subject or body_text")
    {
        return (StatusCode::BAD_REQUEST, message);
    }

    internal_error(message)
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
    use super::{
        classify_client_submission_storage_error, delete_client_note_with_store,
        delete_journal_entry_with_store, get_client_note_with_store, get_journal_entry_with_store,
        list_client_notes_with_store, list_journal_entries_with_store, map_submit_message_request,
        query_client_reminders_with_store, resolve_client_sender_fields, submit_message_with_store,
        upsert_client_note_with_store, upsert_journal_entry_with_store,
    };
    use crate::types::{
        ReminderQueryRequest, SubmitMessageRequest, UpsertClientNoteRequest,
        UpsertJournalEntryRequest,
    };
    use axum::http::{HeaderMap, HeaderValue};
    use lpe_storage::{
        AuditEntryInput, AuthenticatedAccount, ClientNote, ClientReminder, JournalEntry,
        MailboxAccountAccess, ReminderQuery, SubmitMessageInput, SubmittedMessage,
        UpsertClientNoteInput, UpsertJournalEntryInput,
    };
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone)]
    struct FakeSubmissionStore {
        session: Option<AuthenticatedAccount>,
        accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
        submitted: Arc<Mutex<Vec<SubmitMessageInput>>>,
        audits: Arc<Mutex<Vec<AuditEntryInput>>>,
    }

    #[derive(Clone)]
    struct FakeOutlookStore {
        session: Option<AuthenticatedAccount>,
        notes: Arc<Mutex<Vec<ClientNote>>>,
        journal_entries: Arc<Mutex<Vec<JournalEntry>>>,
        reminders: Arc<Mutex<Vec<ClientReminder>>>,
        note_inputs: Arc<Mutex<Vec<UpsertClientNoteInput>>>,
        journal_inputs: Arc<Mutex<Vec<UpsertJournalEntryInput>>>,
        deleted_notes: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
        deleted_journal_entries: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
        reminder_queries: Arc<Mutex<Vec<(Uuid, bool)>>>,
    }

    impl Default for FakeOutlookStore {
        fn default() -> Self {
            Self {
                session: Some(account()),
                notes: Arc::new(Mutex::new(vec![note()])),
                journal_entries: Arc::new(Mutex::new(vec![journal_entry()])),
                reminders: Arc::new(Mutex::new(vec![reminder()])),
                note_inputs: Arc::new(Mutex::new(Vec::new())),
                journal_inputs: Arc::new(Mutex::new(Vec::new())),
                deleted_notes: Arc::new(Mutex::new(Vec::new())),
                deleted_journal_entries: Arc::new(Mutex::new(Vec::new())),
                reminder_queries: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[allow(async_fn_in_trait)]
    impl super::ClientSessionStore for FakeSubmissionStore {
        async fn fetch_account_session(
            &self,
            token: &str,
        ) -> anyhow::Result<Option<AuthenticatedAccount>> {
            Ok(if token == "token" {
                self.session.clone()
            } else {
                None
            })
        }
    }

    #[allow(async_fn_in_trait)]
    impl super::ClientSubmissionStore for FakeSubmissionStore {
        async fn fetch_accessible_mailbox_accounts(
            &self,
            _principal_account_id: Uuid,
        ) -> anyhow::Result<Vec<MailboxAccountAccess>> {
            Ok(self.accessible_mailbox_accounts.clone())
        }

        async fn submit_message(
            &self,
            input: SubmitMessageInput,
            audit: AuditEntryInput,
        ) -> anyhow::Result<SubmittedMessage> {
            self.submitted.lock().unwrap().push(input.clone());
            self.audits.lock().unwrap().push(audit);
            Ok(SubmittedMessage {
                message_id: Uuid::parse_str("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb").unwrap(),
                thread_id: Uuid::parse_str("cccccccc-1111-2222-3333-dddddddddddd").unwrap(),
                account_id: input.account_id,
                submitted_by_account_id: input.submitted_by_account_id,
                sent_mailbox_id: Uuid::parse_str("eeeeeeee-1111-2222-3333-ffffffffffff").unwrap(),
                outbound_queue_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                delivery_status: "queued".to_string(),
            })
        }
    }

    #[allow(async_fn_in_trait)]
    impl super::ClientSessionStore for FakeOutlookStore {
        async fn fetch_account_session(
            &self,
            token: &str,
        ) -> anyhow::Result<Option<AuthenticatedAccount>> {
            Ok(if token == "token" {
                self.session.clone()
            } else {
                None
            })
        }
    }

    #[allow(async_fn_in_trait)]
    impl super::ClientOutlookStore for FakeOutlookStore {
        async fn fetch_client_notes(&self, account_id: Uuid) -> anyhow::Result<Vec<ClientNote>> {
            Ok(self
                .notes
                .lock()
                .unwrap()
                .iter()
                .filter(|_| account_id == account().account_id)
                .cloned()
                .collect())
        }

        async fn fetch_client_notes_by_ids(
            &self,
            account_id: Uuid,
            ids: &[Uuid],
        ) -> anyhow::Result<Vec<ClientNote>> {
            Ok(self
                .notes
                .lock()
                .unwrap()
                .iter()
                .filter(|note| account_id == account().account_id && ids.contains(&note.id))
                .cloned()
                .collect())
        }

        async fn upsert_client_note(
            &self,
            input: UpsertClientNoteInput,
        ) -> anyhow::Result<ClientNote> {
            self.note_inputs.lock().unwrap().push(input.clone());
            let note = ClientNote {
                id: input.id.unwrap_or_else(note_id),
                title: input.title,
                body_text: input.body_text,
                color: input.color,
                categories_json: input.categories_json,
                created_at: "2026-05-19T10:00:00Z".to_string(),
                updated_at: "2026-05-19T10:00:00Z".to_string(),
            };
            self.notes.lock().unwrap().push(note.clone());
            Ok(note)
        }

        async fn delete_client_note(&self, account_id: Uuid, note_id: Uuid) -> anyhow::Result<()> {
            self.deleted_notes
                .lock()
                .unwrap()
                .push((account_id, note_id));
            self.notes.lock().unwrap().retain(|note| note.id != note_id);
            Ok(())
        }

        async fn fetch_journal_entries(
            &self,
            account_id: Uuid,
        ) -> anyhow::Result<Vec<JournalEntry>> {
            Ok(self
                .journal_entries
                .lock()
                .unwrap()
                .iter()
                .filter(|_| account_id == account().account_id)
                .cloned()
                .collect())
        }

        async fn fetch_journal_entries_by_ids(
            &self,
            account_id: Uuid,
            ids: &[Uuid],
        ) -> anyhow::Result<Vec<JournalEntry>> {
            Ok(self
                .journal_entries
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| account_id == account().account_id && ids.contains(&entry.id))
                .cloned()
                .collect())
        }

        async fn upsert_journal_entry(
            &self,
            input: UpsertJournalEntryInput,
        ) -> anyhow::Result<JournalEntry> {
            self.journal_inputs.lock().unwrap().push(input.clone());
            let entry = JournalEntry {
                id: input.id.unwrap_or_else(journal_entry_id),
                subject: input.subject,
                body_text: input.body_text,
                entry_type: input.entry_type,
                message_class: input.message_class,
                starts_at: input.starts_at,
                ends_at: input.ends_at,
                occurred_at: input.occurred_at,
                companies_json: input.companies_json,
                contacts_json: input.contacts_json,
                created_at: "2026-05-19T10:00:00Z".to_string(),
                updated_at: "2026-05-19T10:00:00Z".to_string(),
            };
            self.journal_entries.lock().unwrap().push(entry.clone());
            Ok(entry)
        }

        async fn delete_journal_entry(
            &self,
            account_id: Uuid,
            entry_id: Uuid,
        ) -> anyhow::Result<()> {
            self.deleted_journal_entries
                .lock()
                .unwrap()
                .push((account_id, entry_id));
            self.journal_entries
                .lock()
                .unwrap()
                .retain(|entry| entry.id != entry_id);
            Ok(())
        }

        async fn query_client_reminders(
            &self,
            account_id: Uuid,
            query: ReminderQuery,
        ) -> anyhow::Result<Vec<ClientReminder>> {
            self.reminder_queries
                .lock()
                .unwrap()
                .push((account_id, query.include_inactive));
            Ok(self.reminders.lock().unwrap().clone())
        }
    }

    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
            account_id: account_id(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
            expires_at: "2026-04-22T00:00:00Z".to_string(),
        }
    }

    fn account_id() -> Uuid {
        Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap()
    }

    fn note_id() -> Uuid {
        Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    }

    fn journal_entry_id() -> Uuid {
        Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap()
    }

    fn note() -> ClientNote {
        ClientNote {
            id: note_id(),
            title: "Sticky note".to_string(),
            body_text: "Body".to_string(),
            color: "yellow".to_string(),
            categories_json: r#"["outlook"]"#.to_string(),
            created_at: "2026-05-19T09:00:00Z".to_string(),
            updated_at: "2026-05-19T09:00:00Z".to_string(),
        }
    }

    fn journal_entry() -> JournalEntry {
        JournalEntry {
            id: journal_entry_id(),
            subject: "Phone call".to_string(),
            body_text: "Call notes".to_string(),
            entry_type: "phone-call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: Some("2026-05-19T09:00:00Z".to_string()),
            ends_at: None,
            occurred_at: None,
            companies_json: "[]".to_string(),
            contacts_json: "[]".to_string(),
            created_at: "2026-05-19T09:00:00Z".to_string(),
            updated_at: "2026-05-19T09:00:00Z".to_string(),
        }
    }

    fn reminder() -> ClientReminder {
        ClientReminder {
            source_type: "task".to_string(),
            source_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
            title: "Reminder".to_string(),
            due_at: None,
            reminder_at: "2026-05-19T09:00:00Z".to_string(),
            dismissed_at: None,
            completed_at: None,
            status: "due".to_string(),
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
            to: vec![crate::types::SubmitRecipientRequest {
                address: "to@example.test".to_string(),
                display_name: Some("Primary".to_string()),
            }],
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

    fn owned_mailbox_access(authenticated: &AuthenticatedAccount) -> MailboxAccountAccess {
        MailboxAccountAccess {
            tenant_id: authenticated.tenant_id,
            account_id: authenticated.account_id,
            email: authenticated.email.clone(),
            display_name: authenticated.display_name.clone(),
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: true,
        }
    }

    fn bearer_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );
        headers
    }

    #[test]
    fn delegated_send_on_behalf_defaults_sender_to_authenticated_account() {
        let authenticated = account();
        let mailbox_access = MailboxAccountAccess {
            tenant_id: authenticated.tenant_id,
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
            tenant_id: authenticated.tenant_id,
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
            tenant_id: authenticated.tenant_id,
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

    #[test]
    fn client_submission_storage_errors_keep_actionable_status_codes() {
        let (status, message) = classify_client_submission_storage_error(anyhow::anyhow!(
            "at least one recipient is required"
        ));
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(message, "at least one recipient is required");

        let (status, message) = classify_client_submission_storage_error(anyhow::anyhow!(
            "send as is not granted for this mailbox"
        ));
        assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
        assert_eq!(message, "send as is not granted for this mailbox");
    }

    #[test]
    fn map_submit_message_request_preserves_web_submission_source() {
        let authenticated = account();
        let mailbox_access = owned_mailbox_access(&authenticated);

        let mapped = map_submit_message_request(&authenticated, &mailbox_access, submit_request());

        assert_eq!(mapped.source, "web-client");
        assert_eq!(mapped.submitted_by_account_id, authenticated.account_id);
        assert_eq!(mapped.draft_message_id, None);
    }

    #[tokio::test]
    async fn submit_message_handler_uses_canonical_submission_store_path() {
        let authenticated = account();
        let mut request = submit_request();
        request.account_id = authenticated.account_id;
        let store = FakeSubmissionStore {
            session: Some(authenticated.clone()),
            accessible_mailbox_accounts: vec![owned_mailbox_access(&authenticated)],
            submitted: Arc::new(Mutex::new(Vec::new())),
            audits: Arc::new(Mutex::new(Vec::new())),
        };

        let submitted = submit_message_with_store(&store, &bearer_headers(), request)
            .await
            .unwrap();

        assert_eq!(submitted.delivery_status, "queued");
        let recorded = store.submitted.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "web-client");
        assert_eq!(recorded[0].draft_message_id, None);
        assert_eq!(recorded[0].to[0].address, "to@example.test");
        assert!(recorded[0].cc.is_empty());
        assert!(recorded[0].bcc.is_empty());
        assert_eq!(store.audits.lock().unwrap()[0].action, "submit-message");
    }

    #[tokio::test]
    async fn notes_api_helpers_cover_authenticated_crud_path() {
        let store = FakeOutlookStore::default();
        let headers = bearer_headers();

        let listed = list_client_notes_with_store(&store, &headers)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        let fetched = get_client_note_with_store(&store, &headers, note_id())
            .await
            .unwrap();
        assert_eq!(fetched.title, "Sticky note");

        let created = upsert_client_note_with_store(
            &store,
            &headers,
            UpsertClientNoteRequest {
                id: None,
                title: "New note".to_string(),
                body_text: "API body".to_string(),
                color: "blue".to_string(),
                categories_json: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(created.title, "New note");
        let recorded = store.note_inputs.lock().unwrap();
        assert_eq!(recorded[0].account_id, account_id());
        assert_eq!(recorded[0].categories_json, "[]");
        drop(recorded);

        delete_client_note_with_store(&store, &headers, note_id())
            .await
            .unwrap();
        assert_eq!(
            store.deleted_notes.lock().unwrap().as_slice(),
            &[(account_id(), note_id())]
        );
    }

    #[tokio::test]
    async fn journal_api_helpers_cover_authenticated_crud_path() {
        let store = FakeOutlookStore::default();
        let headers = bearer_headers();

        let listed = list_journal_entries_with_store(&store, &headers)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        let fetched = get_journal_entry_with_store(&store, &headers, journal_entry_id())
            .await
            .unwrap();
        assert_eq!(fetched.subject, "Phone call");

        let created = upsert_journal_entry_with_store(
            &store,
            &headers,
            UpsertJournalEntryRequest {
                id: None,
                subject: "New journal".to_string(),
                body_text: "API body".to_string(),
                entry_type: "note".to_string(),
                message_class: None,
                starts_at: None,
                ends_at: None,
                occurred_at: Some("2026-05-19T09:30:00Z".to_string()),
                companies_json: None,
                contacts_json: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(created.subject, "New journal");
        let recorded = store.journal_inputs.lock().unwrap();
        assert_eq!(recorded[0].account_id, account_id());
        assert_eq!(recorded[0].message_class, "IPM.Activity");
        assert_eq!(recorded[0].companies_json, "[]");
        assert_eq!(recorded[0].contacts_json, "[]");
        drop(recorded);

        delete_journal_entry_with_store(&store, &headers, journal_entry_id())
            .await
            .unwrap();
        assert_eq!(
            store.deleted_journal_entries.lock().unwrap().as_slice(),
            &[(account_id(), journal_entry_id())]
        );
    }

    #[tokio::test]
    async fn reminder_api_helper_preserves_include_inactive_query() {
        let store = FakeOutlookStore::default();
        let reminders = query_client_reminders_with_store(
            &store,
            &bearer_headers(),
            ReminderQueryRequest {
                include_inactive: Some(true),
            },
        )
        .await
        .unwrap();

        assert_eq!(reminders[0].status, "due");
        assert_eq!(
            store.reminder_queries.lock().unwrap().as_slice(),
            &[(account_id(), true)]
        );
    }
}
