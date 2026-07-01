use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{
    AccessibleContact, AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent,
    ClientNote, ClientReminder, ClientTask, ClientTaskList, ClientWorkspace,
    CollaborationCollection, HealthResponse, JmapEmail, JmapEmailFollowupUpdate, JournalEntry,
    MailboxAccountAccess, OutlookProfileState, RecipientSuggestion, RecoverableItem, ReminderQuery,
    SavedDraftMessage, SearchFolderDefinition, Storage, SubmitMessageInput, SubmittedMessage,
    SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput, UpsertSearchFolderInput,
};
use tracing::info;
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    observability, require_account,
    types::{
        ApiResult, PatchClientContactRequest, RecipientSuggestionQuery,
        RecoverableItemsQueryRequest, ReminderQueryRequest, RestoreRecoverableItemRequest,
        SubmitMessageRequest, SubmitRecipientRequest, UpdateMessageFlagRequest,
        UpsertClientContactRequest, UpsertClientEventRequest, UpsertClientNoteRequest,
        UpsertClientTaskRequest, UpsertJournalEntryRequest, UpsertSearchFolderRequest,
    },
};

mod public_folders;
pub(crate) use public_folders::*;

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
    async fn update_jmap_email_followup_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail>;
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
    async fn fetch_search_folders(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<Vec<SearchFolderDefinition>>;
    async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<SearchFolderDefinition>>;
    async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> anyhow::Result<SearchFolderDefinition>;
    async fn delete_search_folder(
        &self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> anyhow::Result<()>;
    async fn fetch_outlook_profile_state(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<OutlookProfileState>;
}

#[allow(async_fn_in_trait)]
trait ClientRecoverableStore: ClientSessionStore {
    async fn list_recoverable_items(
        &self,
        account_id: Uuid,
        recoverable_folder: Option<&str>,
    ) -> anyhow::Result<Vec<RecoverableItem>>;
    async fn restore_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail>;
    async fn purge_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> anyhow::Result<()>;
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

    async fn update_jmap_email_followup_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail> {
        Storage::update_jmap_email_followup_flags(self, account_id, message_id, update, audit).await
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

    async fn fetch_search_folders(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<Vec<SearchFolderDefinition>> {
        Storage::fetch_search_folders(self, account_id).await
    }

    async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<SearchFolderDefinition>> {
        Storage::fetch_search_folders_by_ids(self, account_id, ids).await
    }

    async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> anyhow::Result<SearchFolderDefinition> {
        Storage::upsert_search_folder(self, input).await
    }

    async fn delete_search_folder(
        &self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> anyhow::Result<()> {
        Storage::delete_search_folder(self, account_id, search_folder_id).await
    }

    async fn fetch_outlook_profile_state(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<OutlookProfileState> {
        Storage::fetch_outlook_profile_state(self, account_id).await
    }
}

impl ClientRecoverableStore for Storage {
    async fn list_recoverable_items(
        &self,
        account_id: Uuid,
        recoverable_folder: Option<&str>,
    ) -> anyhow::Result<Vec<RecoverableItem>> {
        Storage::list_recoverable_items(self, account_id, recoverable_folder).await
    }

    async fn restore_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail> {
        Storage::restore_recoverable_item(
            self,
            account_id,
            recoverable_item_id,
            target_mailbox_id,
            audit,
        )
        .await
    }

    async fn purge_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> anyhow::Result<()> {
        Storage::purge_recoverable_item(self, account_id, recoverable_item_id, audit).await
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

pub(crate) async fn update_message_flag(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(message_id): AxumPath<Uuid>,
    Json(request): Json<UpdateMessageFlagRequest>,
) -> ApiResult<HealthResponse> {
    update_message_flag_with_store(&storage, &headers, message_id, request).await?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn update_message_flag_with_store<S: ClientSubmissionStore>(
    storage: &S,
    headers: &HeaderMap,
    message_id: Uuid,
    request: UpdateMessageFlagRequest,
) -> std::result::Result<(), (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .update_jmap_email_followup_flags(
            account.account_id,
            message_id,
            map_update_message_flag_request(&request),
            AuditEntryInput {
                actor: account.email,
                action: "client-update-message-flag".to_string(),
                subject: format!("message:{message_id}"),
            },
        )
        .await
        .map(|_| ())
        .map_err(classify_client_submission_storage_error)
}

fn map_update_message_flag_request(request: &UpdateMessageFlagRequest) -> JmapEmailFollowupUpdate {
    if !request.flagged {
        return JmapEmailFollowupUpdate {
            flagged: Some(false),
            followup_flag_status: Some("none".to_string()),
            followup_icon: Some(0),
            todo_item_flags: Some(0),
            followup_request: Some(String::new()),
            ..Default::default()
        };
    }

    let status = if request.completed.unwrap_or(false) {
        "complete"
    } else {
        "flagged"
    };
    let due_at = request
        .due_at
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let clear_due = request.clear_due.unwrap_or(false);
    let reminder_at = request
        .reminder_at
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let clear_reminder = request.clear_reminder.unwrap_or(false);
    JmapEmailFollowupUpdate {
        flagged: Some(true),
        followup_flag_status: Some(status.to_string()),
        followup_icon: Some(6),
        todo_item_flags: Some(8),
        followup_request: Some("Follow up".to_string()),
        followup_start_at: if clear_due {
            Some(String::new())
        } else {
            due_at.clone()
        },
        followup_due_at: if clear_due {
            Some(String::new())
        } else {
            due_at
        },
        reminder_set: if clear_reminder {
            Some(false)
        } else {
            reminder_at.as_ref().map(|_| true)
        },
        reminder_at: if clear_reminder {
            Some(String::new())
        } else {
            reminder_at
        },
        reminder_dismissed_at: if clear_reminder {
            Some(String::new())
        } else {
            None
        },
        ..Default::default()
    }
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

pub(crate) async fn list_recoverable_items(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Query(request): Query<RecoverableItemsQueryRequest>,
) -> ApiResult<Vec<RecoverableItem>> {
    list_recoverable_items_with_store(&storage, &headers, request).await
}

async fn list_recoverable_items_with_store<S: ClientRecoverableStore>(
    storage: &S,
    headers: &HeaderMap,
    request: RecoverableItemsQueryRequest,
) -> ApiResult<Vec<RecoverableItem>> {
    let account = require_account_from_store(storage, headers).await?;
    Ok(Json(
        storage
            .list_recoverable_items(account.account_id, request.folder.as_deref())
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn restore_recoverable_item(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(recoverable_item_id): AxumPath<Uuid>,
    Json(request): Json<RestoreRecoverableItemRequest>,
) -> ApiResult<JmapEmail> {
    restore_recoverable_item_with_store(&storage, &headers, recoverable_item_id, request).await
}

async fn restore_recoverable_item_with_store<S: ClientRecoverableStore>(
    storage: &S,
    headers: &HeaderMap,
    recoverable_item_id: Uuid,
    request: RestoreRecoverableItemRequest,
) -> ApiResult<JmapEmail> {
    let account = require_account_from_store(storage, headers).await?;
    Ok(Json(
        storage
            .restore_recoverable_item(
                account.account_id,
                recoverable_item_id,
                request.target_mailbox_id,
                AuditEntryInput {
                    actor: account.email,
                    action: "restore-recoverable-message".to_string(),
                    subject: recoverable_item_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn purge_recoverable_item(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(recoverable_item_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    purge_recoverable_item_with_store(&storage, &headers, recoverable_item_id).await
}

async fn purge_recoverable_item_with_store<S: ClientRecoverableStore>(
    storage: &S,
    headers: &HeaderMap,
    recoverable_item_id: Uuid,
) -> ApiResult<HealthResponse> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .purge_recoverable_item(
            account.account_id,
            recoverable_item_id,
            AuditEntryInput {
                actor: account.email,
                action: "purge-recoverable-message".to_string(),
                subject: recoverable_item_id.to_string(),
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
    let contact_id = request.id;
    let collection_id = request.collection_id.clone();
    let input = contact_input_from_request(account.account_id, request);
    let contact = if let Some(contact_id) = contact_id {
        storage
            .update_accessible_contact(account.account_id, contact_id, input)
            .await
            .map_err(bad_request_error)?
    } else {
        storage
            .create_accessible_contact(account.account_id, collection_id.as_deref(), input)
            .await
            .map_err(bad_request_error)?
    };
    Ok(Json(client_contact_from_accessible(contact)))
}

pub(crate) async fn list_contact_books(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<CollaborationCollection>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_accessible_contact_collections(account.account_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn list_client_contacts(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<ClientContact>> {
    let account = require_account(&storage, &headers).await?;
    let contacts = storage
        .fetch_accessible_contacts(account.account_id)
        .await
        .map_err(bad_request_error)?
        .into_iter()
        .map(client_contact_from_accessible)
        .collect();
    Ok(Json(contacts))
}

pub(crate) async fn get_client_contact(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(contact_id): AxumPath<Uuid>,
) -> ApiResult<ClientContact> {
    let account = require_account(&storage, &headers).await?;
    let contact = storage
        .fetch_accessible_contacts_by_ids(account.account_id, &[contact_id])
        .await
        .map_err(bad_request_error)?
        .into_iter()
        .next()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "contact not found".to_string()))?;
    Ok(Json(client_contact_from_accessible(contact)))
}

pub(crate) async fn patch_client_contact(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(contact_id): AxumPath<Uuid>,
    Json(request): Json<PatchClientContactRequest>,
) -> ApiResult<ClientContact> {
    let account = require_account(&storage, &headers).await?;
    let existing = storage
        .fetch_accessible_contacts_by_ids(account.account_id, &[contact_id])
        .await
        .map_err(bad_request_error)?
        .into_iter()
        .next()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "contact not found".to_string()))?;
    let raw_vcard_is_explicit = request.raw_vcard.is_present();
    let raw_vcard = match request.raw_vcard {
        crate::types::PatchField::Missing => existing.raw_vcard.clone(),
        crate::types::PatchField::Null => None,
        crate::types::PatchField::Value(value) => Some(value),
    };
    let input = UpsertClientContactInput {
        id: Some(contact_id),
        account_id: account.account_id,
        name: request.name.unwrap_or(existing.name),
        role: request.role.unwrap_or(existing.role),
        email: request.email.unwrap_or(existing.email),
        phone: request.phone.unwrap_or(existing.phone),
        team: request.team.unwrap_or(existing.team),
        notes: request.notes.unwrap_or(existing.notes),
        structured_name: request.structured_name.unwrap_or(existing.structured_name),
        emails_json: Some(request.emails_json.unwrap_or(existing.emails_json)),
        phones_json: Some(request.phones_json.unwrap_or(existing.phones_json)),
        addresses_json: Some(request.addresses_json.unwrap_or(existing.addresses_json)),
        urls_json: Some(request.urls_json.unwrap_or(existing.urls_json)),
        organization_name: request
            .organization_name
            .unwrap_or(existing.organization_name),
        job_title: request.job_title.unwrap_or(existing.job_title),
        raw_vcard_is_explicit,
        raw_vcard,
        source_is_explicit: request.source.is_some(),
        source: request.source.unwrap_or(existing.source),
    };
    let contact = storage
        .update_accessible_contact(account.account_id, contact_id, input)
        .await
        .map_err(bad_request_error)?;
    Ok(Json(client_contact_from_accessible(contact)))
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

pub(crate) async fn query_recipient_suggestions(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Query(request): Query<RecipientSuggestionQuery>,
) -> ApiResult<Vec<RecipientSuggestion>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .query_recipient_suggestions(account.account_id, request.q.as_deref())
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn dismiss_recipient_suggestion(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(suggestion_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .dismiss_recipient_suggestion(account.account_id, suggestion_id)
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

fn contact_input_from_request(
    account_id: Uuid,
    request: UpsertClientContactRequest,
) -> UpsertClientContactInput {
    UpsertClientContactInput {
        id: request.id,
        account_id,
        name: request.name,
        role: request.role,
        email: request.email,
        phone: request.phone,
        team: request.team,
        notes: request.notes,
        structured_name: request.structured_name,
        emails_json: request.emails_json,
        phones_json: request.phones_json,
        addresses_json: request.addresses_json,
        urls_json: request.urls_json,
        organization_name: request.organization_name,
        job_title: request.job_title,
        raw_vcard_is_explicit: request.raw_vcard.is_some(),
        raw_vcard: request.raw_vcard,
        source_is_explicit: request.source.is_some(),
        source: request.source.unwrap_or_default(),
    }
}

fn client_contact_from_accessible(contact: AccessibleContact) -> ClientContact {
    ClientContact {
        id: contact.id,
        address_book_id: contact.collection_id,
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
        structured_name: contact.structured_name,
        emails_json: contact.emails_json,
        phones_json: contact.phones_json,
        addresses_json: contact.addresses_json,
        urls_json: contact.urls_json,
        organization_name: contact.organization_name,
        job_title: contact.job_title,
        raw_vcard: contact.raw_vcard,
        source: contact.source,
    }
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
        all_day: request.all_day,
        status: if request.status.trim().is_empty() {
            "confirmed".to_string()
        } else {
            request.status
        },
        sequence: request.sequence,
        recurrence_rule: request.recurrence_rule,
        recurrence_json: if request.recurrence_json.trim().is_empty() {
            "{}".to_string()
        } else {
            request.recurrence_json
        },
        recurrence_exceptions_json: if request.recurrence_exceptions_json.trim().is_empty() {
            "[]".to_string()
        } else {
            request.recurrence_exceptions_json
        },
        title: request.title,
        location: request.location,
        organizer_json: if request.organizer_json.trim().is_empty() {
            "{}".to_string()
        } else {
            request.organizer_json
        },
        attendees: request.attendees,
        attendees_json: request.attendees_json,
        notes: request.notes,
        body_html: request.body_html,
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
        all_day: event.all_day,
        status: event.status,
        sequence: event.sequence,
        recurrence_rule: event.recurrence_rule,
        recurrence_json: event.recurrence_json,
        recurrence_exceptions_json: event.recurrence_exceptions_json,
        title: event.title,
        location: event.location,
        organizer_json: event.organizer_json,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
        body_html: event.body_html,
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
                recurrence_rule: request.recurrence_rule.unwrap_or_default(),
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

pub(crate) async fn list_search_folders(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<SearchFolderDefinition>> {
    Ok(Json(
        list_search_folders_with_store(&storage, &headers).await?,
    ))
}

pub(crate) async fn get_search_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(search_folder_id): AxumPath<Uuid>,
) -> ApiResult<SearchFolderDefinition> {
    Ok(Json(
        get_search_folder_with_store(&storage, &headers, search_folder_id).await?,
    ))
}

pub(crate) async fn upsert_search_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertSearchFolderRequest>,
) -> ApiResult<SearchFolderDefinition> {
    Ok(Json(
        upsert_search_folder_with_store(&storage, &headers, request).await?,
    ))
}

pub(crate) async fn delete_search_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(search_folder_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    delete_search_folder_with_store(&storage, &headers, search_folder_id).await?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn outlook_profile_state(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<OutlookProfileState> {
    Ok(Json(
        outlook_profile_state_with_store(&storage, &headers).await?,
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

async fn list_search_folders_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
) -> std::result::Result<Vec<SearchFolderDefinition>, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .fetch_search_folders(account.account_id)
        .await
        .map_err(internal_error)
}

async fn get_search_folder_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    search_folder_id: Uuid,
) -> std::result::Result<SearchFolderDefinition, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    let mut folders = storage
        .fetch_search_folders_by_ids(account.account_id, &[search_folder_id])
        .await
        .map_err(internal_error)?;
    folders
        .pop()
        .ok_or((StatusCode::NOT_FOUND, "search folder not found".to_string()))
}

async fn upsert_search_folder_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    request: UpsertSearchFolderRequest,
) -> std::result::Result<SearchFolderDefinition, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .upsert_search_folder(UpsertSearchFolderInput {
            id: request.id,
            account_id: account.account_id,
            display_name: request.display_name,
            result_object_kind: request.result_object_kind,
            scope_json: request.scope,
            restriction_json: request.restriction,
            excluded_folder_roles: request.excluded_folder_roles,
        })
        .await
        .map_err(bad_request_error)
}

async fn delete_search_folder_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
    search_folder_id: Uuid,
) -> std::result::Result<(), (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .delete_search_folder(account.account_id, search_folder_id)
        .await
        .map_err(bad_request_error)
}

async fn outlook_profile_state_with_store<S: ClientOutlookStore>(
    storage: &S,
    headers: &HeaderMap,
) -> std::result::Result<OutlookProfileState, (StatusCode, String)> {
    let account = require_account_from_store(storage, headers).await?;
    storage
        .fetch_outlook_profile_state(account.account_id)
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
mod tests;
