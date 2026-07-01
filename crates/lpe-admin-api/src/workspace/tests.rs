
use super::{
    classify_client_submission_storage_error, delete_client_note_with_store,
    delete_journal_entry_with_store, delete_search_folder_with_store, get_client_note_with_store,
    get_journal_entry_with_store, get_search_folder_with_store, list_client_notes_with_store,
    list_journal_entries_with_store, list_recoverable_items_with_store,
    list_search_folders_with_store, map_submit_message_request, map_update_message_flag_request,
    outlook_profile_state_with_store, purge_recoverable_item_with_store,
    query_client_reminders_with_store, resolve_client_sender_fields,
    restore_recoverable_item_with_store, submit_message_with_store, update_message_flag_with_store,
    upsert_client_note_with_store, upsert_journal_entry_with_store,
    upsert_search_folder_with_store,
};
use crate::types::{
    RecoverableItemsQueryRequest, ReminderQueryRequest, RestoreRecoverableItemRequest,
    SubmitMessageRequest, UpdateMessageFlagRequest, UpsertClientNoteRequest,
    UpsertJournalEntryRequest, UpsertSearchFolderRequest,
};
use axum::http::{HeaderMap, HeaderValue};
use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, ClientNote, ClientReminder, JmapEmail, JmapEmailAddress,
    JmapEmailFollowupUpdate, JmapEmailMailboxState, JournalEntry, MailboxAccountAccess,
    OutlookProfileState, RecoverableItem, ReminderQuery, SearchFolderDefinition,
    SubmitMessageInput, SubmittedMessage, UpsertClientNoteInput, UpsertJournalEntryInput,
    UpsertSearchFolderInput,
};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct FakeSubmissionStore {
    session: Option<AuthenticatedAccount>,
    accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
    submitted: Arc<Mutex<Vec<SubmitMessageInput>>>,
    audits: Arc<Mutex<Vec<AuditEntryInput>>>,
    flag_updates: Arc<Mutex<Vec<FlagUpdate>>>,
}

#[derive(Clone)]
struct FlagUpdate {
    account_id: Uuid,
    message_id: Uuid,
    update: JmapEmailFollowupUpdate,
    audit: AuditEntryInput,
}

#[derive(Clone)]
struct FakeOutlookStore {
    session: Option<AuthenticatedAccount>,
    notes: Arc<Mutex<Vec<ClientNote>>>,
    journal_entries: Arc<Mutex<Vec<JournalEntry>>>,
    reminders: Arc<Mutex<Vec<ClientReminder>>>,
    search_folders: Arc<Mutex<Vec<SearchFolderDefinition>>>,
    recoverable_items: Arc<Mutex<Vec<RecoverableItem>>>,
    note_inputs: Arc<Mutex<Vec<UpsertClientNoteInput>>>,
    journal_inputs: Arc<Mutex<Vec<UpsertJournalEntryInput>>>,
    search_folder_inputs: Arc<Mutex<Vec<UpsertSearchFolderInput>>>,
    deleted_notes: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    deleted_journal_entries: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    deleted_search_folders: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    reminder_queries: Arc<Mutex<Vec<(Uuid, bool)>>>,
    recoverable_queries: Arc<Mutex<Vec<(Uuid, Option<String>)>>>,
    restored_recoverable_items: Arc<Mutex<Vec<(Uuid, Uuid, Option<Uuid>, AuditEntryInput)>>>,
    purged_recoverable_items: Arc<Mutex<Vec<(Uuid, Uuid, AuditEntryInput)>>>,
}

impl Default for FakeOutlookStore {
    fn default() -> Self {
        Self {
            session: Some(account()),
            notes: Arc::new(Mutex::new(vec![note()])),
            journal_entries: Arc::new(Mutex::new(vec![journal_entry()])),
            reminders: Arc::new(Mutex::new(vec![reminder()])),
            search_folders: Arc::new(Mutex::new(vec![search_folder()])),
            recoverable_items: Arc::new(Mutex::new(vec![recoverable_item()])),
            note_inputs: Arc::new(Mutex::new(Vec::new())),
            journal_inputs: Arc::new(Mutex::new(Vec::new())),
            search_folder_inputs: Arc::new(Mutex::new(Vec::new())),
            deleted_notes: Arc::new(Mutex::new(Vec::new())),
            deleted_journal_entries: Arc::new(Mutex::new(Vec::new())),
            deleted_search_folders: Arc::new(Mutex::new(Vec::new())),
            reminder_queries: Arc::new(Mutex::new(Vec::new())),
            recoverable_queries: Arc::new(Mutex::new(Vec::new())),
            restored_recoverable_items: Arc::new(Mutex::new(Vec::new())),
            purged_recoverable_items: Arc::new(Mutex::new(Vec::new())),
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

    async fn update_jmap_email_followup_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail> {
        let flagged = update.flagged.unwrap_or(false);
        self.flag_updates.lock().unwrap().push(FlagUpdate {
            account_id,
            message_id,
            update,
            audit,
        });
        Ok(jmap_email(message_id, account_id, flagged))
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

    async fn upsert_client_note(&self, input: UpsertClientNoteInput) -> anyhow::Result<ClientNote> {
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

    async fn fetch_journal_entries(&self, account_id: Uuid) -> anyhow::Result<Vec<JournalEntry>> {
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

    async fn delete_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> anyhow::Result<()> {
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

    async fn fetch_search_folders(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<Vec<SearchFolderDefinition>> {
        Ok(self
            .search_folders
            .lock()
            .unwrap()
            .iter()
            .filter(|folder| folder.account_id == account_id)
            .cloned()
            .collect())
    }

    async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> anyhow::Result<Vec<SearchFolderDefinition>> {
        Ok(self
            .search_folders
            .lock()
            .unwrap()
            .iter()
            .filter(|folder| folder.account_id == account_id && ids.contains(&folder.id))
            .cloned()
            .collect())
    }

    async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> anyhow::Result<SearchFolderDefinition> {
        self.search_folder_inputs
            .lock()
            .unwrap()
            .push(input.clone());
        let folder = SearchFolderDefinition {
            id: input.id.unwrap_or_else(search_folder_id),
            account_id: input.account_id,
            role: "custom".to_string(),
            display_name: input.display_name,
            definition_kind: "user_saved".to_string(),
            result_object_kind: input.result_object_kind,
            scope_json: input.scope_json,
            restriction_json: input.restriction_json,
            excluded_folder_roles: input.excluded_folder_roles,
            is_builtin: false,
        };
        self.search_folders.lock().unwrap().push(folder.clone());
        Ok(folder)
    }

    async fn delete_search_folder(
        &self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> anyhow::Result<()> {
        self.deleted_search_folders
            .lock()
            .unwrap()
            .push((account_id, search_folder_id));
        self.search_folders
            .lock()
            .unwrap()
            .retain(|folder| folder.id != search_folder_id);
        Ok(())
    }

    async fn fetch_outlook_profile_state(
        &self,
        account_id: Uuid,
    ) -> anyhow::Result<OutlookProfileState> {
        Ok(outlook_profile_state(account_id))
    }
}

#[allow(async_fn_in_trait)]
impl super::ClientRecoverableStore for FakeOutlookStore {
    async fn list_recoverable_items(
        &self,
        account_id: Uuid,
        recoverable_folder: Option<&str>,
    ) -> anyhow::Result<Vec<RecoverableItem>> {
        self.recoverable_queries
            .lock()
            .unwrap()
            .push((account_id, recoverable_folder.map(str::to_string)));
        Ok(self
            .recoverable_items
            .lock()
            .unwrap()
            .iter()
            .filter(|item| {
                account_id == account().account_id
                    && recoverable_folder
                        .map(|folder| item.recoverable_folder == folder)
                        .unwrap_or(true)
            })
            .cloned()
            .collect())
    }

    async fn restore_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> anyhow::Result<JmapEmail> {
        self.restored_recoverable_items.lock().unwrap().push((
            account_id,
            recoverable_item_id,
            target_mailbox_id,
            audit,
        ));
        Ok(jmap_email(recoverable_item_id, account_id, false))
    }

    async fn purge_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> anyhow::Result<()> {
        self.purged_recoverable_items.lock().unwrap().push((
            account_id,
            recoverable_item_id,
            audit,
        ));
        Ok(())
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

fn search_folder_id() -> Uuid {
    Uuid::parse_str("dddddddd-1111-1111-1111-dddddddddddd").unwrap()
}

fn recoverable_item_id() -> Uuid {
    Uuid::parse_str("eeeeeeee-1111-1111-1111-eeeeeeeeeeee").unwrap()
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
        occurrence_start_at: None,
        title: "Reminder".to_string(),
        due_at: None,
        reminder_at: "2026-05-19T09:00:00Z".to_string(),
        dismissed_at: None,
        completed_at: None,
        status: "due".to_string(),
    }
}

fn search_folder() -> SearchFolderDefinition {
    SearchFolderDefinition {
        id: search_folder_id(),
        account_id: account_id(),
        role: "custom".to_string(),
        display_name: "Follow Up Mail".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "text", "query": "follow"}),
        excluded_folder_roles: vec!["trash".to_string()],
        is_builtin: false,
    }
}

fn recoverable_item() -> RecoverableItem {
    RecoverableItem {
        id: recoverable_item_id(),
        message_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
        source_mailbox_message_id: Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap(),
        source_mailbox_id: Uuid::parse_str("33333333-4444-5555-6666-777777777777").unwrap(),
        source_imap_uid: 42,
        recoverable_folder: "deletions".to_string(),
        delete_kind: "hard_delete".to_string(),
        status: "active".to_string(),
        deleted_at: "2026-05-20T10:00:00Z".to_string(),
        retained_until: None,
        legal_hold: false,
        subject: "Deleted message".to_string(),
        sender_address: "from@example.test".to_string(),
        received_at: "2026-05-20T09:00:00Z".to_string(),
        size_octets: 1024,
        has_attachments: false,
    }
}

fn outlook_profile_state(account_id: Uuid) -> OutlookProfileState {
    OutlookProfileState {
        id: "profile".to_string(),
        account_id,
        messages_backed_by_canonical_mailbox: true,
        contacts_backed_by_canonical_store: true,
        calendars_backed_by_canonical_store: true,
        tasks_backed_by_canonical_store: true,
        notes_backed_by_canonical_store: true,
        journals_backed_by_canonical_store: true,
        search_folders_count: 1,
        rules_count: 1,
        sender_identities_count: 1,
        mapi_named_properties_count: 2,
        mapi_custom_properties_count: 3,
        mapi_navigation_shortcuts_count: 4,
        mapi_sync_checkpoints_count: 5,
        mapi_profile_settings_present: true,
        ipm_subtree_ost_id_present: true,
        ipm_subtree_ost_id_size_octets: 16,
        profile_settings_updated_at: Some("2026-05-20T10:00:00Z".to_string()),
        unsupported_client_local_state: vec!["client_local_ost_cache".to_string()],
    }
}

fn jmap_email(id: Uuid, account_id: Uuid, flagged: bool) -> JmapEmail {
    let mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    JmapEmail {
        id,
        thread_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 1,
            unread: false,
            flagged,
            followup_flag_status: if flagged { "flagged" } else { "none" }.to_string(),
            followup_icon: if flagged { 6 } else { 0 },
            todo_item_flags: if flagged { 1 } else { 0 },
            followup_request: if flagged { "Follow up" } else { "" }.to_string(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 1,
        received_at: "2026-05-19T09:00:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "direct".to_string(),
        submitted_by_account_id: account_id,
        to: vec![JmapEmailAddress {
            address: "delegate@example.test".to_string(),
            display_name: Some("Delegate".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Subject".to_string(),
        preview: "Preview".to_string(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged,
        followup_flag_status: if flagged { "flagged" } else { "none" }.to_string(),
        followup_icon: if flagged { 6 } else { 0 },
        todo_item_flags: if flagged { 1 } else { 0 },
        followup_request: if flagged { "Follow up" } else { "" }.to_string(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 32,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
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
        flag_updates: Arc::new(Mutex::new(Vec::new())),
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
async fn update_message_flag_handler_uses_canonical_flag_store_path() {
    let authenticated = account();
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeSubmissionStore {
        session: Some(authenticated.clone()),
        accessible_mailbox_accounts: vec![owned_mailbox_access(&authenticated)],
        submitted: Arc::new(Mutex::new(Vec::new())),
        audits: Arc::new(Mutex::new(Vec::new())),
        flag_updates: Arc::new(Mutex::new(Vec::new())),
    };

    update_message_flag_with_store(
        &store,
        &bearer_headers(),
        message_id,
        UpdateMessageFlagRequest {
            flagged: true,
            completed: None,
            due_at: None,
            clear_due: None,
            reminder_at: None,
            clear_reminder: None,
        },
    )
    .await
    .unwrap();

    let updates = store.flag_updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].account_id, authenticated.account_id);
    assert_eq!(updates[0].message_id, message_id);
    assert_eq!(updates[0].update.unread, None);
    assert_eq!(updates[0].update.flagged, Some(true));
    assert_eq!(
        updates[0].update.followup_flag_status.as_deref(),
        Some("flagged")
    );
    assert_eq!(updates[0].audit.actor, authenticated.email);
    assert_eq!(updates[0].audit.action, "client-update-message-flag");
}

#[test]
fn update_message_flag_request_maps_complete_and_clear_states() {
    let complete = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: true,
        completed: Some(true),
        due_at: None,
        clear_due: None,
        reminder_at: None,
        clear_reminder: None,
    });
    assert_eq!(complete.flagged, Some(true));
    assert_eq!(complete.followup_flag_status.as_deref(), Some("complete"));
    assert_eq!(complete.followup_icon, Some(6));
    assert_eq!(complete.todo_item_flags, Some(8));

    let clear = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: false,
        completed: Some(true),
        due_at: None,
        clear_due: None,
        reminder_at: None,
        clear_reminder: None,
    });
    assert_eq!(clear.flagged, Some(false));
    assert_eq!(clear.followup_flag_status.as_deref(), Some("none"));
    assert_eq!(clear.followup_icon, Some(0));
    assert_eq!(clear.todo_item_flags, Some(0));
}

#[test]
fn update_message_flag_request_maps_due_date_controls() {
    let due = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: true,
        completed: None,
        due_at: Some("2026-05-20T23:59:59Z".to_string()),
        clear_due: None,
        reminder_at: None,
        clear_reminder: None,
    });
    assert_eq!(due.followup_flag_status.as_deref(), Some("flagged"));
    assert_eq!(
        due.followup_start_at.as_deref(),
        Some("2026-05-20T23:59:59Z")
    );
    assert_eq!(due.followup_due_at.as_deref(), Some("2026-05-20T23:59:59Z"));

    let clear_due = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: true,
        completed: None,
        due_at: None,
        clear_due: Some(true),
        reminder_at: None,
        clear_reminder: None,
    });
    assert_eq!(clear_due.followup_flag_status.as_deref(), Some("flagged"));
    assert_eq!(clear_due.followup_start_at.as_deref(), Some(""));
    assert_eq!(clear_due.followup_due_at.as_deref(), Some(""));
}

#[test]
fn update_message_flag_request_maps_reminder_controls() {
    let reminder = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: true,
        completed: None,
        due_at: None,
        clear_due: None,
        reminder_at: Some("2026-05-20T12:00:00Z".to_string()),
        clear_reminder: None,
    });
    assert_eq!(reminder.reminder_set, Some(true));
    assert_eq!(
        reminder.reminder_at.as_deref(),
        Some("2026-05-20T12:00:00Z")
    );
    assert_eq!(reminder.reminder_dismissed_at, None);

    let clear_reminder = map_update_message_flag_request(&UpdateMessageFlagRequest {
        flagged: true,
        completed: None,
        due_at: None,
        clear_due: None,
        reminder_at: None,
        clear_reminder: Some(true),
    });
    assert_eq!(clear_reminder.reminder_set, Some(false));
    assert_eq!(clear_reminder.reminder_at.as_deref(), Some(""));
    assert_eq!(clear_reminder.reminder_dismissed_at.as_deref(), Some(""));
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
async fn search_folder_api_helpers_cover_authenticated_crud_path() {
    let store = FakeOutlookStore::default();
    let headers = bearer_headers();

    let listed = list_search_folders_with_store(&store, &headers)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    let fetched = get_search_folder_with_store(&store, &headers, search_folder_id())
        .await
        .unwrap();
    assert_eq!(fetched.display_name, "Follow Up Mail");

    let created = upsert_search_folder_with_store(
        &store,
        &headers,
        UpsertSearchFolderRequest {
            id: None,
            display_name: "Unread from Alice".to_string(),
            result_object_kind: "message".to_string(),
            scope: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction: serde_json::json!({"kind": "text", "query": "alice"}),
            excluded_folder_roles: vec!["trash".to_string()],
        },
    )
    .await
    .unwrap();

    assert_eq!(created.display_name, "Unread from Alice");
    let recorded = store.search_folder_inputs.lock().unwrap();
    assert_eq!(recorded[0].account_id, account_id());
    assert_eq!(recorded[0].result_object_kind, "message");
    assert_eq!(recorded[0].excluded_folder_roles, vec!["trash".to_string()]);
    drop(recorded);

    delete_search_folder_with_store(&store, &headers, search_folder_id())
        .await
        .unwrap();
    assert_eq!(
        store.deleted_search_folders.lock().unwrap().as_slice(),
        &[(account_id(), search_folder_id())]
    );
}

#[tokio::test]
async fn recoverable_items_api_helpers_use_canonical_store_path() {
    let store = FakeOutlookStore::default();
    let headers = bearer_headers();
    let target_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();

    let listed = list_recoverable_items_with_store(
        &store,
        &headers,
        RecoverableItemsQueryRequest {
            folder: Some("deletions".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, recoverable_item_id());
    assert_eq!(
        store.recoverable_queries.lock().unwrap().as_slice(),
        &[(account_id(), Some("deletions".to_string()))]
    );

    let restored = restore_recoverable_item_with_store(
        &store,
        &headers,
        recoverable_item_id(),
        RestoreRecoverableItemRequest {
            target_mailbox_id: Some(target_mailbox_id),
        },
    )
    .await
    .unwrap();
    assert_eq!(restored.id, recoverable_item_id());
    let restored_records = store.restored_recoverable_items.lock().unwrap();
    assert_eq!(restored_records.len(), 1);
    assert_eq!(restored_records[0].0, account_id());
    assert_eq!(restored_records[0].1, recoverable_item_id());
    assert_eq!(restored_records[0].2, Some(target_mailbox_id));
    assert_eq!(restored_records[0].3.action, "restore-recoverable-message");
    drop(restored_records);

    let _ = purge_recoverable_item_with_store(&store, &headers, recoverable_item_id())
        .await
        .unwrap();
    let purged_records = store.purged_recoverable_items.lock().unwrap();
    assert_eq!(purged_records.len(), 1);
    assert_eq!(purged_records[0].0, account_id());
    assert_eq!(purged_records[0].1, recoverable_item_id());
    assert_eq!(purged_records[0].2.action, "purge-recoverable-message");
}

#[tokio::test]
async fn outlook_profile_api_helper_reads_canonical_profile_state() {
    let store = FakeOutlookStore::default();
    let profile = outlook_profile_state_with_store(&store, &bearer_headers())
        .await
        .unwrap();

    assert_eq!(profile.id, "profile");
    assert_eq!(profile.account_id, account_id());
    assert!(profile.messages_backed_by_canonical_mailbox);
    assert_eq!(profile.search_folders_count, 1);
    assert_eq!(profile.rules_count, 1);
    assert!(profile.ipm_subtree_ost_id_present);
    assert!(profile
        .unsupported_client_local_state
        .contains(&"client_local_ost_cache".to_string()));
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
