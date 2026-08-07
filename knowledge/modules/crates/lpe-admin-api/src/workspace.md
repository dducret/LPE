---
type: Rust Module
title: workspace
resource: crates/lpe-admin-api/src/workspace.rs#L1-L1503
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-query-state-http-headermap-statuscode-json
  - external/lpe-storage-accessiblecontact-auditentryinput-authenticatedaccount-clientcontact-clientevent-clientnote-clientreminder-clienttask-clienttasklist-clientworkspace-collaborationcollection-healthresponse-jmapemail-jmapemailfollowupupdate-journalentry-mailboxaccountaccess-outlookprofilestate-recipientsuggestion-recoverableitem-reminderquery-saveddraftmessage-searchfolderdefinition-storage-submitmessageinput-submittedmessage-submittedrecipientinput-upsertclientcontactinput-upsertclienteventinput-upsertclientnoteinput-upsertclienttaskinput-upsertjournalentryinput-upsertsearchfolderinput
  - external/tracing-info
  - external/uuid-uuid
  - external/crate-http-bad-request-error-internal-error-observability-require-account-types-apiresult-patchclientcontactrequest-recipientsuggestionquery-recoverableitemsqueryrequest-reminderqueryrequest-restorerecoverableitemrequest-submitmessagerequest-submitrecipientrequest-updatemessageflagrequest-upsertclientcontactrequest-upsertclienteventrequest-upsertclientnoterequest-upsertclienttaskrequest-upsertjournalentryrequest-upsertsearchfolderrequest
  - external/pub-crate-use-public-folders
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [ClientSessionStore](../../../../interfaces/crates/lpe-admin-api/src/workspace/ClientSessionStore.md)
- [ClientSubmissionStore](../../../../interfaces/crates/lpe-admin-api/src/workspace/ClientSubmissionStore.md)
- [fetch_account_session](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientsessionstore/fetch_account_session.md)
- [ClientOutlookStore](../../../../interfaces/crates/lpe-admin-api/src/workspace/ClientOutlookStore.md)
- [ClientRecoverableStore](../../../../interfaces/crates/lpe-admin-api/src/workspace/ClientRecoverableStore.md)
- [fetch_accessible_mailbox_accounts](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientsubmissionstore/fetch_accessible_mailbox_accounts.md)
- [submit_message](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientsubmissionstore/submit_message.md)
- [update_jmap_email_followup_flags](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientsubmissionstore/update_jmap_email_followup_flags.md)
- [fetch_client_notes](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_client_notes.md)
- [fetch_client_notes_by_ids](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_client_notes_by_ids.md)
- [upsert_client_note](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/upsert_client_note.md)
- [delete_client_note](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/delete_client_note.md)
- [fetch_journal_entries](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_journal_entries.md)
- [fetch_journal_entries_by_ids](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_journal_entries_by_ids.md)
- [upsert_journal_entry](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/upsert_journal_entry.md)
- [delete_journal_entry](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/delete_journal_entry.md)
- [query_client_reminders](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/query_client_reminders.md)
- [fetch_search_folders](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_search_folders.md)
- [fetch_search_folders_by_ids](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_search_folders_by_ids.md)
- [upsert_search_folder](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/upsert_search_folder.md)
- [delete_search_folder](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/delete_search_folder.md)
- [fetch_outlook_profile_state](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientoutlookstore/fetch_outlook_profile_state.md)
- [list_recoverable_items](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientrecoverablestore/list_recoverable_items.md)
- [restore_recoverable_item](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientrecoverablestore/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../functions/crates/lpe-admin-api/src/workspace/Storage/clientrecoverablestore/purge_recoverable_item.md)
- [client_workspace](../../../../functions/crates/lpe-admin-api/src/workspace/client_workspace.md)
- [submit_message](../../../../functions/crates/lpe-admin-api/src/workspace/submit_message.md)
- [submit_message_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)
- [save_draft_message](../../../../functions/crates/lpe-admin-api/src/workspace/save_draft_message.md)
- [update_message_flag](../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag.md)
- [update_message_flag_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store.md)
- [map_update_message_flag_request](../../../../functions/crates/lpe-admin-api/src/workspace/map_update_message_flag_request.md)
- [delete_draft_message](../../../../functions/crates/lpe-admin-api/src/workspace/delete_draft_message.md)
- [list_recoverable_items](../../../../functions/crates/lpe-admin-api/src/workspace/list_recoverable_items.md)
- [list_recoverable_items_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/list_recoverable_items_with_store.md)
- [restore_recoverable_item](../../../../functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item.md)
- [restore_recoverable_item_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item_with_store.md)
- [purge_recoverable_item](../../../../functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item.md)
- [purge_recoverable_item_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item_with_store.md)
- [upsert_client_contact](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_contact.md)
- [list_contact_books](../../../../functions/crates/lpe-admin-api/src/workspace/list_contact_books.md)
- [list_client_contacts](../../../../functions/crates/lpe-admin-api/src/workspace/list_client_contacts.md)
- [get_client_contact](../../../../functions/crates/lpe-admin-api/src/workspace/get_client_contact.md)
- [patch_client_contact](../../../../functions/crates/lpe-admin-api/src/workspace/patch_client_contact.md)
- [delete_client_contact](../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_contact.md)
- [query_recipient_suggestions](../../../../functions/crates/lpe-admin-api/src/workspace/query_recipient_suggestions.md)
- [dismiss_recipient_suggestion](../../../../functions/crates/lpe-admin-api/src/workspace/dismiss_recipient_suggestion.md)
- [contact_input_from_request](../../../../functions/crates/lpe-admin-api/src/workspace/contact_input_from_request.md)
- [client_contact_from_accessible](../../../../functions/crates/lpe-admin-api/src/workspace/client_contact_from_accessible.md)
- [upsert_client_event](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_event.md)
- [preserve_empty](../../../../functions/crates/lpe-admin-api/src/workspace/preserve_empty.md)
- [delete_client_event](../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_event.md)
- [list_client_tasks](../../../../functions/crates/lpe-admin-api/src/workspace/list_client_tasks.md)
- [list_client_task_lists](../../../../functions/crates/lpe-admin-api/src/workspace/list_client_task_lists.md)
- [get_client_task](../../../../functions/crates/lpe-admin-api/src/workspace/get_client_task.md)
- [upsert_client_task](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_task.md)
- [delete_client_task](../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_task.md)
- [list_client_notes](../../../../functions/crates/lpe-admin-api/src/workspace/list_client_notes.md)
- [get_client_note](../../../../functions/crates/lpe-admin-api/src/workspace/get_client_note.md)
- [upsert_client_note](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_note.md)
- [delete_client_note](../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_note.md)
- [list_journal_entries](../../../../functions/crates/lpe-admin-api/src/workspace/list_journal_entries.md)
- [get_journal_entry](../../../../functions/crates/lpe-admin-api/src/workspace/get_journal_entry.md)
- [upsert_journal_entry](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry.md)
- [delete_journal_entry](../../../../functions/crates/lpe-admin-api/src/workspace/delete_journal_entry.md)
- [query_client_reminders](../../../../functions/crates/lpe-admin-api/src/workspace/query_client_reminders.md)
- [list_search_folders](../../../../functions/crates/lpe-admin-api/src/workspace/list_search_folders.md)
- [get_search_folder](../../../../functions/crates/lpe-admin-api/src/workspace/get_search_folder.md)
- [upsert_search_folder](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_search_folder.md)
- [delete_search_folder](../../../../functions/crates/lpe-admin-api/src/workspace/delete_search_folder.md)
- [outlook_profile_state](../../../../functions/crates/lpe-admin-api/src/workspace/outlook_profile_state.md)
- [list_client_notes_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/list_client_notes_with_store.md)
- [get_client_note_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/get_client_note_with_store.md)
- [upsert_client_note_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_note_with_store.md)
- [delete_client_note_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_note_with_store.md)
- [list_journal_entries_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/list_journal_entries_with_store.md)
- [get_journal_entry_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/get_journal_entry_with_store.md)
- [upsert_journal_entry_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry_with_store.md)
- [delete_journal_entry_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/delete_journal_entry_with_store.md)
- [query_client_reminders_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/query_client_reminders_with_store.md)
- [list_search_folders_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/list_search_folders_with_store.md)
- [get_search_folder_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/get_search_folder_with_store.md)
- [upsert_search_folder_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/upsert_search_folder_with_store.md)
- [delete_search_folder_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/delete_search_folder_with_store.md)
- [outlook_profile_state_with_store](../../../../functions/crates/lpe-admin-api/src/workspace/outlook_profile_state_with_store.md)
- [require_account_from_store](../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)
- [resolve_client_mailbox_access](../../../../functions/crates/lpe-admin-api/src/workspace/resolve_client_mailbox_access.md)
- [ensure_client_mailbox_write_access](../../../../functions/crates/lpe-admin-api/src/workspace/ensure_client_mailbox_write_access.md)
- [classify_client_submission_storage_error](../../../../functions/crates/lpe-admin-api/src/workspace/classify_client_submission_storage_error.md)
- [map_submit_message_request](../../../../functions/crates/lpe-admin-api/src/workspace/map_submit_message_request.md)
- [resolve_client_sender_fields](../../../../functions/crates/lpe-admin-api/src/workspace/resolve_client_sender_fields.md)
- [map_recipients](../../../../functions/crates/lpe-admin-api/src/workspace/map_recipients.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_storage::{
    AccessibleContact, AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent,
    ClientNote, ClientReminder, ClientTask, ClientTaskList, ClientWorkspace,
    CollaborationCollection, HealthResponse, JmapEmail, JmapEmailFollowupUpdate, JournalEntry,
    MailboxAccountAccess, OutlookProfileState, RecipientSuggestion, RecoverableItem, ReminderQuery,
    SavedDraftMessage, SearchFolderDefinition, Storage, SubmitMessageInput, SubmittedMessage,
    SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput, UpsertSearchFolderInput,
}`
- `tracing::info`
- `uuid::Uuid`
- `crate::{
    http::{bad_request_error, internal_error},
    observability, require_account,
    types::{
        ApiResult, PatchClientContactRequest, RecipientSuggestionQuery,
        RecoverableItemsQueryRequest, ReminderQueryRequest, RestoreRecoverableItemRequest,
        SubmitMessageRequest, SubmitRecipientRequest, UpdateMessageFlagRequest,
        UpsertClientContactRequest, UpsertClientEventRequest, UpsertClientNoteRequest,
        UpsertClientTaskRequest, UpsertJournalEntryRequest, UpsertSearchFolderRequest,
    },
}`
- `pub(crate) use public_folders::*`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)