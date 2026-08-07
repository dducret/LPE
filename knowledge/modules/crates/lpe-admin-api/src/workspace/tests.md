---
type: Rust Module
title: tests
resource: crates/lpe-admin-api/src/workspace/tests.rs#L1-L1208
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-classify-client-submission-storage-error-delete-client-note-with-store-delete-journal-entry-with-store-delete-search-folder-with-store-get-client-note-with-store-get-journal-entry-with-store-get-search-folder-with-store-list-client-notes-with-store-list-journal-entries-with-store-list-recoverable-items-with-store-list-search-folders-with-store-map-submit-message-request-map-update-message-flag-request-outlook-profile-state-with-store-purge-recoverable-item-with-store-query-client-reminders-with-store-resolve-client-mailbox-access-resolve-client-sender-fields-restore-recoverable-item-with-store-submit-message-with-store-update-message-flag-with-store-upsert-client-note-with-store-upsert-journal-entry-with-store-upsert-search-folder-with-store
  - external/crate-types-recoverableitemsqueryrequest-reminderqueryrequest-restorerecoverableitemrequest-submitmessagerequest-updatemessageflagrequest-upsertclientnoterequest-upsertjournalentryrequest-upsertsearchfolderrequest
  - external/axum-http-headermap-headervalue
  - external/lpe-storage-auditentryinput-authenticatedaccount-clientnote-clientreminder-jmapemail-jmapemailaddress-jmapemailfollowupupdate-jmapemailmailboxstate-journalentry-mailboxaccountaccess-outlookprofilestate-recoverableitem-reminderquery-searchfolderdefinition-submitmessageinput-submittedmessage-upsertclientnoteinput-upsertjournalentryinput-upsertsearchfolderinput
  - external/std-sync-arc-mutex
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [FakeSubmissionStore](../../../../../classes/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore.md)
- [FlagUpdate](../../../../../classes/crates/lpe-admin-api/src/workspace/tests/FlagUpdate.md)
- [FakeOutlookStore](../../../../../classes/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore.md)
- [default](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/default/default.md)
- [fetch_account_session](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsessionstore/fetch_account_session.md)
- [fetch_accessible_mailbox_accounts](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsubmissionstore/fetch_accessible_mailbox_accounts.md)
- [submit_message](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsubmissionstore/submit_message.md)
- [update_jmap_email_followup_flags](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeSubmissionStore/super-clientsubmissionstore/update_jmap_email_followup_flags.md)
- [fetch_account_session](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientsessionstore/fetch_account_session.md)
- [fetch_client_notes](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_client_notes.md)
- [fetch_client_notes_by_ids](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_client_notes_by_ids.md)
- [upsert_client_note](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/upsert_client_note.md)
- [delete_client_note](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/delete_client_note.md)
- [fetch_journal_entries](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_journal_entries.md)
- [fetch_journal_entries_by_ids](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_journal_entries_by_ids.md)
- [upsert_journal_entry](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/upsert_journal_entry.md)
- [delete_journal_entry](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/delete_journal_entry.md)
- [query_client_reminders](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/query_client_reminders.md)
- [fetch_search_folders](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_search_folders.md)
- [fetch_search_folders_by_ids](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_search_folders_by_ids.md)
- [upsert_search_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/upsert_search_folder.md)
- [delete_search_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/delete_search_folder.md)
- [fetch_outlook_profile_state](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientoutlookstore/fetch_outlook_profile_state.md)
- [list_recoverable_items](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientrecoverablestore/list_recoverable_items.md)
- [restore_recoverable_item](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientrecoverablestore/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/FakeOutlookStore/super-clientrecoverablestore/purge_recoverable_item.md)
- [account](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/account.md)
- [account_id](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/account_id.md)
- [note_id](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/note_id.md)
- [journal_entry_id](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_entry_id.md)
- [search_folder_id](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/search_folder_id.md)
- [recoverable_item_id](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/recoverable_item_id.md)
- [note](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/note.md)
- [journal_entry](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_entry.md)
- [reminder](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/reminder.md)
- [search_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/search_folder.md)
- [recoverable_item](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/recoverable_item.md)
- [outlook_profile_state](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/outlook_profile_state.md)
- [jmap_email](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/jmap_email.md)
- [submit_request](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/submit_request.md)
- [owned_mailbox_access](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/owned_mailbox_access.md)
- [bearer_headers](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/bearer_headers.md)
- [delegated_send_on_behalf_defaults_sender_to_authenticated_account](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_on_behalf_defaults_sender_to_authenticated_account.md)
- [delegated_send_as_without_explicit_sender_keeps_sender_empty](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_as_without_explicit_sender_keeps_sender_empty.md)
- [explicit_sender_fields_are_preserved](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/explicit_sender_fields_are_preserved.md)
- [client_submission_storage_errors_keep_actionable_status_codes](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/client_submission_storage_errors_keep_actionable_status_codes.md)
- [map_submit_message_request_preserves_web_submission_source](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/map_submit_message_request_preserves_web_submission_source.md)
- [submit_message_handler_uses_canonical_submission_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/submit_message_handler_uses_canonical_submission_store_path.md)
- [delegated_mailbox_access_requires_a_canonical_grant](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_mailbox_access_requires_a_canonical_grant.md)
- [delegated_mailbox_access_returns_the_canonical_rights](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_mailbox_access_returns_the_canonical_rights.md)
- [update_message_flag_handler_uses_canonical_flag_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_handler_uses_canonical_flag_store_path.md)
- [update_message_flag_request_maps_complete_and_clear_states](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_complete_and_clear_states.md)
- [update_message_flag_request_maps_due_date_controls](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_due_date_controls.md)
- [update_message_flag_request_maps_reminder_controls](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/update_message_flag_request_maps_reminder_controls.md)
- [notes_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/notes_api_helpers_cover_authenticated_crud_path.md)
- [journal_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/journal_api_helpers_cover_authenticated_crud_path.md)
- [search_folder_api_helpers_cover_authenticated_crud_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/search_folder_api_helpers_cover_authenticated_crud_path.md)
- [recoverable_items_api_helpers_use_canonical_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/recoverable_items_api_helpers_use_canonical_store_path.md)
- [outlook_profile_api_helper_reads_canonical_profile_state](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/outlook_profile_api_helper_reads_canonical_profile_state.md)
- [reminder_api_helper_preserves_include_inactive_query](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/reminder_api_helper_preserves_include_inactive_query.md)

# Imports

- `super::{
    classify_client_submission_storage_error, delete_client_note_with_store,
    delete_journal_entry_with_store, delete_search_folder_with_store, get_client_note_with_store,
    get_journal_entry_with_store, get_search_folder_with_store, list_client_notes_with_store,
    list_journal_entries_with_store, list_recoverable_items_with_store,
    list_search_folders_with_store, map_submit_message_request, map_update_message_flag_request,
    outlook_profile_state_with_store, purge_recoverable_item_with_store,
    query_client_reminders_with_store, resolve_client_mailbox_access, resolve_client_sender_fields,
    restore_recoverable_item_with_store, submit_message_with_store, update_message_flag_with_store,
    upsert_client_note_with_store, upsert_journal_entry_with_store,
    upsert_search_folder_with_store,
}`
- `crate::types::{
    RecoverableItemsQueryRequest, ReminderQueryRequest, RestoreRecoverableItemRequest,
    SubmitMessageRequest, UpdateMessageFlagRequest, UpsertClientNoteRequest,
    UpsertJournalEntryRequest, UpsertSearchFolderRequest,
}`
- `axum::http::{HeaderMap, HeaderValue}`
- `lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, ClientNote, ClientReminder, JmapEmail, JmapEmailAddress,
    JmapEmailFollowupUpdate, JmapEmailMailboxState, JournalEntry, MailboxAccountAccess,
    OutlookProfileState, RecoverableItem, ReminderQuery, SearchFolderDefinition,
    SubmitMessageInput, SubmittedMessage, UpsertClientNoteInput, UpsertJournalEntryInput,
    UpsertSearchFolderInput,
}`
- `std::sync::{Arc, Mutex}`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../../packages/crates/lpe-admin-api.md)