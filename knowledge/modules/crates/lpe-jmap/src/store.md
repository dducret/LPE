---
type: Rust Module
title: store
resource: crates/lpe-jmap/src/store.rs#L1-L1407
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-storage-accessiblecontact-accessibleevent-attachmentuploadinput-auditentryinput-authenticatedaccount-calendareventattachment-canonicalchangecategory-canonicalchangelistener-canonicalchangereplay-canonicalpushchangeset-clientnote-clientreminder-clienttask-clienttasklist-collaborationcollection-collaborationgrantinput-createtasklistinput-jmapemail-jmapemailfollowupupdate-jmapemailquery-jmapemailsubmission-jmapimportedemailinput-jmapmailobjectchange-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-jmapquota-jmapstoredquerystate-jmapstringobjectchange-jmapthreadquery-jmapuploadblob-journalentry-mailboxaccountaccess-mailboxdelegationgrantinput-mailboxrule-outlookprofilestate-recipientsuggestion-reminderquery-saveddraftmessage-searchfolderdefinition-senderdelegationgrantinput-senderidentity-sievescriptdocument-storage-submitmessageinput-submittedmessage-tasklistgrantinput-updatetasklistinput-upsertclientcontactinput-upsertclienteventinput-upsertclientnoteinput-upsertclienttaskinput-upsertjournalentryinput-upsertsearchfolderinput
  - external/serde-json-value
  - external/uuid-uuid
  - external/shares-parse-collaboration-kind-parse-sender-right-project-share-share-type-share-uuid
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [JmapShareInput](../../../../classes/crates/lpe-jmap/src/store/JmapShareInput.md)
- [JmapPushListener](../../../../interfaces/crates/lpe-jmap/src/store/JmapPushListener.md)
- [JmapStore](../../../../interfaces/crates/lpe-jmap/src/store/JmapStore.md)
- [fetch_jmap_mail_change_cursor](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_jmap_mail_change_cursor.md)
- [fetch_jmap_object_change_cursor](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_jmap_object_change_cursor.md)
- [replay_jmap_mail_object_changes](../../../../functions/crates/lpe-jmap/src/store/JmapStore/replay_jmap_mail_object_changes.md)
- [replay_jmap_object_changes](../../../../functions/crates/lpe-jmap/src/store/JmapStore/replay_jmap_object_changes.md)
- [replay_jmap_string_object_changes](../../../../functions/crates/lpe-jmap/src/store/JmapStore/replay_jmap_string_object_changes.md)
- [save_jmap_query_state](../../../../functions/crates/lpe-jmap/src/store/JmapStore/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_jmap_query_state.md)
- [fetch_jmap_message_blob](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_jmap_message_blob.md)
- [fetch_calendar_attachment_blob](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_calendar_attachment_blob.md)
- [update_jmap_task_reminder](../../../../functions/crates/lpe-jmap/src/store/JmapStore/update_jmap_task_reminder.md)
- [update_jmap_event_reminder](../../../../functions/crates/lpe-jmap/src/store/JmapStore/update_jmap_event_reminder.md)
- [update_jmap_mail_reminder](../../../../functions/crates/lpe-jmap/src/store/JmapStore/update_jmap_mail_reminder.md)
- [dismiss_jmap_reminder_occurrence](../../../../functions/crates/lpe-jmap/src/store/JmapStore/dismiss_jmap_reminder_occurrence.md)
- [fetch_jmap_shares](../../../../functions/crates/lpe-jmap/src/store/JmapStore/fetch_jmap_shares.md)
- [upsert_jmap_share](../../../../functions/crates/lpe-jmap/src/store/JmapStore/upsert_jmap_share.md)
- [delete_jmap_share](../../../../functions/crates/lpe-jmap/src/store/JmapStore/delete_jmap_share.md)
- [wait_for_change](../../../../functions/crates/lpe-jmap/src/store/CanonicalChangeListener/jmappushlistener/wait_for_change.md)
- [fetch_account_session](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_account_session.md)
- [create_push_listener](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_push_listener.md)
- [fetch_canonical_change_cursor](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_canonical_change_cursor.md)
- [fetch_jmap_mail_change_cursor](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_mail_change_cursor.md)
- [fetch_jmap_object_change_cursor](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_object_change_cursor.md)
- [replay_jmap_mail_object_changes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/replay_jmap_mail_object_changes.md)
- [replay_jmap_object_changes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/replay_jmap_object_changes.md)
- [replay_jmap_string_object_changes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/replay_jmap_string_object_changes.md)
- [save_jmap_query_state](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/save_jmap_query_state.md)
- [fetch_jmap_query_state](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_query_state.md)
- [replay_canonical_changes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/replay_canonical_changes.md)
- [fetch_jmap_mailboxes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_mailboxes.md)
- [fetch_accessible_mailbox_accounts](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_mailbox_accounts.md)
- [fetch_sender_identities](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_sender_identities.md)
- [fetch_jmap_mailbox_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_mailbox_ids.md)
- [create_jmap_mailbox](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_jmap_mailbox.md)
- [update_jmap_mailbox](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_mailbox.md)
- [destroy_jmap_mailbox](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/destroy_jmap_mailbox.md)
- [query_jmap_email_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/query_jmap_email_ids.md)
- [fetch_all_jmap_email_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_all_jmap_email_ids.md)
- [fetch_all_jmap_thread_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_all_jmap_thread_ids.md)
- [query_jmap_thread_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/query_jmap_thread_ids.md)
- [fetch_jmap_emails](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_emails.md)
- [fetch_jmap_emails_with_protected_bcc](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_emails_with_protected_bcc.md)
- [fetch_jmap_draft](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_draft.md)
- [fetch_jmap_email_submissions](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_email_submissions.md)
- [fetch_jmap_quota](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_quota.md)
- [list_mailbox_rules](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/list_mailbox_rules.md)
- [fetch_outlook_profile_state](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_outlook_profile_state.md)
- [fetch_search_folders](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_search_folders.md)
- [fetch_search_folders_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_search_folders_by_ids.md)
- [upsert_search_folder](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_search_folder.md)
- [delete_search_folder](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_search_folder.md)
- [fetch_active_sieve_script](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_active_sieve_script.md)
- [put_sieve_script](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/put_sieve_script.md)
- [set_active_sieve_script](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/set_active_sieve_script.md)
- [save_jmap_upload_blob](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/save_jmap_upload_blob.md)
- [fetch_jmap_upload_blob](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_upload_blob.md)
- [fetch_jmap_message_blob](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_message_blob.md)
- [fetch_calendar_attachment_blob](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_calendar_attachment_blob.md)
- [save_draft_message](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/save_draft_message.md)
- [delete_draft_message](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_draft_message.md)
- [submit_draft_message](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/submit_draft_message.md)
- [copy_jmap_email](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/copy_jmap_email.md)
- [copy_jmap_email_between_accounts](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/copy_jmap_email_between_accounts.md)
- [import_jmap_email](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/import_jmap_email.md)
- [fetch_accessible_contact_collections](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_contact_collections.md)
- [fetch_accessible_contacts](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_contacts.md)
- [fetch_accessible_contacts_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_contacts_by_ids.md)
- [create_accessible_contact](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_accessible_contact.md)
- [update_accessible_contact](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_accessible_contact.md)
- [delete_accessible_contact](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_accessible_contact.md)
- [query_recipient_suggestions](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/query_recipient_suggestions.md)
- [fetch_accessible_calendar_collections](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_calendar_collections.md)
- [create_accessible_calendar_collection](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_accessible_calendar_collection.md)
- [update_accessible_calendar_collection](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_accessible_calendar_collection.md)
- [delete_accessible_calendar_collection](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_accessible_calendar_collection.md)
- [fetch_accessible_events](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_events.md)
- [fetch_accessible_events_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_accessible_events_by_ids.md)
- [create_accessible_event](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_accessible_event.md)
- [update_accessible_event](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_accessible_event.md)
- [delete_accessible_event](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_accessible_event.md)
- [fetch_calendar_attachments_for_events](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_calendar_attachments_for_events.md)
- [add_calendar_event_attachment](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/add_calendar_event_attachment.md)
- [fetch_jmap_task_lists](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists.md)
- [fetch_jmap_task_lists_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_task_lists_by_ids.md)
- [create_jmap_task_list](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/create_jmap_task_list.md)
- [update_jmap_task_list](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_task_list.md)
- [delete_jmap_task_list](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_task_list.md)
- [fetch_jmap_tasks](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks.md)
- [fetch_jmap_tasks_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_tasks_by_ids.md)
- [upsert_jmap_task](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_task.md)
- [delete_jmap_task](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_task.md)
- [fetch_jmap_notes](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_notes.md)
- [fetch_jmap_notes_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_notes_by_ids.md)
- [upsert_jmap_note](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_note.md)
- [delete_jmap_note](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_note.md)
- [fetch_jmap_journal_entries](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_journal_entries.md)
- [fetch_jmap_journal_entries_by_ids](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_journal_entries_by_ids.md)
- [upsert_jmap_journal_entry](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_journal_entry.md)
- [delete_jmap_journal_entry](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_journal_entry.md)
- [query_jmap_reminders](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/query_jmap_reminders.md)
- [update_jmap_task_reminder](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_task_reminder.md)
- [update_jmap_event_reminder](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_event_reminder.md)
- [update_jmap_mail_reminder](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/update_jmap_mail_reminder.md)
- [dismiss_jmap_reminder_occurrence](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/dismiss_jmap_reminder_occurrence.md)
- [fetch_jmap_shares](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/fetch_jmap_shares.md)
- [upsert_jmap_share](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/upsert_jmap_share.md)
- [delete_jmap_share](../../../../functions/crates/lpe-jmap/src/store/Storage/jmapstore/delete_jmap_share.md)

# Imports

- `anyhow::Result`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, AttachmentUploadInput, AuditEntryInput,
    AuthenticatedAccount, CalendarEventAttachment, CanonicalChangeCategory,
    CanonicalChangeListener, CanonicalChangeReplay, CanonicalPushChangeSet, ClientNote,
    ClientReminder, ClientTask, ClientTaskList, CollaborationCollection, CollaborationGrantInput,
    CreateTaskListInput, JmapEmail, JmapEmailFollowupUpdate, JmapEmailQuery, JmapEmailSubmission,
    JmapImportedEmailInput, JmapMailObjectChange, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JmapQuota, JmapStoredQueryState, JmapStringObjectChange,
    JmapThreadQuery, JmapUploadBlob, JournalEntry, MailboxAccountAccess,
    MailboxDelegationGrantInput, MailboxRule, OutlookProfileState, RecipientSuggestion,
    ReminderQuery, SavedDraftMessage, SearchFolderDefinition, SenderDelegationGrantInput,
    SenderIdentity, SieveScriptDocument, Storage, SubmitMessageInput, SubmittedMessage,
    TaskListGrantInput, UpdateTaskListInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput, UpsertSearchFolderInput,
}`
- `serde_json::Value`
- `uuid::Uuid`
- `shares::{parse_collaboration_kind, parse_sender_right, project_share, share_type, share_uuid}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)