---
type: Rust Module
title: sync
resource: crates/lpe-exchange/src/mapi/sync.rs#L1-L1468
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-rop
  - external/super-session
  - external/super-tables
  - external/super
  - external/crate-mapi-properties
  - external/crate-mapi-wire-ropid
  - external/lpe-storage-searchfolderdefinition
  - external/pub-in-crate-mapi-use-super-identity-archive-folder-id-calendar-folder-id-common-views-folder-id-conflicts-folder-id-contacts-folder-id-contacts-search-folder-id-conversation-action-settings-folder-id-conversation-history-folder-id-deferred-action-folder-id-document-libraries-folder-id-drafts-folder-id-freebusy-data-folder-id-im-contact-list-folder-id-inbox-folder-id-ipm-subtree-folder-id-journal-folder-id-junk-folder-id-local-failures-folder-id-notes-folder-id-outbox-folder-id-public-folders-root-folder-id-quick-contacts-folder-id-quick-step-settings-folder-id-reminders-folder-id-root-folder-id-rss-feeds-folder-id-schedule-folder-id-search-folder-id-sent-folder-id-server-failures-folder-id-shortcuts-folder-id-spooler-queue-folder-id-store-replica-id-suggested-contacts-folder-id-sync-issues-folder-id-tasks-folder-id-todo-search-folder-id-tracked-mail-processing-folder-id-trash-folder-id-views-folder-id
  - external/associated-config-associated-config-direct-fast-transfer-object-associated-config-sync-object
  - external/pub-in-crate-mapi-use-responses
  - external/pub-in-crate-mapi-use-scope
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [sync_emails_for](../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for.md)
- [normal_message_sync_facts_for](../../../../../functions/crates/lpe-exchange/src/mapi/sync/normal_message_sync_facts_for.md)
- [sync_checkpoint_kind](../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_kind.md)
- [sync_checkpoint_mailbox_id](../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id.md)
- [changed_sync_mailboxes](../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_sync_mailboxes.md)
- [changed_sync_emails](../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_sync_emails.md)
- [special_sync_objects_for](../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [populate_special_message_named_property_definitions](../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)
- [special_message_with_named_property_definitions](../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_message_with_named_property_definitions.md)
- [common_views_sync_messages](../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_messages.md)
- [sync_object_projected_to_folder](../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_object_projected_to_folder.md)
- [public_folder_item_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/public_folder_item_sync_object.md)
- [contact_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/contact_sync_object.md)
- [task_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/task_sync_object.md)
- [changed_special_sync_objects](../../../../../functions/crates/lpe-exchange/src/mapi/sync/changed_special_sync_objects.md)
- [journal_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object.md)
- [navigation_shortcut_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/navigation_shortcut_sync_object.md)
- [common_views_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)
- [search_folder_definition_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)
- [common_view_named_view_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [conversation_action_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/conversation_action_sync_object.md)
- [delegate_freebusy_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/delegate_freebusy_sync_object.md)
- [special_message_property_value](../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_message_property_value.md)
- [calendar_sync_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)
- [sync_attachment_facts_for](../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [fast_transfer_message_children](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_message_children.md)
- [fast_transfer_manifest_for_object](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [message_for_id](../../../../../functions/crates/lpe-exchange/src/mapi/sync/message_for_id.md)
- [mapi_item_id_matches](../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)
- [next_pending_attachment_num](../../../../../functions/crates/lpe-exchange/src/mapi/sync/next_pending_attachment_num.md)
- [email_matches_folder](../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)

# Imports

- `super::rop::*`
- `super::session::*`
- `super::tables::*`
- `super::*`
- `crate::mapi::properties::*`
- `crate::mapi::wire::RopId`
- `lpe_storage::SearchFolderDefinition`
- `pub(in crate::mapi) use super::identity::{
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID,
    DRAFTS_FOLDER_ID, FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID,
    NOTES_FOLDER_ID, OUTBOX_FOLDER_ID, PUBLIC_FOLDERS_ROOT_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID,
    QUICK_STEP_SETTINGS_FOLDER_ID, REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID,
    SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID, SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID,
    SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID, STORE_REPLICA_ID, SUGGESTED_CONTACTS_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
}`
- `associated_config::{
    associated_config_direct_fast_transfer_object, associated_config_sync_object,
}`
- `pub(in crate::mapi) use responses::*`
- `pub(in crate::mapi) use scope::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)