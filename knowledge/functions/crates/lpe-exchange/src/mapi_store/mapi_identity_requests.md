---
type: Rust Function
title: mapi_identity_requests
resource: crates/lpe-exchange/src/mapi_store.rs#L744-L878
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`fn mapi_identity_requests( mailboxes: &[JmapMailbox], emails: &[JmapEmail], contact_collections: &[CollaborationCollection], calendar_collections: &[CollaborationCollection], task_collections: &[CollaborationCollection], contacts: &[AccessibleContact], events: &[AccessibleEvent], deleted_events: &[AccessibleEvent], tasks: &[ClientTask], notes: &[ClientNote], journal_entries: &[JournalEntry], search_folder_definitions: &[SearchFolderDefinition], rules: &[MailboxRule], navigation_shortcuts: &[MapiNavigationShortcutRecord], associated_configs: &[MapiAssociatedConfigRecord], conversation_actions: &[ConversationAction], delegate_freebusy_messages: &[DelegateFreeBusyMessageObject], public_folders: &[PublicFolder], public_folder_items: &[PublicFolderItem], ) -> Vec<MapiIdentityRequest>`

# Calls

- [mapi_folder_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests.md)
- [collaboration_folder_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests.md)

# Called by

- [load_mapi_mail_store](../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)