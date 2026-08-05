---
type: Rust Module
title: folders
resource: crates/lpe-exchange/src/mapi/rop/debug/folders.rs#L1-L156
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-format-bytes-hex-format-property-value-shapes-for-debug-hex-preview-for-debug-mapi-object-debug-fields
  - external/crate-mapi-properties
  - external/crate-mapi-rop-canonical-property-storage-tag-special-folder-identification-property-value-accountprincipal-jmapemail-jmapmailbox-mapimailstoresnapshot-mapiobject-calendar-folder-id-common-views-folder-id-contacts-folder-id-drafts-folder-id-inbox-folder-id-ipm-subtree-folder-id-journal-folder-id-notes-folder-id-outbox-folder-id-reminders-folder-id-root-folder-id-search-folder-id-sent-folder-id-tasks-folder-id-trash-folder-id-views-folder-id
  - external/crate-mapi-sync-archive-folder-id
  - external/crate-mapi-mailstore
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [log_calendar_default_folder_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug.md)
- [default_folder_property_mappings_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mappings_for_debug.md)
- [default_folder_property_mapping_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mapping_for_debug.md)

# Imports

- `super::{
    format_bytes_hex, format_property_value_shapes_for_debug, hex_preview_for_debug,
    mapi_object_debug_fields,
}`
- `crate::mapi::properties::*`
- `crate::mapi::rop::{
    canonical_property_storage_tag, special_folder_identification_property_value, AccountPrincipal,
    JmapEmail, JmapMailbox, MapiMailStoreSnapshot, MapiObject, CALENDAR_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID, CONTACTS_FOLDER_ID, DRAFTS_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, NOTES_FOLDER_ID, OUTBOX_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, SEARCH_FOLDER_ID, SENT_FOLDER_ID, TASKS_FOLDER_ID,
    TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
}`
- `crate::mapi::sync::ARCHIVE_FOLDER_ID`
- `crate::mapi_mailstore`

# Member of

- [lpe-exchange](../../../../../../../packages/crates/lpe-exchange.md)