---
type: Rust Function
title: journal_entry_property_value
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L53-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_named_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry
  - functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row
---

# Signature

`pub(in crate::mapi) fn journal_entry_property_value( entry: &JournalEntry, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [journal_entry_named_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_named_property_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [journal_entry_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [restriction_matches_journal_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry.md)
- [collaboration_item_properties_project_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns.md)
- [journal_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/journal_sync_object.md)
- [serialize_journal_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row.md)