---
type: Rust Function
title: note_property_value
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L3-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_named_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note
  - functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row
---

# Signature

`pub(in crate::mapi) fn note_property_value( note: &ClientNote, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [note_named_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_named_property_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [note_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [restriction_matches_note](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note.md)
- [collaboration_item_properties_project_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns.md)
- [serialize_note_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row.md)