---
type: Rust Function
title: default_note_for_mapping
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L244-L254
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_named_properties_project_canonical_values
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row
---

# Signature

`pub(in crate::mapi) fn default_note_for_mapping() -> ClientNote`

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [collaboration_item_properties_project_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns.md)
- [microsoft_oxprops_message_size_projects_integer32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property.md)
- [mapi_note_and_journal_inputs_preserve_canonical_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields.md)
- [mapi_note_and_journal_named_properties_project_canonical_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_named_properties_project_canonical_values.md)
- [serialize_pending_note_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row.md)