---
type: Rust Function
title: journal_entry_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L322-L393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string
  - functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string_value
  - functions/crates/lpe-exchange/src/mapi/properties/notes/contact_names_from_link_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row
---

# Signature

`pub(in crate::mapi) fn journal_entry_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &JournalEntry, properties: &HashMap<u32, MapiValue>, ) -> UpsertJournalEntryInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [json_from_mapi_multi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string.md)
- [json_from_mapi_multi_string_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/json_from_mapi_multi_string_value.md)
- [contact_names_from_link_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/contact_names_from_link_name.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_canonical_journal_entry_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values.md)
- [mapi_note_and_journal_inputs_preserve_canonical_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields.md)
- [serialize_pending_journal_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row.md)