---
type: Rust Function
title: note_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/notes.rs#L273-L301
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row
---

# Signature

`pub(in crate::mapi) fn note_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &ClientNote, properties: &HashMap<u32, MapiValue>, ) -> UpsertClientNoteInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_canonical_note_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values.md)
- [mapi_note_and_journal_inputs_preserve_canonical_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_note_and_journal_inputs_preserve_canonical_fields.md)
- [serialize_pending_note_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_note_row.md)