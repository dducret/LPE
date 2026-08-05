---
type: Rust Function
title: serialize_pending_note_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L163-L189
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/notes/default_note_for_mapping
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_note_row( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [note_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi.md)
- [default_note_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/default_note_for_mapping.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [serialize_note_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)