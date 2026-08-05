---
type: Rust Function
title: serialize_pending_conversation_action_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L224-L241
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_conversation_action_row( properties: &HashMap<u32, MapiValue>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [conversation_action_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [serialize_conversation_action_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)