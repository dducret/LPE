---
type: Rust Function
title: conversation_id_from_index
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L388-L391
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties
---

# Signature

`pub(in crate::mapi) fn conversation_id_from_index(value: &[u8]) -> Option<Uuid>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [conversation_action_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/conversation_action_from_mapi_properties.md)