---
type: Rust Function
title: apply_canonical_message_property_values
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L841-L911
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_is_empty
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_message_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, message_id: u64, values: Vec<(u32, MapiValue)>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [message_followup_update_from_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [message_followup_update_is_empty](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_is_empty.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)