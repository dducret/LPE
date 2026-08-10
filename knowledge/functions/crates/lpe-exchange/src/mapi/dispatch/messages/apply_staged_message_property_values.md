---
type: Rust Function
title: apply_staged_message_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L387-L440
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn apply_staged_message_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, message_id: u64, saved_email: Option<MapiSavedEmail>, pending_properties: HashMap<u32, MapiValue>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [unique_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id.md)
- [upsert_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)