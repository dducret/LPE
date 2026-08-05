---
type: Rust Function
title: append_property_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/property_dispatch.rs#L45-L137
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/echo_input_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/continue_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/stop_with_echo_input_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_property_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], created_emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, response_size_limit: usize, responses: &mut Vec<u8>, ) -> PropertyDispatchFlow where S: ExchangeStore,`

# Calls

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [echo_input_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/echo_input_handle_table.md)
- [append_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response.md)
- [continue_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/continue_batch.md)
- [append_get_properties_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [stop_with_echo_input_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/PropertyDispatchFlow/stop_with_echo_input_handle_table.md)
- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)