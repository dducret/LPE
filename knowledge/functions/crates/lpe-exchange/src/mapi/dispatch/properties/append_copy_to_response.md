---
type: Rust Function
title: append_copy_to_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L1108-L1185
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_to_null_destination_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_to_excluded_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response
---

# Signature

`pub(super) async fn append_copy_to_response<S>( store: &S, principal: &AccountPrincipal, session: &MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [move_copy_target_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [rop_copy_to_null_destination_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_to_null_destination_response.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [copy_all_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request.md)
- [copy_to_excluded_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/copy_to_excluded_property_tags.md)
- [rop_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_properties_response.md)
- [copy_all_message_followup_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/copy_all_message_followup_property_values_for_request.md)
- [unsupported_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/unsupported_rop_response.md)

# Called by

- [append_stream_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response.md)