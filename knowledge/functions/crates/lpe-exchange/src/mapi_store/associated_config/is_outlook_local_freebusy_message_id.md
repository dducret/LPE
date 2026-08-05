---
type: Rust Function
title: is_outlook_local_freebusy_message_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L244-L246
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/open_message_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/ensure_virtual_local_freebusy_message
---

# Signature

`pub(crate) fn is_outlook_local_freebusy_message_id(item_id: u64) -> bool`

# Called by

- [open_message_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/open_message_folder_id.md)
- [stage_delegate_freebusy_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values.md)
- [property_stream_data](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [unresolved_mapi_object_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [is_expected_unbacked_mapi_object](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)
- [serialize_freebusy_row_staged](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged.md)
- [delegate_freebusy_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/delegate_freebusy_property_value.md)
- [ensure_virtual_local_freebusy_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/ensure_virtual_local_freebusy_message.md)