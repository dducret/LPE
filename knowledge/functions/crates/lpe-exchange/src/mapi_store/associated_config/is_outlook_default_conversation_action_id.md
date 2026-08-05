---
type: Rust Function
title: is_outlook_default_conversation_action_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L240-L242
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete
  - functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle
---

# Signature

`pub(crate) fn is_outlook_default_conversation_action_id(item_id: u64) -> bool`

# Called by

- [stage_virtual_conversation_action_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_virtual_conversation_action_property_delete](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete.md)
- [unresolved_mapi_object_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [is_expected_unbacked_mapi_object](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)
- [add_object_ids_for_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle.md)