---
type: Rust Function
title: is_outlook_inbox_default_associated_config_id
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L160-L175
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin
---

# Signature

`pub(crate) fn is_outlook_inbox_default_associated_config_id(item_id: u64) -> bool`

# Called by

- [unresolved_mapi_object_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/unresolved_mapi_object_scope.md)
- [is_expected_unbacked_mapi_object](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)
- [add_object_ids_for_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle.md)
- [fai_debug_item_classification](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification.md)
- [fai_debug_state_origin](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin.md)