---
type: Rust Function
title: apply_canonical_task_property_values
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L387-L429
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/task/reject_unsupported_mapi_task_properties
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_accessible_task
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_task_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, task_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [reject_unsupported_mapi_task_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/reject_unsupported_mapi_task_properties.md)
- [task_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi.md)
- [update_accessible_task](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_accessible_task.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)