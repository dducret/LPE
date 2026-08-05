---
type: Rust Method
title: update_accessible_task
resource: crates/lpe-exchange/src/tests/mod.rs#L9384-L9414
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
---

# Signature

`fn update_accessible_task<'a>( &'a self, _principal_account_id: Uuid, task_id: Uuid, input: UpsertClientTaskInput, ) -> StoreFuture<'a, ClientTask>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [apply_canonical_task_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)