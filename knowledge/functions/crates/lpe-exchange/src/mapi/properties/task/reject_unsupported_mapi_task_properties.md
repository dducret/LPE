---
type: Rust Function
title: reject_unsupported_mapi_task_properties
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L358-L385
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values
---

# Signature

`fn reject_unsupported_mapi_task_properties(properties: &HashMap<u32, MapiValue>) -> Result<()>`

# Called by

- [apply_canonical_task_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values.md)