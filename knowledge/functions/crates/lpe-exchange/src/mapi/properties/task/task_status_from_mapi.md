---
type: Rust Function
title: task_status_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L285-L334
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi
---

# Signature

`fn task_status_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [task_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi.md)