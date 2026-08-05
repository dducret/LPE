---
type: Rust Function
title: object_map_len
resource: crates/lpe-jmap/src/service/helpers.rs#L632-L637
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error
  - functions/crates/lpe-jmap/src/service/helpers/set_object_count
---

# Signature

`pub(super) fn object_map_len(arguments: &Value, field: &str) -> Option<usize>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [method_object_limit_error](../../../../../../functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error.md)
- [set_object_count](../../../../../../functions/crates/lpe-jmap/src/service/helpers/set_object_count.md)