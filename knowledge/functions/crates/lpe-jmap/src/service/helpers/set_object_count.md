---
type: Rust Function
title: set_object_count
resource: crates/lpe-jmap/src/service/helpers.rs#L639-L644
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/object_map_len
  - functions/crates/lpe-jmap/src/service/helpers/object_array_len
  called_by:
  - functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error
---

# Signature

`pub(super) fn set_object_count(arguments: &Value) -> Option<usize>`

# Calls

- [object_map_len](../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_map_len.md)
- [object_array_len](../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_array_len.md)

# Called by

- [method_object_limit_error](../../../../../../functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error.md)