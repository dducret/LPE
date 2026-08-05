---
type: Rust Function
title: method_object_limit_error
resource: crates/lpe-jmap/src/service/helpers.rs#L518-L626
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/object_array_len
  - functions/crates/lpe-jmap/src/service/helpers/set_object_count
  - functions/crates/lpe-jmap/src/service/helpers/object_map_len
  - functions/crates/lpe-jmap/src/error/method_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(super) fn method_object_limit_error(method_name: &str, arguments: &Value) -> Option<Value>`

# Calls

- [object_array_len](../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_array_len.md)
- [set_object_count](../../../../../../functions/crates/lpe-jmap/src/service/helpers/set_object_count.md)
- [object_map_len](../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_map_len.md)
- [method_error](../../../../../../functions/crates/lpe-jmap/src/error/method_error.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)