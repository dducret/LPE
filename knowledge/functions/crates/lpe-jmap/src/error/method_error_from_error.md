---
type: Rust Function
title: method_error_from_error
resource: crates/lpe-jmap/src/error.rs#L60-L67
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/error/method_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) fn method_error_from_error(error: Error) -> Value`

# Calls

- [method_error](../../../../../functions/crates/lpe-jmap/src/error/method_error.md)

# Called by

- [handle_api_request_for_account](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)