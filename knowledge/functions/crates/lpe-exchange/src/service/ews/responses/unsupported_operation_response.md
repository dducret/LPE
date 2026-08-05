---
type: Rust Function
title: unsupported_operation_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L295-L301
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
  - functions/crates/lpe-exchange/src/service/ews/responses/site_mailbox_operations_return_explicit_unsupported_errors
---

# Signature

`pub(in crate::service) fn unsupported_operation_response(operation: &str) -> String`

# Calls

- [operation_error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)
- [site_mailbox_operations_return_explicit_unsupported_errors](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/site_mailbox_operations_return_explicit_unsupported_errors.md)