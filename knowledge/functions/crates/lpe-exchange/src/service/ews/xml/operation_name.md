---
type: Rust Function
title: operation_name
resource: crates/lpe-exchange/src/service/ews/xml.rs#L80-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) fn operation_name(body: &str) -> Option<String>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [ews_operation_hint](../../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint.md)
- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)