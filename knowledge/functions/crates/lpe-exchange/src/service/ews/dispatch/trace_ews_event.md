---
type: Rust Function
title: trace_ews_event
resource: crates/lpe-exchange/src/service/ews/dispatch.rs#L285-L336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`fn trace_ews_event( headers: &HeaderMap, principal: &AccountPrincipal, operation: &str, direction: OutlookTraceDirection, response_code: Option<&str>, payload: Option<&[u8]>, )`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_outlook_trace](../../../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)

# Called by

- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)