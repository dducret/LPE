---
type: Rust Function
title: decode_ews_body
resource: crates/lpe-exchange/src/service/ews/xml.rs#L32-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/ews/xml/decode_utf16_body
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) fn decode_ews_body(headers: &HeaderMap, body: &[u8]) -> Result<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [decode_utf16_body](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/decode_utf16_body.md)

# Called by

- [ews_operation_hint](../../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint.md)
- [handle](../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)