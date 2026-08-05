---
type: Rust Module
title: diagnostics
resource: crates/lpe-exchange/src/service/ews/diagnostics.rs#L1-L131
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-headermap-uri-response-response
  - external/tracing-info-warn
  - external/super-super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [ews_operation_hint](../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_operation_hint.md)
- [log_ews_connection](../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/log_ews_connection.md)
- [EwsResponseDebug](../../../../../../classes/crates/lpe-exchange/src/service/ews/diagnostics/EwsResponseDebug.md)
- [ews_response_code](../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_code.md)
- [ews_response_debug_detail](../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_response_debug_detail.md)
- [ews_payload_debug_detail](../../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/ews_payload_debug_detail.md)

# Imports

- `axum::{
    http::{HeaderMap, Uri},
    response::Response,
}`
- `tracing::{info, warn}`
- `super::super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)