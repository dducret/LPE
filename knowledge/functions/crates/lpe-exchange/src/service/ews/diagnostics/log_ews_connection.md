---
type: Rust Function
title: log_ews_connection
resource: crates/lpe-exchange/src/service/ews/diagnostics.rs#L15-L74
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-exchange/src/service/post_handler
---

# Signature

`pub(in crate::service) fn log_ews_connection( uri: &Uri, headers: &HeaderMap, request_body_bytes: usize, operation: &str, ews_response_code: Option<&str>, response: &Response, duration_ms: f64, error: Option<&str>, debug_detail: Option<&str>, )`

# Calls

- [status](../../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [post_handler](../../../../../../../functions/crates/lpe-exchange/src/service/post_handler.md)