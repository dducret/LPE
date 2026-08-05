---
type: Rust Function
title: trace_autodiscover_connection
resource: crates/lpe-admin-api/src/client_config.rs#L1060-L1134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection
---

# Signature

`fn trace_autodiscover_connection( method: &str, uri: &Uri, headers: &HeaderMap, email: Option<&str>, response_kind: &str, request_body: Option<&[u8]>, response: &Response, response_body: Option<&str>, )`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [write_outlook_trace](../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [log_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)