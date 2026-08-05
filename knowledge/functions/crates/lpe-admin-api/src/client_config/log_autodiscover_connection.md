---
type: Rust Function
title: log_autodiscover_connection
resource: crates/lpe-admin-api/src/client_config.rs#L967-L1058
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json
---

# Signature

`fn log_autodiscover_connection( method: &str, uri: &Uri, headers: &HeaderMap, endpoints: &PublishedEndpoints, email: Option<&str>, response_kind: &str, request_body_bytes: usize, response: &Response, request_body: Option<&[u8]>, response_body: Option<&str>, error: Option<&str>, )`

# Calls

- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [trace_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection.md)

# Called by

- [outlook_autodiscover_get](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get.md)
- [outlook_autodiscover_post](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [outlook_autodiscover_json](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json.md)