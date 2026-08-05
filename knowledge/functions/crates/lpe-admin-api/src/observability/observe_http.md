---
type: Rust Function
title: observe_http
resource: crates/lpe-admin-api/src/observability.rs#L88-L146
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-admin-api/src/observability/should_log_outlook_http_route_gap
---

# Signature

`pub async fn observe_http(mut request: Request, next: Next) -> Response`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [should_log_outlook_http_route_gap](../../../../../functions/crates/lpe-admin-api/src/observability/should_log_outlook_http_route_gap.md)