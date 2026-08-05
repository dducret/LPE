---
type: Rust Function
title: observe_http
resource: LPE-CT/src/observability.rs#L81-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/LPE-CT/src/host_logs/HostLogError/status
---

# Signature

`pub async fn observe_http(mut request: Request, next: Next) -> Response`

# Calls

- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)