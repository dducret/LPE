---
type: Rust Function
title: should_log_outlook_http_route_gap
resource: crates/lpe-admin-api/src/observability.rs#L149-L171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/observability/observe_http
---

# Signature

`fn should_log_outlook_http_route_gap( path: &str, user_agent: &str, status: u16, matched_route: bool, ) -> bool`

# Called by

- [observe_http](../../../../../functions/crates/lpe-admin-api/src/observability/observe_http.md)