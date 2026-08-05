---
type: Rust Function
title: record_http_request
resource: crates/lpe-admin-api/src/observability.rs#L227-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
---

# Signature

`fn record_http_request(method: &str, route: &str, status: u16, elapsed: Duration)`

# Calls

- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)