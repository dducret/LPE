---
type: Rust Function
title: snapshot_not_found
resource: crates/lpe-admin-api/src/snapshots.rs#L253-L260
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/bad_request_error
---

# Signature

`fn snapshot_not_found(error: anyhow::Error) -> (StatusCode, String)`

# Calls

- [bad_request_error](../../../../../functions/crates/lpe-admin-api/src/http/bad_request_error.md)