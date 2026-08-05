---
type: Rust Function
title: classify_submission_storage_error
resource: crates/lpe-admin-api/src/integration.rs#L481-L502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/internal_error
---

# Signature

`fn classify_submission_storage_error(error: anyhow::Error) -> (StatusCode, String)`

# Calls

- [internal_error](../../../../../functions/crates/lpe-admin-api/src/http/internal_error.md)