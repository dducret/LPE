---
type: Rust Function
title: storage_policy_error
resource: crates/lpe-admin-api/src/storage.rs#L388-L404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/bad_request_error
  - functions/crates/lpe-admin-api/src/http/internal_error
  called_by:
  - functions/crates/lpe-admin-api/src/storage/storage_policy_errors_map_validation_to_bad_request
---

# Signature

`fn storage_policy_error(error: anyhow::Error) -> (StatusCode, String)`

# Calls

- [bad_request_error](../../../../../functions/crates/lpe-admin-api/src/http/bad_request_error.md)
- [internal_error](../../../../../functions/crates/lpe-admin-api/src/http/internal_error.md)

# Called by

- [storage_policy_errors_map_validation_to_bad_request](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_errors_map_validation_to_bad_request.md)