---
type: Rust Function
title: classify_client_submission_storage_error
resource: crates/lpe-admin-api/src/workspace.rs#L1395-L1416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/internal_error
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/tests/client_submission_storage_errors_keep_actionable_status_codes
---

# Signature

`fn classify_client_submission_storage_error(error: anyhow::Error) -> (StatusCode, String)`

# Calls

- [internal_error](../../../../../functions/crates/lpe-admin-api/src/http/internal_error.md)

# Called by

- [client_submission_storage_errors_keep_actionable_status_codes](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/client_submission_storage_errors_keep_actionable_status_codes.md)