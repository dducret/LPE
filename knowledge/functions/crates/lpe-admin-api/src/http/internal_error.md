---
type: Rust Function
title: internal_error
resource: crates/lpe-admin-api/src/http.rs#L4-L6
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/classify_submission_storage_error
  - functions/crates/lpe-admin-api/src/storage/storage_policy_error
  - functions/crates/lpe-admin-api/src/workspace/classify_client_submission_storage_error
  - functions/crates/lpe-admin-api/src/workspace/mailbox_access/classify_client_mailbox_access_error
---

# Signature

`pub(crate) fn internal_error(error: impl ToString) -> (StatusCode, String)`

# Called by

- [classify_submission_storage_error](../../../../../functions/crates/lpe-admin-api/src/integration/classify_submission_storage_error.md)
- [storage_policy_error](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_error.md)
- [classify_client_submission_storage_error](../../../../../functions/crates/lpe-admin-api/src/workspace/classify_client_submission_storage_error.md)
- [classify_client_mailbox_access_error](../../../../../functions/crates/lpe-admin-api/src/workspace/mailbox_access/classify_client_mailbox_access_error.md)