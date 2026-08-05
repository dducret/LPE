---
type: Rust Function
title: bad_request_error
resource: crates/lpe-admin-api/src/http.rs#L8-L10
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/upsert_calendar_collection_grant
  - functions/crates/lpe-admin-api/src/delegation/get_free_busy
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_not_found
  - functions/crates/lpe-admin-api/src/storage/storage_policy_error
---

# Signature

`pub(crate) fn bad_request_error(error: impl ToString) -> (StatusCode, String)`

# Called by

- [upsert_calendar_collection_grant](../../../../../functions/crates/lpe-admin-api/src/delegation/upsert_calendar_collection_grant.md)
- [get_free_busy](../../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)
- [snapshot_not_found](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_not_found.md)
- [storage_policy_error](../../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_error.md)