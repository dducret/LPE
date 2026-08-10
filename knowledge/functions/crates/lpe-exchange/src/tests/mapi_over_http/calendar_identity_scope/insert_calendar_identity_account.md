---
type: Rust Function
title: insert_calendar_identity_account
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope.rs#L33-L55
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
---

# Signature

`async fn insert_calendar_identity_account( storage: &Storage, owner_account_id: Uuid, account_id: Uuid, ) -> anyhow::Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)

# Called by

- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)