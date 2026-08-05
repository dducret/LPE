---
type: Rust Function
title: snapshot_database_url
resource: crates/lpe-admin-api/src/snapshots.rs#L192-L201
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/database_url
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
---

# Signature

`fn snapshot_database_url(storage: &Storage) -> Result<String, (StatusCode, String)>`

# Calls

- [database_url](../../../../../functions/crates/lpe-storage/src/core/Storage/database_url.md)

# Called by

- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)