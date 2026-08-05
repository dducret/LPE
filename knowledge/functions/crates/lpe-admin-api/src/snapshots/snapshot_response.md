---
type: Rust Function
title: snapshot_response
resource: crates/lpe-admin-api/src/snapshots.rs#L203-L210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_dir
  - functions/crates/lpe-admin-api/src/snapshots/read_snapshots
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/list_snapshots
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
---

# Signature

`fn snapshot_response() -> anyhow::Result<SnapshotListResponse>`

# Calls

- [snapshot_dir](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_dir.md)
- [read_snapshots](../../../../../functions/crates/lpe-admin-api/src/snapshots/read_snapshots.md)

# Called by

- [list_snapshots](../../../../../functions/crates/lpe-admin-api/src/snapshots/list_snapshots.md)
- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [delete_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)