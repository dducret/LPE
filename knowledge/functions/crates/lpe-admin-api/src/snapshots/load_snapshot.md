---
type: Rust Function
title: load_snapshot
resource: crates/lpe-admin-api/src/snapshots.rs#L230-L236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/snapshots/clean_snapshot_id
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_dir
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
---

# Signature

`fn load_snapshot(snapshot_id: &str) -> anyhow::Result<SnapshotMetadata>`

# Calls

- [clean_snapshot_id](../../../../../functions/crates/lpe-admin-api/src/snapshots/clean_snapshot_id.md)
- [snapshot_dir](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_dir.md)

# Called by

- [delete_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)