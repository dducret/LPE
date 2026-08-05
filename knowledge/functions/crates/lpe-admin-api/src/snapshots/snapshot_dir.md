---
type: Rust Function
title: snapshot_dir
resource: crates/lpe-admin-api/src/snapshots.rs#L262-L266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
  - functions/crates/lpe-admin-api/src/snapshots/load_snapshot
---

# Signature

`fn snapshot_dir() -> PathBuf`

# Called by

- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [delete_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)
- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)
- [load_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/load_snapshot.md)