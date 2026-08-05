---
type: Rust Function
title: read_snapshots
resource: crates/lpe-admin-api/src/snapshots.rs#L212-L228
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/snapshot_response
---

# Signature

`fn read_snapshots(dir: &Path) -> anyhow::Result<Vec<SnapshotMetadata>>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [snapshot_response](../../../../../functions/crates/lpe-admin-api/src/snapshots/snapshot_response.md)