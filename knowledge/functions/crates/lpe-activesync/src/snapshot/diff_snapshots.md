---
type: Rust Function
title: diff_snapshots
resource: crates/lpe-activesync/src/snapshot.rs#L463-L493
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/snapshot_fingerprints
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
---

# Signature

`pub(crate) fn diff_snapshots(previous: Option<&Value>, current: &Value) -> Vec<SnapshotChange>`

# Calls

- [snapshot_fingerprints](../../../../../functions/crates/lpe-activesync/src/snapshot/snapshot_fingerprints.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_folder_sync](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)