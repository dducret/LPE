---
type: Rust Function
title: pending_page
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L86-L93
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`pub(super) fn pending_page( changes: &[SnapshotChange], offset: usize, window_size: u64, ) -> (Vec<SnapshotChange>, usize)`

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)