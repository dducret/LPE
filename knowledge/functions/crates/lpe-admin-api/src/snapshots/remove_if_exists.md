---
type: Rust Function
title: remove_if_exists
resource: crates/lpe-admin-api/src/snapshots.rs#L296-L302
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/delete_snapshot
---

# Signature

`fn remove_if_exists(path: &Path) -> anyhow::Result<()>`

# Called by

- [delete_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/delete_snapshot.md)