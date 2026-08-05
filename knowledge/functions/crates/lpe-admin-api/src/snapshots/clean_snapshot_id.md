---
type: Rust Function
title: clean_snapshot_id
resource: crates/lpe-admin-api/src/snapshots.rs#L238-L251
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/load_snapshot
---

# Signature

`fn clean_snapshot_id(snapshot_id: &str) -> anyhow::Result<String>`

# Called by

- [load_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/load_snapshot.md)