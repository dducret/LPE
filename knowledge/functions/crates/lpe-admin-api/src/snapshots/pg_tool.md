---
type: Rust Function
title: pg_tool
resource: crates/lpe-admin-api/src/snapshots.rs#L288-L294
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
---

# Signature

`fn pg_tool(name: &str) -> String`

# Called by

- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)