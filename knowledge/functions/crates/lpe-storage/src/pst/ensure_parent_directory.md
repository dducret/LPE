---
type: Rust Function
title: ensure_parent_directory
resource: crates/lpe-storage/src/pst.rs#L513-L521
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst
---

# Signature

`fn ensure_parent_directory(path: &str) -> Result<()>`

# Called by

- [export_mailbox_to_pst](../../../../../functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst.md)