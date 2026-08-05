---
type: Rust Function
title: split_permissions
resource: crates/lpe-storage/src/util.rs#L246-L251
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/util/normalize_admin_permissions
---

# Signature

`fn split_permissions(raw: &str) -> Vec<String>`

# Called by

- [normalize_admin_permissions](../../../../../functions/crates/lpe-storage/src/util/normalize_admin_permissions.md)