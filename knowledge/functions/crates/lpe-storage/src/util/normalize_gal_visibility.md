---
type: Rust Function
title: normalize_gal_visibility
resource: crates/lpe-storage/src/util.rs#L189-L195
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/update_account
---

# Signature

`pub(crate) fn normalize_gal_visibility(value: &str) -> Result<String>`

# Called by

- [create_account](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account.md)
- [update_account](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/update_account.md)