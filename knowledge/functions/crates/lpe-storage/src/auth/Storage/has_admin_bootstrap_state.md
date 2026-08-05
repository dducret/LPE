---
type: Rust Method
title: has_admin_bootstrap_state
resource: crates/lpe-storage/src/auth.rs#L876-L907
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin
  - functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing
---

# Signature

`pub async fn has_admin_bootstrap_state(&self) -> Result<bool>`

# Called by

- [bootstrap_admin](../../../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)
- [auto_bootstrap_admin_if_missing](../../../../../../functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing.md)