---
type: Rust Function
title: default_permissions_for_role
resource: crates/lpe-storage/src/util.rs#L120-L167
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/shared/built_in_role_permissions_include_dashboard
  - functions/crates/lpe-storage/src/util/normalize_admin_permissions
---

# Signature

`pub(crate) fn default_permissions_for_role(role: &str) -> Vec<String>`

# Called by

- [built_in_role_permissions_include_dashboard](../../../../../functions/crates/lpe-storage/src/shared/built_in_role_permissions_include_dashboard.md)
- [normalize_admin_permissions](../../../../../functions/crates/lpe-storage/src/util/normalize_admin_permissions.md)