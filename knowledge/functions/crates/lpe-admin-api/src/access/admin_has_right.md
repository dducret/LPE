---
type: Rust Function
title: admin_has_right
resource: crates/lpe-admin-api/src/access.rs#L48-L53
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`fn admin_has_right(admin: &AuthenticatedAdmin, right: &str) -> bool`

# Called by

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)