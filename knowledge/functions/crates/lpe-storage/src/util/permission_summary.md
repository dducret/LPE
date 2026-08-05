---
type: Rust Function
title: permission_summary
resource: crates/lpe-storage/src/util.rs#L116-L118
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/create_server_administrator
  - functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session
---

# Signature

`pub(crate) fn permission_summary(permissions: &[String]) -> String`

# Called by

- [create_server_administrator](../../../../../functions/crates/lpe-storage/src/admin/Storage/create_server_administrator.md)
- [find_server_administrator_by_email](../../../../../functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email.md)
- [fetch_server_administrators](../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators.md)
- [fetch_admin_login](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)
- [fetch_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)