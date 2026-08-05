---
type: Rust Function
title: permissions_from_storage
resource: crates/lpe-storage/src/util.rs#L76-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/normalize_admin_permissions
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session
---

# Signature

`pub(crate) fn permissions_from_storage( role: &str, rights_summary: Option<&str>, permissions_json: Option<&str>, ) -> Vec<String>`

# Calls

- [normalize_admin_permissions](../../../../../functions/crates/lpe-storage/src/util/normalize_admin_permissions.md)

# Called by

- [find_server_administrator_by_email](../../../../../functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email.md)
- [fetch_server_administrators](../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators.md)
- [fetch_admin_login](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)
- [fetch_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)