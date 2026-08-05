---
type: Rust Method
title: fetch_admin_session
resource: crates/lpe-storage/src/auth.rs#L1064-L1134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/permissions_from_storage
  - functions/crates/lpe-storage/src/util/permission_summary
  called_by:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/logout
---

# Signature

`pub async fn fetch_admin_session(&self, token: &str) -> Result<Option<AuthenticatedAdmin>>`

# Calls

- [permissions_from_storage](../../../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)
- [permission_summary](../../../../../../functions/crates/lpe-storage/src/util/permission_summary.md)

# Called by

- [require_admin](../../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [login](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [logout](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/logout.md)