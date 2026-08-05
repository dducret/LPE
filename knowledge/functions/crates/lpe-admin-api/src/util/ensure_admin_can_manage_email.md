---
type: Rust Function
title: ensure_admin_can_manage_email
resource: crates/lpe-admin-api/src/util.rs#L23-L40
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/console/create_account
  - functions/crates/lpe-admin-api/src/console/update_account
  - functions/crates/lpe-admin-api/src/console/create_pst_transfer_job
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
  - functions/crates/lpe-admin-api/src/console/create_alias
---

# Signature

`pub(crate) fn ensure_admin_can_manage_email( admin: &AuthenticatedAdmin, email: &str, ) -> std::result::Result<(), (StatusCode, String)>`

# Called by

- [create_account](../../../../../functions/crates/lpe-admin-api/src/console/create_account.md)
- [update_account](../../../../../functions/crates/lpe-admin-api/src/console/update_account.md)
- [create_pst_transfer_job](../../../../../functions/crates/lpe-admin-api/src/console/create_pst_transfer_job.md)
- [upload_pst_import](../../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)
- [create_alias](../../../../../functions/crates/lpe-admin-api/src/console/create_alias.md)