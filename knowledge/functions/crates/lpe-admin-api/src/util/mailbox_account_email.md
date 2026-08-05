---
type: Rust Function
title: mailbox_account_email
resource: crates/lpe-admin-api/src/util.rs#L42-L56
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/console/create_pst_transfer_job
  - functions/crates/lpe-admin-api/src/console/upload_pst_import
---

# Signature

`pub(crate) fn mailbox_account_email( dashboard: &AdminDashboard, mailbox_id: Uuid, ) -> Option<String>`

# Called by

- [create_pst_transfer_job](../../../../../functions/crates/lpe-admin-api/src/console/create_pst_transfer_job.md)
- [upload_pst_import](../../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)