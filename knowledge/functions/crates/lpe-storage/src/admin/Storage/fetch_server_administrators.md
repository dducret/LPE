---
type: Rust Method
title: fetch_server_administrators
resource: crates/lpe-storage/src/admin.rs#L1371-L1413
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/permissions_from_storage
  - functions/crates/lpe-storage/src/util/permission_summary
  called_by:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`async fn fetch_server_administrators(&self) -> Result<Vec<ServerAdministrator>>`

# Calls

- [permissions_from_storage](../../../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)
- [permission_summary](../../../../../../functions/crates/lpe-storage/src/util/permission_summary.md)

# Called by

- [fetch_admin_dashboard](../../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)