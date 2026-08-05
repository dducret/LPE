---
type: Rust Method
title: fetch_security_settings
resource: crates/lpe-storage/src/admin.rs#L1174-L1330
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`async fn fetch_security_settings(&self) -> Result<SecuritySettings>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_admin_dashboard](../../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)