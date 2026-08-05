---
type: Rust Method
title: fetch_local_ai_settings
resource: crates/lpe-storage/src/admin.rs#L1332-L1360
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

`async fn fetch_local_ai_settings(&self) -> Result<LocalAiSettings>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_admin_dashboard](../../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)