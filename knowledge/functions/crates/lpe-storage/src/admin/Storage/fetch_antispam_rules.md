---
type: Rust Method
title: fetch_antispam_rules
resource: crates/lpe-storage/src/admin.rs#L1415-L1417
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`async fn fetch_antispam_rules(&self) -> Result<Vec<FilterRule>>`

# Called by

- [fetch_admin_dashboard](../../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)