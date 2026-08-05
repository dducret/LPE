---
type: Rust Method
title: update_settings
resource: crates/lpe-storage/src/admin.rs#L778-L956
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  called_by:
  - functions/crates/lpe-admin-api/src/console/update_server_settings
  - functions/crates/lpe-admin-api/src/console/update_security_settings
  - functions/crates/lpe-admin-api/src/console/update_local_ai_settings
---

# Signature

`pub async fn update_settings( &self, update: DashboardUpdate, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)

# Called by

- [update_server_settings](../../../../../../functions/crates/lpe-admin-api/src/console/update_server_settings.md)
- [update_security_settings](../../../../../../functions/crates/lpe-admin-api/src/console/update_security_settings.md)
- [update_local_ai_settings](../../../../../../functions/crates/lpe-admin-api/src/console/update_local_ai_settings.md)