---
type: Rust Method
title: create_server_administrator
resource: crates/lpe-storage/src/admin.rs#L35-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_id
  - functions/crates/lpe-storage/src/util/normalize_admin_permissions
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/util/permission_summary
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
---

# Signature

`pub async fn create_server_administrator( &self, input: NewServerAdministrator, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_domain_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_id.md)
- [normalize_admin_permissions](../../../../../../functions/crates/lpe-storage/src/util/normalize_admin_permissions.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [permission_summary](../../../../../../functions/crates/lpe-storage/src/util/permission_summary.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)