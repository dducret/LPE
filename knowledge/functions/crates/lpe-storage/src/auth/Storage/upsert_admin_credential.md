---
type: Rust Method
title: upsert_admin_credential
resource: crates/lpe-storage/src/auth.rs#L137-L168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  called_by:
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin
  - functions/crates/lpe-admin-api/src/console/create_server_administrator
---

# Signature

`pub async fn upsert_admin_credential( &self, input: AdminCredentialInput, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)

# Called by

- [bootstrap_admin](../../../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)
- [create_server_administrator](../../../../../../functions/crates/lpe-admin-api/src/console/create_server_administrator.md)