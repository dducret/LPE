---
type: Rust Method
title: upsert_account_credential
resource: crates/lpe-storage/src/auth.rs#L826-L874
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  called_by:
  - functions/crates/lpe-admin-api/src/console/create_account
---

# Signature

`pub async fn upsert_account_credential( &self, input: AccountCredentialInput, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)

# Called by

- [create_account](../../../../../../functions/crates/lpe-admin-api/src/console/create_account.md)