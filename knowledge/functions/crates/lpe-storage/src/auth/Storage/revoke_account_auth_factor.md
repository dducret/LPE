---
type: Rust Method
title: revoke_account_auth_factor
resource: crates/lpe-storage/src/auth.rs#L640-L664
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor
---

# Signature

`pub async fn revoke_account_auth_factor( &self, account_email: &str, factor_id: Uuid, ) -> Result<bool>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [revoke_account_factor](../../../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor.md)