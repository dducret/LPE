---
type: Rust Method
title: revoke_admin_auth_factor
resource: crates/lpe-storage/src/auth.rs#L403-L427
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor
---

# Signature

`pub async fn revoke_admin_auth_factor( &self, admin_email: &str, factor_id: Uuid, ) -> Result<bool>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [revoke_admin_factor](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor.md)