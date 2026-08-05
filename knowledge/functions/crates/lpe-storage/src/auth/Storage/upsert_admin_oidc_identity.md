---
type: Rust Method
title: upsert_admin_oidc_identity
resource: crates/lpe-storage/src/auth.rs#L229-L250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn upsert_admin_oidc_identity(&self, claims: &AdminOidcClaims) -> Result<()>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)