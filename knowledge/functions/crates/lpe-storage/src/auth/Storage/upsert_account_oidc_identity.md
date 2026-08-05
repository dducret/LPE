---
type: Rust Method
title: upsert_account_oidc_identity
resource: crates/lpe-storage/src/auth.rs#L466-L487
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`pub async fn upsert_account_oidc_identity(&self, claims: &AccountOidcClaims) -> Result<()>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [client_oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)