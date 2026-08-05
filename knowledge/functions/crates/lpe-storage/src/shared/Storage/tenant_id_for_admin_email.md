---
type: Rust Method
title: tenant_id_for_admin_email
resource: crates/lpe-storage/src/shared.rs#L772-L811
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential
  - functions/crates/lpe-storage/src/auth/Storage/ensure_admin_credential_stub
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_oidc_identity
  - functions/crates/lpe-storage/src/auth/Storage/create_admin_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_totp_secret
  - functions/crates/lpe-storage/src/auth/Storage/fetch_pending_admin_factor_secret
  - functions/crates/lpe-storage/src/auth/Storage/activate_admin_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/revoke_admin_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login
---

# Signature

`pub(crate) async fn tenant_id_for_admin_email(&self, email: &str) -> Result<Uuid>`

# Called by

- [find_server_administrator_by_email](../../../../../../functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email.md)
- [upsert_admin_credential](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential.md)
- [ensure_admin_credential_stub](../../../../../../functions/crates/lpe-storage/src/auth/Storage/ensure_admin_credential_stub.md)
- [upsert_admin_oidc_identity](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_oidc_identity.md)
- [create_admin_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_auth_factor.md)
- [fetch_admin_auth_factors](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors.md)
- [fetch_admin_totp_secret](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_totp_secret.md)
- [fetch_pending_admin_factor_secret](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_admin_factor_secret.md)
- [activate_admin_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/activate_admin_auth_factor.md)
- [revoke_admin_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_admin_auth_factor.md)
- [fetch_admin_login](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)