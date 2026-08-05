---
type: Rust Method
title: tenant_id_for_account_email
resource: crates/lpe-storage/src/shared.rs#L746-L770
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/domain_from_email
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_name
  called_by:
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_alias
  - functions/crates/lpe-storage/src/auth/Storage/upsert_account_oidc_identity
  - functions/crates/lpe-storage/src/auth/Storage/create_account_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_totp_secret
  - functions/crates/lpe-storage/src/auth/Storage/fetch_pending_account_factor_secret
  - functions/crates/lpe-storage/src/auth/Storage/activate_account_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/revoke_account_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/create_account_app_password
  - functions/crates/lpe-storage/src/auth/Storage/list_account_app_passwords
  - functions/crates/lpe-storage/src/auth/Storage/fetch_active_account_app_passwords
  - functions/crates/lpe-storage/src/auth/Storage/touch_account_app_password
  - functions/crates/lpe-storage/src/auth/Storage/revoke_account_app_password
  - functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_login
---

# Signature

`pub(crate) async fn tenant_id_for_account_email(&self, email: &str) -> Result<Uuid>`

# Calls

- [domain_from_email](../../../../../../functions/crates/lpe-storage/src/util/domain_from_email.md)
- [tenant_id_for_domain_name](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_domain_name.md)

# Called by

- [create_account](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account.md)
- [create_alias](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_alias.md)
- [upsert_account_oidc_identity](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_oidc_identity.md)
- [create_account_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_auth_factor.md)
- [fetch_account_auth_factors](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors.md)
- [fetch_account_totp_secret](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_totp_secret.md)
- [fetch_pending_account_factor_secret](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_account_factor_secret.md)
- [activate_account_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/activate_account_auth_factor.md)
- [revoke_account_auth_factor](../../../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_account_auth_factor.md)
- [create_account_app_password](../../../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_app_password.md)
- [list_account_app_passwords](../../../../../../functions/crates/lpe-storage/src/auth/Storage/list_account_app_passwords.md)
- [fetch_active_account_app_passwords](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../../../functions/crates/lpe-storage/src/auth/Storage/touch_account_app_password.md)
- [revoke_account_app_password](../../../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_account_app_password.md)
- [upsert_account_credential](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential.md)
- [fetch_account_login](../../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_login.md)