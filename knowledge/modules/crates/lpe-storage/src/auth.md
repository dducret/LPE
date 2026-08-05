---
type: Rust Module
title: auth
resource: crates/lpe-storage/src/auth.rs#L1-L1240
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/uuid-uuid
  - external/crate-normalize-admin-session-auth-method-normalize-email-permission-summary-permissions-from-storage-accountapppasswordrow-accountauthfactorrow-accountloginrow-adminauthfactorrow-adminloginrow-auditentryinput-authenticatedaccountrow-authenticatedadminrow-storage-platform-tenant-id
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [AuthenticatedAdmin](../../../../classes/crates/lpe-storage/src/auth/AuthenticatedAdmin.md)
- [AdminAuthFactor](../../../../classes/crates/lpe-storage/src/auth/AdminAuthFactor.md)
- [AdminCredentialInput](../../../../classes/crates/lpe-storage/src/auth/AdminCredentialInput.md)
- [AccountCredentialInput](../../../../classes/crates/lpe-storage/src/auth/AccountCredentialInput.md)
- [AdminLogin](../../../../classes/crates/lpe-storage/src/auth/AdminLogin.md)
- [AdminOidcClaims](../../../../classes/crates/lpe-storage/src/auth/AdminOidcClaims.md)
- [AccountOidcClaims](../../../../classes/crates/lpe-storage/src/auth/AccountOidcClaims.md)
- [NewAdminAuthFactor](../../../../classes/crates/lpe-storage/src/auth/NewAdminAuthFactor.md)
- [AccountAuthFactor](../../../../classes/crates/lpe-storage/src/auth/AccountAuthFactor.md)
- [NewAccountAuthFactor](../../../../classes/crates/lpe-storage/src/auth/NewAccountAuthFactor.md)
- [AccountAppPassword](../../../../classes/crates/lpe-storage/src/auth/AccountAppPassword.md)
- [StoredAccountAppPassword](../../../../classes/crates/lpe-storage/src/auth/StoredAccountAppPassword.md)
- [AccountLogin](../../../../classes/crates/lpe-storage/src/auth/AccountLogin.md)
- [AuthenticatedAccount](../../../../classes/crates/lpe-storage/src/auth/AuthenticatedAccount.md)
- [upsert_admin_credential](../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential.md)
- [ensure_admin_credential_stub](../../../../functions/crates/lpe-storage/src/auth/Storage/ensure_admin_credential_stub.md)
- [find_admin_oidc_identity](../../../../functions/crates/lpe-storage/src/auth/Storage/find_admin_oidc_identity.md)
- [upsert_admin_oidc_identity](../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_oidc_identity.md)
- [create_admin_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_auth_factor.md)
- [fetch_admin_auth_factors](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors.md)
- [fetch_admin_totp_secret](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_totp_secret.md)
- [fetch_pending_admin_factor_secret](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_admin_factor_secret.md)
- [activate_admin_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/activate_admin_auth_factor.md)
- [revoke_admin_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_admin_auth_factor.md)
- [find_account_oidc_identity](../../../../functions/crates/lpe-storage/src/auth/Storage/find_account_oidc_identity.md)
- [upsert_account_oidc_identity](../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_oidc_identity.md)
- [create_account_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_auth_factor.md)
- [fetch_account_auth_factors](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors.md)
- [fetch_account_totp_secret](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_totp_secret.md)
- [fetch_pending_account_factor_secret](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_account_factor_secret.md)
- [activate_account_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/activate_account_auth_factor.md)
- [revoke_account_auth_factor](../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_account_auth_factor.md)
- [create_account_app_password](../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_app_password.md)
- [list_account_app_passwords](../../../../functions/crates/lpe-storage/src/auth/Storage/list_account_app_passwords.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-storage/src/auth/Storage/touch_account_app_password.md)
- [revoke_account_app_password](../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_account_app_password.md)
- [upsert_account_credential](../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential.md)
- [has_admin_bootstrap_state](../../../../functions/crates/lpe-storage/src/auth/Storage/has_admin_bootstrap_state.md)
- [fetch_admin_login](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)
- [create_admin_session](../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_login.md)
- [create_account_session](../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_session.md)
- [fetch_admin_session](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)
- [delete_admin_session](../../../../functions/crates/lpe-storage/src/auth/Storage/delete_admin_session.md)
- [fetch_account_session](../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_session.md)
- [delete_account_session](../../../../functions/crates/lpe-storage/src/auth/Storage/delete_account_session.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `uuid::Uuid`
- `crate::{
    normalize_admin_session_auth_method, normalize_email, permission_summary,
    permissions_from_storage, AccountAppPasswordRow, AccountAuthFactorRow, AccountLoginRow,
    AdminAuthFactorRow, AdminLoginRow, AuditEntryInput, AuthenticatedAccountRow,
    AuthenticatedAdminRow, Storage, PLATFORM_TENANT_ID,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)