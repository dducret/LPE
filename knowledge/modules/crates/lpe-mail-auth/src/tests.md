---
type: Rust Module
title: tests
resource: crates/lpe-mail-auth/src/tests.rs#L1-L298
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/argon2-password-hash-rand-core-osrng-passwordhasher-saltstring
  - external/axum-http-header-authorization-headermap-headervalue
  - external/base64-engine
  - external/lpe-storage-accountlogin-auditentryinput-authenticatedaccount-storedaccountapppassword
  - external/std-sync-arc-mutex-mutexguard
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-mail-auth
---

# Contains

- [env_lock](../../../../functions/crates/lpe-mail-auth/src/tests/env_lock.md)
- [tenant_id](../../../../functions/crates/lpe-mail-auth/src/tests/tenant_id.md)
- [FakeStore](../../../../classes/crates/lpe-mail-auth/src/tests/FakeStore.md)
- [fetch_account_session](../../../../functions/crates/lpe-mail-auth/src/tests/FakeStore/accountauthstore/fetch_account_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-mail-auth/src/tests/FakeStore/accountauthstore/fetch_account_login.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-mail-auth/src/tests/FakeStore/accountauthstore/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-mail-auth/src/tests/FakeStore/accountauthstore/touch_account_app_password.md)
- [append_audit_event](../../../../functions/crates/lpe-mail-auth/src/tests/FakeStore/accountauthstore/append_audit_event.md)
- [password_hash](../../../../functions/crates/lpe-mail-auth/src/tests/password_hash.md)
- [bearer_session_preserves_tenant_id](../../../../functions/crates/lpe-mail-auth/src/tests/bearer_session_preserves_tenant_id.md)
- [basic_auth_preserves_tenant_id](../../../../functions/crates/lpe-mail-auth/src/tests/basic_auth_preserves_tenant_id.md)
- [hinted_user_does_not_override_login_tenant](../../../../functions/crates/lpe-mail-auth/src/tests/hinted_user_does_not_override_login_tenant.md)
- [app_password_is_accepted_for_basic_auth](../../../../functions/crates/lpe-mail-auth/src/tests/app_password_is_accepted_for_basic_auth.md)
- [oauth_access_token_is_accepted_for_bearer_auth](../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_is_accepted_for_bearer_auth.md)
- [oauth_access_token_rejects_surface_outside_scope](../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_rejects_surface_outside_scope.md)
- [normalize_scope_accepts_smtp_surface](../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_smtp_surface.md)
- [normalize_scope_accepts_ews_surface](../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_ews_surface.md)
- [normalize_scope_accepts_mapi_surface](../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_mapi_surface.md)

# Imports

- `super::*`
- `argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString}`
- `axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue}`
- `base64::Engine`
- `lpe_storage::{AccountLogin, AuditEntryInput, AuthenticatedAccount, StoredAccountAppPassword}`
- `std::sync::{Arc, Mutex, MutexGuard}`
- `uuid::Uuid`

# Member of

- [lpe-mail-auth](../../../../packages/crates/lpe-mail-auth.md)