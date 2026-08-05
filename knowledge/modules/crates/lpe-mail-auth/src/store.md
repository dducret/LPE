---
type: Rust Module
title: store
resource: crates/lpe-mail-auth/src/store.rs#L1-L69
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-storage-accountlogin-auditentryinput-authenticatedaccount-storage-storedaccountapppassword
  - external/std-future-future-pin-pin
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-mail-auth
---

# Contains

- [AccountAuthStore](../../../../interfaces/crates/lpe-mail-auth/src/store/AccountAuthStore.md)
- [fetch_account_session](../../../../functions/crates/lpe-mail-auth/src/store/Storage/accountauthstore/fetch_account_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-mail-auth/src/store/Storage/accountauthstore/fetch_account_login.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-mail-auth/src/store/Storage/accountauthstore/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-mail-auth/src/store/Storage/accountauthstore/touch_account_app_password.md)
- [append_audit_event](../../../../functions/crates/lpe-mail-auth/src/store/Storage/accountauthstore/append_audit_event.md)

# Imports

- `anyhow::Result`
- `lpe_storage::{
    AccountLogin, AuditEntryInput, AuthenticatedAccount, Storage, StoredAccountAppPassword,
}`
- `std::{future::Future, pin::Pin}`
- `uuid::Uuid`

# Member of

- [lpe-mail-auth](../../../../packages/crates/lpe-mail-auth.md)