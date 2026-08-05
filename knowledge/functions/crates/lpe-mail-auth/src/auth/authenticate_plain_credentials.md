---
type: Rust Function
title: authenticate_plain_credentials
resource: crates/lpe-mail-auth/src/auth.rs#L89-L163
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/authenticate_smtp_submission
  - functions/crates/lpe-imap/src/auth/Session/handle_login
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-managesieve/src/auth/authenticate
---

# Signature

`pub async fn authenticate_plain_credentials<S: AccountAuthStore>( store: &S, hinted_user: Option<&str>, username: &str, password: &str, surface: &str, ) -> Result<AccountPrincipal>`

# Called by

- [authenticate_smtp_submission](../../../../../functions/crates/lpe-admin-api/src/integration/authenticate_smtp_submission.md)
- [handle_login](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_login.md)
- [handle_authenticate](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)
- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [authenticate](../../../../../functions/crates/lpe-managesieve/src/auth/authenticate.md)