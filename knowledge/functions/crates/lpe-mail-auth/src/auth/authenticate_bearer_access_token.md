---
type: Rust Function
title: authenticate_bearer_access_token
resource: crates/lpe-mail-auth/src/auth.rs#L51-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token
  - functions/crates/lpe-mail-auth/src/oauth/scope_allows_surface
  called_by:
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-mail-auth/src/tests/oauth_access_token_rejects_surface_outside_scope
  - functions/crates/lpe-managesieve/src/auth/authenticate
---

# Signature

`pub async fn authenticate_bearer_access_token<S: AccountAuthStore>( store: &S, hinted_user: Option<&str>, token: &str, surface: &str, ) -> Result<AccountPrincipal>`

# Calls

- [decode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token.md)
- [scope_allows_surface](../../../../../functions/crates/lpe-mail-auth/src/oauth/scope_allows_surface.md)

# Called by

- [handle_authenticate](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)
- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [oauth_access_token_rejects_surface_outside_scope](../../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_rejects_surface_outside_scope.md)
- [authenticate](../../../../../functions/crates/lpe-managesieve/src/auth/authenticate.md)