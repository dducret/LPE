---
type: Rust Function
title: issue_oauth_access_token
resource: crates/lpe-mail-auth/src/oauth.rs#L46-L63
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/normalize_scope
  - functions/crates/lpe-mail-auth/src/oauth/oauth_signing_secret
  - functions/crates/lpe-mail-auth/src/oauth/encode_oauth_access_token
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token
  - functions/crates/lpe-imap/src/tests/xoauth2_authenticate_is_accepted
  - functions/crates/lpe-mail-auth/src/tests/oauth_access_token_is_accepted_for_bearer_auth
  - functions/crates/lpe-mail-auth/src/tests/oauth_access_token_rejects_surface_outside_scope
  - functions/crates/lpe-managesieve/src/tests/managesieve_accepts_xoauth2
---

# Signature

`pub fn issue_oauth_access_token( principal: &AccountPrincipal, scope: &str, expires_in_seconds: u32, ) -> Result<String>`

# Calls

- [normalize_scope](../../../../../functions/crates/lpe-mail-auth/src/oauth/normalize_scope.md)
- [oauth_signing_secret](../../../../../functions/crates/lpe-mail-auth/src/oauth/oauth_signing_secret.md)
- [encode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/encode_oauth_access_token.md)

# Called by

- [create_client_oauth_access_token](../../../../../functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token.md)
- [xoauth2_authenticate_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/xoauth2_authenticate_is_accepted.md)
- [oauth_access_token_is_accepted_for_bearer_auth](../../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_is_accepted_for_bearer_auth.md)
- [oauth_access_token_rejects_surface_outside_scope](../../../../../functions/crates/lpe-mail-auth/src/tests/oauth_access_token_rejects_surface_outside_scope.md)
- [managesieve_accepts_xoauth2](../../../../../functions/crates/lpe-managesieve/src/tests/managesieve_accepts_xoauth2.md)