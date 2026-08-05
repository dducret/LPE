---
type: Rust Function
title: decode_oauth_access_token
resource: crates/lpe-mail-auth/src/oauth.rs#L133-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/oauth_signing_secret
  - functions/crates/lpe-mail-auth/src/oauth/verify_signature
  - functions/crates/lpe-mail-auth/src/oauth/normalize_scope
  called_by:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
---

# Signature

`pub(crate) fn decode_oauth_access_token(token: &str) -> Result<AccountPrincipalClaims>`

# Calls

- [oauth_signing_secret](../../../../../functions/crates/lpe-mail-auth/src/oauth/oauth_signing_secret.md)
- [verify_signature](../../../../../functions/crates/lpe-mail-auth/src/oauth/verify_signature.md)
- [normalize_scope](../../../../../functions/crates/lpe-mail-auth/src/oauth/normalize_scope.md)

# Called by

- [authenticate_bearer_access_token](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)