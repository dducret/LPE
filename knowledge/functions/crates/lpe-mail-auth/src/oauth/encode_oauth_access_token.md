---
type: Rust Function
title: encode_oauth_access_token
resource: crates/lpe-mail-auth/src/oauth.rs#L171-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/sign_payload
  called_by:
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
---

# Signature

`fn encode_oauth_access_token(claims: &OAuthAccessTokenClaims, secret: &str) -> Result<String>`

# Calls

- [sign_payload](../../../../../functions/crates/lpe-mail-auth/src/oauth/sign_payload.md)

# Called by

- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)