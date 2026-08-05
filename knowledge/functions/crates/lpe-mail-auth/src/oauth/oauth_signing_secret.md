---
type: Rust Function
title: oauth_signing_secret
resource: crates/lpe-mail-auth/src/oauth.rs#L115-L131
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
  - functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token
---

# Signature

`pub fn oauth_signing_secret() -> Result<String>`

# Called by

- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [decode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token.md)