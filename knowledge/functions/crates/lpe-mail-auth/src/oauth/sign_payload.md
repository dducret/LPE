---
type: Rust Function
title: sign_payload
resource: crates/lpe-mail-auth/src/oauth.rs#L181-L186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-mail-auth/src/oauth/encode_oauth_access_token
---

# Signature

`fn sign_payload(secret: &str, payload: &[u8]) -> Result<Vec<u8>>`

# Called by

- [encode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/encode_oauth_access_token.md)