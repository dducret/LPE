---
type: Rust Function
title: verify_signature
resource: crates/lpe-mail-auth/src/oauth.rs#L188-L195
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token
---

# Signature

`fn verify_signature(secret: &str, payload: &[u8], signature: &[u8]) -> Result<()>`

# Called by

- [decode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token.md)