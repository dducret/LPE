---
type: Rust Function
title: normalize_scope
resource: crates/lpe-mail-auth/src/oauth.rs#L93-L113
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
  - functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token
  - functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_smtp_surface
  - functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_ews_surface
  - functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_mapi_surface
---

# Signature

`pub fn normalize_scope(scope: &str) -> Result<String>`

# Called by

- [create_client_oauth_access_token](../../../../../functions/crates/lpe-admin-api/src/client_auth/create_client_oauth_access_token.md)
- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [decode_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token.md)
- [normalize_scope_accepts_smtp_surface](../../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_smtp_surface.md)
- [normalize_scope_accepts_ews_surface](../../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_ews_surface.md)
- [normalize_scope_accepts_mapi_surface](../../../../../functions/crates/lpe-mail-auth/src/tests/normalize_scope_accepts_mapi_surface.md)