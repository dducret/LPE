---
type: Rust Function
title: oauth_access_token_rejects_surface_outside_scope
resource: crates/lpe-mail-auth/src/tests.rs#L241-L280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
---

# Signature

`async fn oauth_access_token_rejects_surface_outside_scope()`

# Calls

- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [authenticate_bearer_access_token](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)