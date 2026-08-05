---
type: Rust Function
title: oauth_access_token_is_accepted_for_bearer_auth
resource: crates/lpe-mail-auth/src/tests.rs#L195-L237
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
---

# Signature

`async fn oauth_access_token_is_accepted_for_bearer_auth()`

# Calls

- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)