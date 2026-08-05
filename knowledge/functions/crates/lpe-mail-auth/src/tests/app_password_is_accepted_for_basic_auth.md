---
type: Rust Function
title: app_password_is_accepted_for_basic_auth
resource: crates/lpe-mail-auth/src/tests.rs#L159-L191
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
---

# Signature

`async fn app_password_is_accepted_for_basic_auth()`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)