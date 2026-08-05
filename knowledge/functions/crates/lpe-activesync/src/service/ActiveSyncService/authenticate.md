---
type: Rust Method
title: authenticate
resource: crates/lpe-activesync/src/service.rs#L324-L330
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
---

# Signature

`async fn authenticate( &self, hinted_user: Option<&str>, headers: &HeaderMap, ) -> Result<AccountPrincipal>`

# Calls

- [authenticate_account](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)