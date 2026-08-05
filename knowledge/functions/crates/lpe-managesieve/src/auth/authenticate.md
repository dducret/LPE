---
type: Rust Function
title: authenticate
resource: crates/lpe-managesieve/src/auth.rs#L17-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-managesieve/src/parse/as_string
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
---

# Signature

`pub(crate) async fn authenticate<S: ManageSieveStore>( store: &S, arguments: &[Argument], ) -> Result<AuthenticatedAccount>`

# Calls

- [as_string](../../../../../functions/crates/lpe-managesieve/src/parse/as_string.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [authenticate_plain_credentials](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)
- [authenticate_bearer_access_token](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)