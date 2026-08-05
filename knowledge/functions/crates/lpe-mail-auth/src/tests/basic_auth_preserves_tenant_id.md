---
type: Rust Function
title: basic_auth_preserves_tenant_id
resource: crates/lpe-mail-auth/src/tests.rs#L96-L124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
---

# Signature

`async fn basic_auth_preserves_tenant_id()`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)