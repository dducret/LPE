---
type: Rust Function
title: verify_state
resource: crates/lpe-admin-api/src/account_oidc.rs#L214-L234
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`fn verify_state(state: &str, secret: &str, expected_redirect_uri: &str) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)