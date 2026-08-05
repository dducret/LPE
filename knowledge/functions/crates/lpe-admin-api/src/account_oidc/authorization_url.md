---
type: Rust Function
title: authorization_url
resource: crates/lpe-admin-api/src/account_oidc.rs#L39-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`pub async fn authorization_url(settings: &SecuritySettings, public_origin: &str) -> Result<String>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)