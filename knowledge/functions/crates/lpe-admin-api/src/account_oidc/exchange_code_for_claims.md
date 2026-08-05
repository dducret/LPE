---
type: Rust Function
title: exchange_code_for_claims
resource: crates/lpe-admin-api/src/account_oidc.rs#L61-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`pub async fn exchange_code_for_claims( settings: &SecuritySettings, public_origin: &str, code: &str, state: &str, ) -> Result<AccountOidcClaims>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)