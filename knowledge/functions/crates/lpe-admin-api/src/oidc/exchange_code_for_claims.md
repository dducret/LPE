---
type: Rust Function
title: exchange_code_for_claims
resource: crates/lpe-admin-api/src/oidc.rs#L61-L126
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`pub async fn exchange_code_for_claims( settings: &SecuritySettings, public_origin: &str, code: &str, state: &str, ) -> Result<AdminOidcClaims>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)