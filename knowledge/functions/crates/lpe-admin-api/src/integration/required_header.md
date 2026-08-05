---
type: Rust Function
title: required_header
resource: crates/lpe-admin-api/src/integration.rs#L550-L561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-admin-api/src/integration/integration_auth_error
---

# Signature

`fn required_header( headers: &HeaderMap, name: &'static str, ) -> std::result::Result<String, (StatusCode, String)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [integration_auth_error](../../../../../functions/crates/lpe-admin-api/src/integration/integration_auth_error.md)