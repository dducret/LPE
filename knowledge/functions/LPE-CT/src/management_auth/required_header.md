---
type: Rust Function
title: required_header
resource: LPE-CT/src/management_auth.rs#L108-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/management_auth/integration_auth_api_error
---

# Signature

`fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [integration_auth_api_error](../../../../functions/LPE-CT/src/management_auth/integration_auth_api_error.md)