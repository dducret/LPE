---
type: Rust Function
title: soap_error
resource: crates/lpe-exchange/src/service/ews/errors.rs#L27-L41
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/errors/error_response
  - functions/crates/lpe-exchange/src/service/ews/errors/soap_auth_challenge
---

# Signature

`fn soap_error(status: StatusCode, message: &str) -> Response`

# Called by

- [error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/error_response.md)
- [soap_auth_challenge](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/soap_auth_challenge.md)