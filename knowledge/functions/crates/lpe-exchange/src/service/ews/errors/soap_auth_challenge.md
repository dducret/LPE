---
type: Rust Function
title: soap_auth_challenge
resource: crates/lpe-exchange/src/service/ews/errors.rs#L18-L25
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/errors/soap_error
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/errors/error_response
---

# Signature

`fn soap_auth_challenge(message: &str) -> Response`

# Calls

- [soap_error](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/soap_error.md)

# Called by

- [error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/error_response.md)