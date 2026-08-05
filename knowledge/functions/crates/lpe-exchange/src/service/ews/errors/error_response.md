---
type: Rust Function
title: error_response
resource: crates/lpe-exchange/src/service/ews/errors.rs#L3-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/errors/soap_auth_challenge
  - functions/crates/lpe-exchange/src/service/ews/errors/soap_error
---

# Signature

`pub(crate) fn error_response(error: &anyhow::Error) -> Response`

# Calls

- [soap_auth_challenge](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/soap_auth_challenge.md)
- [soap_error](../../../../../../../functions/crates/lpe-exchange/src/service/ews/errors/soap_error.md)