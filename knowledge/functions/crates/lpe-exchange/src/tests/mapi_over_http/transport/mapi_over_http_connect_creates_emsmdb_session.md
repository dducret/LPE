---
type: Rust Function
title: mapi_over_http_connect_creates_emsmdb_session
resource: crates/lpe-exchange/src/tests/mapi_over_http/transport.rs#L4-L104
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/strip_mapi_http_envelope
---

# Signature

`async fn mapi_over_http_connect_creates_emsmdb_session()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [strip_mapi_http_envelope](../../../../../../../functions/crates/lpe-exchange/src/tests/strip_mapi_http_envelope.md)