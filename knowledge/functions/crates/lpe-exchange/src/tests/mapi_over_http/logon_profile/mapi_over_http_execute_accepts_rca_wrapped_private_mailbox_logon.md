---
type: Rust Function
title: mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon
resource: crates/lpe-exchange/src/tests/mapi_over_http/logon_profile.rs#L125-L209
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/rca_wrapped_private_logon_execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
---

# Signature

`async fn mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [rca_wrapped_private_logon_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/rca_wrapped_private_logon_execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)