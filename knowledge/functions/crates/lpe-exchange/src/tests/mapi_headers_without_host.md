---
type: Rust Function
title: mapi_headers_without_host
resource: crates/lpe-exchange/src/tests/mod.rs#L12231-L12235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_host_with_parseable_error
---

# Signature

`fn mapi_headers_without_host(request_type: &str) -> HeaderMap`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [mapi_over_http_rejects_missing_host_with_parseable_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_host_with_parseable_error.md)