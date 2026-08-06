---
type: Rust Function
title: mapi_headers_with_client_info
resource: crates/lpe-exchange/src/tests/mod.rs#L12497-L12501
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_client_info_with_parseable_error
---

# Signature

`fn mapi_headers_with_client_info(request_type: &str, client_info: &'static str) -> HeaderMap`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)

# Called by

- [mapi_over_http_rejects_invalid_client_info_with_parseable_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_client_info_with_parseable_error.md)