---
type: Rust Function
title: mapi_headers_without_client_info
resource: crates/lpe-exchange/src/tests/mod.rs#L12347-L12364
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/insert_mapi_content_length
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_client_info_with_parseable_error
---

# Signature

`fn mapi_headers_without_client_info(request_type: &str) -> HeaderMap`

# Calls

- [insert_mapi_content_length](../../../../../functions/crates/lpe-exchange/src/tests/insert_mapi_content_length.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [mapi_over_http_rejects_missing_client_info_with_parseable_error](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_client_info_with_parseable_error.md)