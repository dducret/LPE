---
type: Rust Function
title: insert_mapi_content_length
resource: crates/lpe-exchange/src/tests/mod.rs#L12092-L12097
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_headers_without_content_type
  - functions/crates/lpe-exchange/src/tests/mapi_headers_with_content_type
  - functions/crates/lpe-exchange/src/tests/mapi_headers_without_request_id
  - functions/crates/lpe-exchange/src/tests/mapi_headers_without_request_type
  - functions/crates/lpe-exchange/src/tests/mapi_headers_without_client_info
---

# Signature

`fn insert_mapi_content_length(headers: &mut HeaderMap)`

# Called by

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_headers_without_content_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_without_content_type.md)
- [mapi_headers_with_content_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_with_content_type.md)
- [mapi_headers_without_request_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_without_request_id.md)
- [mapi_headers_without_request_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_without_request_type.md)
- [mapi_headers_without_client_info](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers_without_client_info.md)