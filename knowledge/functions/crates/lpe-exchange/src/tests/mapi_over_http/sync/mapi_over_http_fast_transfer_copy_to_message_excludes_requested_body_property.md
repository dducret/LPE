---
type: Rust Function
title: mapi_over_http_fast_transfer_copy_to_message_excludes_requested_body_property
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L7929-L8000
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_message_id
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
---

# Signature

`async fn mapi_over_http_fast_transfer_copy_to_message_excludes_requested_body_property()`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [test_mapi_message_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_message_id.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_fast_transfer_chunks](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)