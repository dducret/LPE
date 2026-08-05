---
type: Rust Function
title: mapi_over_http_empty_folder_keeps_child_folder_contents
resource: crates/lpe-exchange/src/tests/mapi_over_http/hierarchy.rs#L3583-L3661
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_empty_folder_keeps_child_folder_contents()`

# Calls

- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)