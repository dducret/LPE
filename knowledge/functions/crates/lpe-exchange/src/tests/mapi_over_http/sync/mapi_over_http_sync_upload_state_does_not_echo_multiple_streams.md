---
type: Rust Function
title: mapi_over_http_sync_upload_state_does_not_echo_multiple_streams
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L9444-L9519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes
---

# Signature

`async fn mapi_over_http_sync_upload_state_does_not_echo_multiple_streams()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [assert_content_upload_final_state_includes](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes.md)