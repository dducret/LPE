---
type: Rust Function
title: mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L8527-L8619
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_content_sync_manifest_get_buffer_with_state
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/mapi_binary_property_value
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
---

# Signature

`async fn mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [outlook_content_sync_state_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_outlook_content_sync_manifest_get_buffer_with_state](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_content_sync_manifest_get_buffer_with_state.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_fast_transfer_chunks](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [mapi_binary_property_value](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_binary_property_value.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)