---
type: Rust Function
title: mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L5755-L5832
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`async fn mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding()`

# Calls

- [content_sync_response_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [strict_validate_replguid_globset](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_fast_transfer_chunks](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [strict_decode_content_sync_stream](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)