---
type: Rust Function
title: mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L7685-L7759
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state()`

# Calls

- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [strict_hierarchy_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)