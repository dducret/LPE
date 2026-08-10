---
type: Rust Function
title: mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L8126-L8249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties
  - functions/crates/lpe-exchange/src/tests/mapi_message_global_counter
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
---

# Signature

`async fn mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state()`

# Calls

- [legacy_migration_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_wire_id](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [outlook_content_sync_state_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties.md)
- [mapi_message_global_counter](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_message_global_counter.md)
- [outlook_content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_content_final_state_includes](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)