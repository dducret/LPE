---
type: Rust Function
title: mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L6238-L6303
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties
  - functions/crates/lpe-exchange/src/tests/mapi_message_global_counter
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes
---

# Signature

`async fn mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc()`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [outlook_content_sync_state_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties.md)
- [mapi_message_global_counter](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_message_global_counter.md)
- [outlook_content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [assert_content_final_state_includes](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes.md)