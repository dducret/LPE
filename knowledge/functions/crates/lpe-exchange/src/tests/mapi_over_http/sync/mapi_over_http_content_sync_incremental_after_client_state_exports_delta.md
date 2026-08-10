---
type: Rust Function
title: mapi_over_http_content_sync_incremental_after_client_state_exports_delta
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L5759-L5866
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/messages
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store_with_rops
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters
---

# Signature

`async fn mapi_over_http_content_sync_incremental_after_client_state_exports_delta()`

# Calls

- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/messages.md)
- [load_mapi_identity_codec_for_test](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [outlook_content_sync_state_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties.md)
- [with_current_mapi_identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [outlook_content_sync_request_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops.md)
- [outlook_content_sync_response_rops_for_store_with_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store_with_rops.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [assert_content_final_state_includes_counters](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters.md)