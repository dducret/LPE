---
type: Rust Function
title: mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets
resource: crates/lpe-exchange/src/tests/mapi_over_http/connect.rs#L2761-L2863
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset
---

# Signature

`async fn mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets()`

# Calls

- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [outlook_content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [strict_validate_replid_globset](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset.md)