---
type: Rust Function
title: replguid_globset_counters
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1138-L1154
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
---

# Signature

`pub(crate) fn replguid_globset_counters(value: &[u8]) -> Result<Vec<u64>, String>`

# Calls

- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [decode_globset_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_globset_ranges.md)

# Called by

- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [upload_sync_state_stream_with_uploaded_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [collect_final_state_debug_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/collect_final_state_debug_property.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)