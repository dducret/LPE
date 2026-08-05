---
type: Rust Function
title: log_fai_content_sync_debug
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L105-L442
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/format_fai_debug_item_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_source_repository
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(crate) fn log_fai_content_sync_debug( sync_type: u8, sync_flags: u16, folder_id: u64, mailbox_guid: Uuid, special_objects: &[SpecialMessageSyncFact], transfer_buffer: &[u8], context: FaiContentSyncDebugContext<'_>, )`

# Calls

- [decode_content_transfer_fai_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [fai_debug_state_origin](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin.md)
- [format_fai_debug_item_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/format_fai_debug_item_order.md)
- [fai_debug_item_classification](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_item_classification.md)
- [fai_debug_source_repository](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_source_repository.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)