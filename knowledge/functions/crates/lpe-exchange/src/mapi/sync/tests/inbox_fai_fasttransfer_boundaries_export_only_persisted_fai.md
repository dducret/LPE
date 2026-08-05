---
type: Rust Function
title: inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L304-L341
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_inbox_associated_configs
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi/sync/tests/assert_fai_boundary_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin
---

# Signature

`fn inbox_fai_fasttransfer_boundaries_export_only_persisted_fai()`

# Calls

- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [persisted_inbox_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_inbox_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [assert_fai_boundary_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_fai_boundary_summary.md)
- [fai_debug_state_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/fai_debug_state_origin.md)