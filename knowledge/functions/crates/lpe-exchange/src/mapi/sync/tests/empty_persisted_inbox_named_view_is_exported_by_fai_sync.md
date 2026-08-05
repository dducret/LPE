---
type: Rust Function
title: empty_persisted_inbox_named_view_is_exported_by_fai_sync
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L344-L385
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi/sync/tests/assert_fai_boundary_summary
---

# Signature

`fn empty_persisted_inbox_named_view_is_exported_by_fai_sync()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [assert_fai_boundary_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_fai_boundary_summary.md)