---
type: Rust Function
title: assert_fai_boundary_summary
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L106-L141
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
---

# Signature

`fn assert_fai_boundary_summary( buffer: &[u8], summary: &mapi_mailstore::ContentTransferFaiDebugSummary, expected_count: usize, )`

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)