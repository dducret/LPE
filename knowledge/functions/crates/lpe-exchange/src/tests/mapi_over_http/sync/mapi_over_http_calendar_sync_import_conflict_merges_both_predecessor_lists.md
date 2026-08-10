---
type: Rust Function
title: mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13528-L13572
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_xid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_pcl
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes
---

# Signature

`async fn mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists()`

# Calls

- [calendar_sync_conflict_xid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_xid.md)
- [calendar_sync_conflict_pcl](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_pcl.md)
- [calendar_sync_conflict_store](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store.md)
- [execute_existing_calendar_sync_import](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [assert_content_upload_final_state_includes](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes.md)