---
type: Rust Function
title: mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13632-L13670
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
---

# Signature

`async fn mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save()`

# Calls

- [calendar_sync_conflict_xid](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_xid.md)
- [calendar_sync_conflict_pcl](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_pcl.md)
- [calendar_sync_conflict_store](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store.md)
- [execute_existing_calendar_sync_import](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)