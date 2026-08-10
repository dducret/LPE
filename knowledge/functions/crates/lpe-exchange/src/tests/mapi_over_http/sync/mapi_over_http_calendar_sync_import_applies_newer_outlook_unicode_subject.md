---
type: Rust Function
title: mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13401-L13449
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_pcl
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store
  - functions/crates/lpe-exchange/src/mapi/event_metrics/mapi_calendar_event_save_metrics
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import
  - functions/crates/lpe-exchange/src/tests/test_filetime
---

# Signature

`async fn mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject()`

# Calls

- [calendar_sync_conflict_pcl](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_pcl.md)
- [calendar_sync_conflict_store](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/calendar_sync_conflict_store.md)
- [mapi_calendar_event_save_metrics](../../../../../../../functions/crates/lpe-exchange/src/mapi/event_metrics/mapi_calendar_event_save_metrics.md)
- [execute_existing_calendar_sync_import](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/execute_existing_calendar_sync_import.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)