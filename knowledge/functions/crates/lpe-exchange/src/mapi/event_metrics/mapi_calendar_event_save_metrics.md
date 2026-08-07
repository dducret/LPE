---
type: Rust Function
title: mapi_calendar_event_save_metrics
resource: crates/lpe-exchange/src/mapi/event_metrics.rs#L62-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/load
  called_by:
  - functions/crates/lpe-admin-api/src/observability/render_metrics
  - functions/crates/lpe-exchange/src/mapi/event_metrics/calendar_event_save_metrics_preserve_direct_and_ics_outcomes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content
---

# Signature

`pub fn mapi_calendar_event_save_metrics() -> MapiCalendarEventSaveMetrics`

# Calls

- [load](../../../../../../functions/LPE-CT/web/app/load.md)

# Called by

- [render_metrics](../../../../../../functions/crates/lpe-admin-api/src/observability/render_metrics.md)
- [calendar_event_save_metrics_preserve_direct_and_ics_outcomes](../../../../../../functions/crates/lpe-exchange/src/mapi/event_metrics/calendar_event_save_metrics_preserve_direct_and_ics_outcomes.md)
- [mapi_over_http_calendar_whole_start_end_update_canonical_event](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject.md)
- [mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save.md)
- [mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content.md)