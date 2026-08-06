---
type: Rust Function
title: append_rop_query_subject_rows
resource: crates/lpe-exchange/src/tests/mod.rs#L15576-L15585
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_query_rows_uses_paged_content_table_lookup
---

# Signature

`fn append_rop_query_subject_rows(rops: &mut Vec<u8>, input: u8, output: u8, row_count: u16)`

# Called by

- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders.md)
- [mapi_over_http_query_rows_uses_paged_content_table_lookup](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_query_rows_uses_paged_content_table_lookup.md)