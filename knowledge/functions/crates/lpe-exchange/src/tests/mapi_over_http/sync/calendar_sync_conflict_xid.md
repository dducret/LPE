---
type: Rust Function
title: calendar_sync_conflict_xid
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13221-L13225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content
---

# Signature

`fn calendar_sync_conflict_xid(replica_byte: u8, counter: u64) -> Vec<u8>`

# Called by

- [mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id.md)
- [mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save.md)
- [mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict.md)
- [mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists.md)
- [mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content.md)