---
type: Rust Function
title: calendar_sync_conflict_pcl
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13227-L13234
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content
---

# Signature

`fn calendar_sync_conflict_pcl(xids: &[&[u8]]) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject.md)
- [mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save.md)
- [mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict.md)
- [mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists.md)
- [mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content.md)