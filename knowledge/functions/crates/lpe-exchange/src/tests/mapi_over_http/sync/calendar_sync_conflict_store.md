---
type: Rust Function
title: calendar_sync_conflict_store
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13236-L13295
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content
---

# Signature

`fn calendar_sync_conflict_store( event_id: Uuid, message_id: u64, change_key: Vec<u8>, predecessor_change_list: Vec<u8>, updated_at: &str, ) -> FakeStore`

# Calls

- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)

# Called by

- [mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_applies_newer_outlook_unicode_subject.md)
- [mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_remapped_mid_uses_global_object_id.md)
- [mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save.md)
- [mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict.md)
- [mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists.md)
- [mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content.md)