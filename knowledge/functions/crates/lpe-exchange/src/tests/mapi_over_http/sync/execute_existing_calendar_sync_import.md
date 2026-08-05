---
type: Rust Function
title: execute_existing_calendar_sync_import
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L13537-L13629
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i64_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content
---

# Signature

`async fn execute_existing_calendar_sync_import( store: FakeStore, source_key: &[u8], last_modification_time: i64, change_key: &[u8], predecessor_change_list: &[u8], import_flag: u8, subject: Option<&str>, request_transfer_state: bool, ) -> Vec<u8>`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [response_rops_and_handles_from_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_mapi_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i64_property.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save.md)
- [mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict.md)
- [mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists.md)
- [mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content.md)