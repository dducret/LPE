---
type: Rust Function
title: mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
resource: crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties.rs#L14-L381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/mapi_private_logon_rops
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i32_property
  - functions/crates/lpe-exchange/src/tests/append_rop_create_associated_message
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/mapi_binary_property
---

# Signature

`async fn mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [mapi_private_logon_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_private_logon_rops.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [mailbox_owner_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id.md)
- [principal_mailbox_store_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_mapi_i32_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i32_property.md)
- [append_rop_create_associated_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_associated_message.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [content_sync_response_rops_for_store_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [mapi_fast_transfer_chunks](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_binary_property.md)