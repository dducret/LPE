---
type: Rust Function
title: mapi_over_http_sync_import_associated_message_persists_and_replays_fai
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L10427-L10720
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i64_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/append_fast_transfer_i64_property
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/read_rop_utf16z
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
---

# Signature

`async fn mapi_over_http_sync_import_associated_message_persists_and_replays_fai()`

# Calls

- [reserve_mapi_local_replica_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_mapi_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i64_property.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_fast_transfer_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/append_fast_transfer_i64_property.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [append_rop_save_changes_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [mapi_get_properties_specific_standard_row_offset](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [read_rop_utf16z](../../../../../../../functions/crates/lpe-exchange/src/tests/read_rop_utf16z.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)