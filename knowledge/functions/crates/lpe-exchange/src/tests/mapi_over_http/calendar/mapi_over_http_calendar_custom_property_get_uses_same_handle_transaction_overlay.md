---
type: Rust Function
title: mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L1258-L1404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values
  - functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_open_message_with_flags
  - functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/append_rop_delete_properties
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values
---

# Signature

`async fn mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay()`

# Calls

- [upsert_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values.md)
- [durable_special_folder_id_for_test](../../../../../../../functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_open_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_message_with_flags.md)
- [test_mapi_uuid_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [response_rops_and_handles_from_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [append_rop_delete_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_delete_properties.md)
- [fetch_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_custom_property_values.md)