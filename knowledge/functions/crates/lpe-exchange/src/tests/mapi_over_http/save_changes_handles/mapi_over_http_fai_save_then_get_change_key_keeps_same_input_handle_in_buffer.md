---
type: Rust Function
title: mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
resource: crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles.rs#L133-L386
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i64_property
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i32_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_bool_property
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/last_get_properties_binary_value
---

# Signature

`async fn mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer()`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_mapi_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i64_property.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [response_rops_and_handles_from_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_mapi_i32_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i32_property.md)
- [append_mapi_bool_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_bool_property.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [last_get_properties_binary_value](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/last_get_properties_binary_value.md)