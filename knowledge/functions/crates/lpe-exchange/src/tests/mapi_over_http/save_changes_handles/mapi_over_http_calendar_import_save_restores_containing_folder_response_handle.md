---
type: Rust Function
title: mapi_over_http_calendar_import_save_restores_containing_folder_response_handle
resource: crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles.rs#L28-L130
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_binary_property
  - functions/crates/lpe-exchange/src/tests/append_mapi_i64_property
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body
---

# Signature

`async fn mapi_over_http_calendar_import_save_restores_containing_folder_response_handle()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_binary_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_binary_property.md)
- [append_mapi_i64_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_i64_property.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_save_changes_message_with_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message_with_flags.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [response_rops_and_handles_from_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_and_handles_from_execute_body.md)