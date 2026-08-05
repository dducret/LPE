---
type: Rust Function
title: mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
resource: crates/lpe-exchange/src/tests/mapi_over_http/connect.rs#L376-L580
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/mapi_recipient_row
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_create_message
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/test_mapi_message_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/tests/append_rop_set_read_flags
  - functions/crates/lpe-exchange/src/tests/append_rop_open_message
  - functions/crates/lpe-exchange/src/tests/append_rop_submit_message
  - functions/crates/lpe-exchange/src/tests/append_rop_query_subject_rows
---

# Signature

`async fn mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [mapi_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_recipient_row.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_create_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_message.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_modify_recipients](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients.md)
- [append_rop_save_changes_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [test_mapi_message_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_message_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [append_rop_sync_manifest_get_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [append_rop_set_read_flags](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_read_flags.md)
- [append_rop_open_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_message.md)
- [append_rop_submit_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_submit_message.md)
- [append_rop_query_subject_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_query_subject_rows.md)