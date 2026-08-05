---
type: Rust Function
title: mapi_submit_execute_body
resource: crates/lpe-exchange/src/tests/mod.rs#L12339-L12352
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/mapi_recipient_row
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_create_message
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients
  - functions/crates/lpe-exchange/src/tests/append_rop_submit_message
  - functions/crates/lpe-exchange/src/tests/execute_body
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_rejects_missing_and_malformed_session_cookies
---

# Signature

`fn mapi_submit_execute_body(subject: &str) -> Vec<u8>`

# Calls

- [append_mapi_utf16_property](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [mapi_recipient_row](../../../../../functions/crates/lpe-exchange/src/tests/mapi_recipient_row.md)
- [append_rop_open_folder](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_create_message](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_message.md)
- [append_rop_set_properties](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients.md)
- [append_rop_submit_message](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_submit_message.md)
- [execute_body](../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)

# Called by

- [mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message.md)
- [mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation.md)
- [mapi_over_http_execute_rejects_missing_and_malformed_session_cookies](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_rejects_missing_and_malformed_session_cookies.md)