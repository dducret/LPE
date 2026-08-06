---
type: Rust Function
title: mapi_over_http_pending_message_display_recipients_follow_modify_recipients
resource: crates/lpe-exchange/src/tests/mapi_over_http/properties.rs#L997-L1030
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_create_message
  - functions/crates/lpe-exchange/src/tests/mapi_wrapped_recipient_row
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients
  - functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific
  - functions/crates/lpe-exchange/src/tests/execute_rops_response_rops
  - functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_over_http_pending_message_display_recipients_follow_modify_recipients()`

# Calls

- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_create_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_message.md)
- [mapi_wrapped_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_wrapped_recipient_row.md)
- [append_rop_modify_recipients](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients.md)
- [append_rop_get_properties_specific](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_get_properties_specific.md)
- [execute_rops_response_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_rops_response_rops.md)
- [mapi_get_properties_specific_standard_row_offset](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)