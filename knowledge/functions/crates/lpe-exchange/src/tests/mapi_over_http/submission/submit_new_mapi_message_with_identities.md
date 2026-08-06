---
type: Rust Function
title: submit_new_mapi_message_with_identities
resource: crates/lpe-exchange/src/tests/mapi_over_http/submission.rs#L3-L66
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property
  - functions/crates/lpe-exchange/src/tests/mapi_recipient_row
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/test_mapi_folder_id
  - functions/crates/lpe-exchange/src/tests/append_rop_create_message
  - functions/crates/lpe-exchange/src/tests/append_rop_set_properties
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients
  - functions/crates/lpe-exchange/src/tests/append_rop_submit_message
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_normal_sender_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_send_as_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_send_on_behalf_identity
---

# Signature

`async fn submit_new_mapi_message_with_identities( subject: &str, sender: (&str, &str), sent_representing: (&str, &str), ) -> (SubmitMessageInput, JmapEmail, lpe_storage::AuditEntryInput)`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_mapi_utf16_property](../../../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_utf16_property.md)
- [mapi_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_recipient_row.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [test_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_folder_id.md)
- [append_rop_create_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_create_message.md)
- [append_rop_set_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_set_properties.md)
- [append_rop_modify_recipients](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients.md)
- [append_rop_submit_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_submit_message.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_submit_new_message_maps_normal_sender_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_normal_sender_identity.md)
- [mapi_over_http_submit_new_message_maps_send_as_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_send_as_identity.md)
- [mapi_over_http_submit_new_message_maps_send_on_behalf_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_submit_new_message_maps_send_on_behalf_identity.md)