---
type: Rust Function
title: mapi_wrapped_recipient_row
resource: crates/lpe-exchange/src/tests/mod.rs#L15824-L15836
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically
---

# Signature

`fn mapi_wrapped_recipient_row(display_name: &str, address: &str, recipient_type: u8) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_pending_message_display_recipients_follow_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients.md)
- [mapi_over_http_public_folder_create_message_rejects_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_create_message_rejects_recipients.md)
- [mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically.md)