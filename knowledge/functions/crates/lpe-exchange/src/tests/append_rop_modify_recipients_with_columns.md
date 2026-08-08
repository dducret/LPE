---
type: Rust Function
title: append_rop_modify_recipients_with_columns
resource: crates/lpe-exchange/src/tests/mod.rs#L15333-L15351
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically
  - functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients
---

# Signature

`fn append_rop_modify_recipients_with_columns( rops: &mut Vec<u8>, input: u8, columns: &[u32], rows: &[(u32, u8, &[u8])], )`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_modify_recipients_x500_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically.md)
- [mapi_over_http_microsoft_modify_recipients_example_saves_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically.md)
- [append_rop_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_modify_recipients.md)