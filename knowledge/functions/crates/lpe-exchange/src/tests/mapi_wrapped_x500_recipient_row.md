---
type: Rust Function
title: mapi_wrapped_x500_recipient_row
resource: crates/lpe-exchange/src/tests/mod.rs#L15520-L15535
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically
---

# Signature

`fn mapi_wrapped_x500_recipient_row(display_name: &str, legacy_dn: &str) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_modify_recipients_x500_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically.md)