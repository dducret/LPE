---
type: Rust Function
title: rca_wrapped_private_logon_execute_body
resource: crates/lpe-exchange/src/tests/mod.rs#L12443-L12477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon
---

# Signature

`fn rca_wrapped_private_logon_execute_body(mailbox: &str, client: &str) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon.md)