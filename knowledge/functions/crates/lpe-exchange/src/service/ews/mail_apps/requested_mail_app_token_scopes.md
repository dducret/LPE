---
type: Rust Function
title: requested_mail_app_token_scopes
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L193-L207
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token
---

# Signature

`pub(in crate::service) fn requested_mail_app_token_scopes(request: &str) -> Vec<String>`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [get_client_access_token](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token.md)