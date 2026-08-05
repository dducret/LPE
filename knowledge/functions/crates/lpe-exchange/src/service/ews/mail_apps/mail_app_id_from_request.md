---
type: Rust Function
title: mail_app_id_from_request
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L185-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token
---

# Signature

`pub(in crate::service) fn mail_app_id_from_request(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [install_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app.md)
- [disable_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app.md)
- [uninstall_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app.md)
- [get_client_access_token](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_client_access_token.md)