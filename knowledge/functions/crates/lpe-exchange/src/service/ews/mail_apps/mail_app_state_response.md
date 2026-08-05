---
type: Rust Function
title: mail_app_state_response
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L269-L290
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app
---

# Signature

`pub(in crate::service) fn mail_app_state_response( operation: &str, app_id: &str, status: &str, ) -> String`

# Called by

- [install_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/install_app.md)
- [disable_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/disable_app.md)
- [uninstall_app](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/uninstall_app.md)