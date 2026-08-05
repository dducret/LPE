---
type: Rust Method
title: get_user_oof_settings
resource: crates/lpe-exchange/src/service/ews/oof.rs#L10-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/oof/get_user_oof_settings_response
  - functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_user_oof_settings( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [get_user_oof_settings_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/get_user_oof_settings_response.md)
- [oof_projection_from_script](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)