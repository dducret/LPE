---
type: Rust Method
title: set_user_oof_settings
resource: crates/lpe-exchange/src/service/ews/oof.rs#L23-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_state
  - functions/crates/lpe-exchange/src/service/ews/oof/normalize_oof_external_audience
  - functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_duration
  - functions/crates/lpe-exchange/src/service/ews/oof/set_user_oof_settings_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/set_user_oof_settings_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn set_user_oof_settings( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [parse_oof_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_state.md)
- [normalize_oof_external_audience](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/normalize_oof_external_audience.md)
- [parse_oof_duration](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/parse_oof_duration.md)
- [set_user_oof_settings_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/set_user_oof_settings_success_response.md)
- [set_user_oof_settings_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/set_user_oof_settings_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)