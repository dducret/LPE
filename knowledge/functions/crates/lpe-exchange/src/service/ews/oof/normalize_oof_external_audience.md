---
type: Rust Function
title: normalize_oof_external_audience
resource: crates/lpe-exchange/src/service/ews/oof.rs#L279-L281
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings
  - functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script
---

# Signature

`pub(in crate::service) fn normalize_oof_external_audience(value: &str) -> Result<&'static str>`

# Called by

- [set_user_oof_settings](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings.md)
- [oof_projection_from_script](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/oof_projection_from_script.md)