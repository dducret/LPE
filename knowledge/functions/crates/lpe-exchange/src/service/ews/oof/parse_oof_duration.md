---
type: Rust Function
title: parse_oof_duration
resource: crates/lpe-exchange/src/service/ews/oof.rs#L181-L196
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings
---

# Signature

`pub(in crate::service) fn parse_oof_duration(settings: &str) -> Result<OofDuration>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [set_user_oof_settings](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/set_user_oof_settings.md)