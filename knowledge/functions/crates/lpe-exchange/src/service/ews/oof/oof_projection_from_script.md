---
type: Rust Function
title: oof_projection_from_script
resource: crates/lpe-exchange/src/service/ews/oof.rs#L198-L242
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/oof/find_vacation_reason
  - functions/crates/lpe-exchange/src/service/ews/oof/oof_metadata_value
  - functions/crates/lpe-exchange/src/service/ews/oof/normalize_oof_external_audience
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips
  - functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/get_user_oof_settings
---

# Signature

`pub(in crate::service) fn oof_projection_from_script(content: Option<&str>) -> OofProjection`

# Calls

- [find_vacation_reason](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/find_vacation_reason.md)
- [oof_metadata_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/oof_metadata_value.md)
- [normalize_oof_external_audience](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/normalize_oof_external_audience.md)

# Called by

- [get_mail_tips](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_tips/ExchangeService/get_mail_tips.md)
- [get_user_oof_settings](../../../../../../../functions/crates/lpe-exchange/src/service/ews/oof/ExchangeService/get_user_oof_settings.md)