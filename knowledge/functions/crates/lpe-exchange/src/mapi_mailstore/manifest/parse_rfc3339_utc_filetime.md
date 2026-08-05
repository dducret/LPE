---
type: Rust Function
title: parse_rfc3339_utc_filetime
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L348-L393
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_digits
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/days_in_month
  - functions/crates/lpe-domain/src/civil_time/days_from_civil
  - functions/crates/lpe-domain/src/civil_time/windows_filetime_from_signed_unix_seconds
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/special_message_delivery_sort_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
---

# Signature

`fn parse_rfc3339_utc_filetime(value: &str) -> Option<u64>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_digits](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_digits.md)
- [days_in_month](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/days_in_month.md)
- [days_from_civil](../../../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)
- [windows_filetime_from_signed_unix_seconds](../../../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_from_signed_unix_seconds.md)

# Called by

- [email_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [special_message_delivery_sort_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/special_message_delivery_sort_time.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)