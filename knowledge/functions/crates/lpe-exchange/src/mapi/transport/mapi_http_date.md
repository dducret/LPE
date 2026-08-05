---
type: Rust Function
title: mapi_http_date
resource: crates/lpe-exchange/src/mapi/transport.rs#L763-L778
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds
  - functions/crates/lpe-domain/src/civil_time/weekday_abbrev_from_unix_days
  - functions/crates/lpe-domain/src/civil_time/month_abbrev
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
---

# Signature

`fn mapi_http_date(time: SystemTime) -> String`

# Calls

- [utc_from_unix_seconds](../../../../../../functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds.md)
- [weekday_abbrev_from_unix_days](../../../../../../functions/crates/lpe-domain/src/civil_time/weekday_abbrev_from_unix_days.md)
- [month_abbrev](../../../../../../functions/crates/lpe-domain/src/civil_time/month_abbrev.md)

# Called by

- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)