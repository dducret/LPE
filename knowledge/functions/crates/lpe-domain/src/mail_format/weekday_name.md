---
type: Rust Function
title: weekday_name
resource: crates/lpe-domain/src/mail_format.rs#L75-L86
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/days_from_civil
  called_by:
  - functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date
---

# Signature

`fn weekday_name(year: i32, month: i32, day: i32) -> &'static str`

# Calls

- [days_from_civil](../../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)

# Called by

- [rfc5322_utc_date](../../../../../functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date.md)