---
type: Rust Function
title: weekday_abbrev_from_unix_days
resource: crates/lpe-domain/src/civil_time.rs#L90-L93
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date
---

# Signature

`pub fn weekday_abbrev_from_unix_days(days_since_epoch: i64) -> &'static str`

# Called by

- [mapi_http_date](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date.md)