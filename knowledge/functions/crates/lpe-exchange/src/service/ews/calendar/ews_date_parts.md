---
type: Rust Function
title: ews_date_parts
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L665-L674
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_after_days
---

# Signature

`fn ews_date_parts(value: &str) -> Option<(i32, u32, u32)>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [ews_datetime_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes.md)
- [ews_date_after_days](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_after_days.md)