---
type: Rust Function
title: recurrence_month_from_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L850-L859
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern
---

# Signature

`fn recurrence_month_from_minutes(minutes_since_1601: u32) -> Result<u32>`

# Called by

- [read_recur_pattern](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern.md)