---
type: Rust Function
title: recurrence_first_date_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L230-L258
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
---

# Signature

`fn recurrence_first_date_minutes( event: &AccessibleEvent, by_month: Option<u32>, by_month_day: Option<u32>, ) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)

# Called by

- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)