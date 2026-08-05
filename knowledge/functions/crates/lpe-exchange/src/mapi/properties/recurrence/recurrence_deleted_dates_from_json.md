---
type: Rust Function
title: recurrence_deleted_dates_from_json
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L292-L307
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
---

# Signature

`fn recurrence_deleted_dates_from_json(value: &str) -> Vec<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)

# Called by

- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)