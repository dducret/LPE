---
type: Rust Function
title: test_monthly_nth_recur_blob
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L5004-L5020
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules
---

# Signature

`fn test_monthly_nth_recur_blob() -> Vec<u8>`

# Calls

- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)
- [append_appointment_recur_suffix](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix.md)

# Called by

- [mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules.md)