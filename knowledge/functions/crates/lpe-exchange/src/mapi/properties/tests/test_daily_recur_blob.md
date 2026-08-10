---
type: Rust Function
title: test_daily_recur_blob
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L4961-L4975
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule
---

# Signature

`fn test_daily_recur_blob(interval_days: u32, count: u32) -> Vec<u8>`

# Calls

- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)
- [append_appointment_recur_suffix](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix.md)

# Called by

- [mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule.md)