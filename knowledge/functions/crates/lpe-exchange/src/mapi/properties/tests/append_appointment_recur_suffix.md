---
type: Rust Function
title: append_appointment_recur_suffix
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L4920-L4928
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_daily_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_yearly_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_nth_recur_blob
---

# Signature

`fn append_appointment_recur_suffix(value: &mut Vec<u8>, start: u32, end: u32, exceptions: u16)`

# Called by

- [mapi_over_http_calendar_recurrence_maps_month_end_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule.md)
- [test_daily_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_daily_recur_blob.md)
- [test_weekly_recur_blob_with_deleted_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance.md)
- [test_monthly_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_recur_blob.md)
- [test_yearly_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_yearly_recur_blob.md)
- [test_monthly_nth_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_nth_recur_blob.md)