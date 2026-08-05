---
type: Rust Function
title: append_recur_tail
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L4896-L4918
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_daily_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_yearly_recur_blob
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_nth_recur_blob
---

# Signature

`fn append_recur_tail( value: &mut Vec<u8>, end_type: u32, count: u32, deleted: &[u32], modified: &[u32], start_date: &str, end_date: &str, )`

# Calls

- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)

# Called by

- [mapi_over_http_calendar_recurrence_maps_month_end_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule.md)
- [test_daily_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_daily_recur_blob.md)
- [test_weekly_recur_blob_with_deleted_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance.md)
- [test_weekly_recur_blob_with_modified_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance.md)
- [test_weekly_recur_blob_with_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances.md)
- [test_monthly_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_recur_blob.md)
- [test_yearly_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_yearly_recur_blob.md)
- [test_monthly_nth_recur_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_monthly_nth_recur_blob.md)