---
type: Rust Function
title: recurrence_minutes_since_1601
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L813-L829
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_first_date_minutes
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance
  - functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
---

# Signature

`pub(super) fn recurrence_minutes_since_1601(date: &str) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)
- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)
- [recurrence_first_date_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_first_date_minutes.md)
- [recurrence_deleted_dates_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json.md)
- [recurrence_modified_exceptions_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json.md)
- [recurrence_datetime_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_minutes_since_1601.md)
- [test_weekly_recur_blob_with_deleted_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_instance.md)
- [test_weekly_recur_blob_with_modified_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_modified_instance.md)
- [test_weekly_recur_blob_with_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/test_weekly_recur_blob_with_deleted_and_modified_instances.md)
- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)