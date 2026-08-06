---
type: Rust Function
title: test_weekly_recur_blob_with_deleted_instance
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L4848-L4863
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides
---

# Signature

`fn test_weekly_recur_blob_with_deleted_instance() -> Vec<u8>`

# Calls

- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)
- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [append_appointment_recur_suffix](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_appointment_recur_suffix.md)

# Called by

- [mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides.md)