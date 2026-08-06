---
type: Rust Function
title: test_weekly_recur_blob_with_modified_instance
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L4865-L4922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions
---

# Signature

`fn test_weekly_recur_blob_with_modified_instance( override_flags: u16, subject: &str, location: &str, ) -> Vec<u8>`

# Calls

- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)
- [append_recur_wide_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string.md)

# Called by

- [mapi_over_http_calendar_recurrence_rejects_unsupported_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes.md)
- [mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides.md)
- [mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions.md)