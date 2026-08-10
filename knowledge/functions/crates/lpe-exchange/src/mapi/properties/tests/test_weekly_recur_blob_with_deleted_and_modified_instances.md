---
type: Rust Function
title: test_weekly_recur_blob_with_deleted_and_modified_instances
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L5053-L5096
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header
  - functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_ansi_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances
---

# Signature

`fn test_weekly_recur_blob_with_deleted_and_modified_instances() -> Vec<u8>`

# Calls

- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [append_recur_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_header.md)
- [append_recur_tail](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/append_recur_tail.md)
- [append_recur_ansi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_ansi_string.md)
- [append_recur_wide_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string.md)

# Called by

- [mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances.md)