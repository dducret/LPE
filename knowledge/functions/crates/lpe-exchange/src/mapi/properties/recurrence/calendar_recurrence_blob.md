---
type: Rust Function
title: calendar_recurrence_blob
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L3-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_end_minutes
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_exception_override_flags
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_ansi_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`pub(super) fn calendar_recurrence_blob(event: &AccessibleEvent) -> Option<Vec<u8>>`

# Calls

- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)
- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [event_start_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes.md)
- [event_end_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_end_minutes.md)
- [recurrence_exception_override_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_exception_override_flags.md)
- [append_recur_ansi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_ansi_string.md)
- [append_recur_wide_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/append_recur_wide_string.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)