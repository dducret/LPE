---
type: Rust Function
title: reminder_delta_minutes
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L133-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value
---

# Signature

`pub(super) fn reminder_delta_minutes(anchor_filetime: u64, reminder_at: &str) -> i32`

# Calls

- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [event_reminder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value.md)
- [task_reminder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value.md)