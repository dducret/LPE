---
type: Rust Function
title: task_reminder_property_value
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L66-L94
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
---

# Signature

`fn task_reminder_property_value( reminder: Option<&lpe_storage::ClientReminder>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [reminder_delta_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [task_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)