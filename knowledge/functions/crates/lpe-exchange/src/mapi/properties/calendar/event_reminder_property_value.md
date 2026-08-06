---
type: Rust Function
title: event_reminder_property_value
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L443-L465
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn event_reminder_property_value( event: &AccessibleEvent, reminder: Option<&lpe_storage::ClientReminder>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [reminder_delta_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/reminder_delta_minutes.md)
- [event_start_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)