---
type: Rust Function
title: task_completion_date_filetime
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L352-L356
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
---

# Signature

`fn task_completion_date_filetime(value: &str) -> u64`

# Calls

- [filetime_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [task_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)