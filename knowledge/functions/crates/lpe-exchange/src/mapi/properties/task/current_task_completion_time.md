---
type: Rust Function
title: current_task_completion_time
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L346-L350
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time
  - functions/crates/lpe-domain/src/civil_time/current_windows_filetime
---

# Signature

`fn current_task_completion_time() -> String`

# Calls

- [filetime_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)
- [current_windows_filetime](../../../../../../../functions/crates/lpe-domain/src/civil_time/current_windows_filetime.md)