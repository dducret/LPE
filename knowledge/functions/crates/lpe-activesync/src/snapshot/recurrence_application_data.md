---
type: Rust Function
title: recurrence_application_data
resource: crates/lpe-activesync/src/snapshot.rs#L369-L411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/snapshot/push_text
  - functions/crates/lpe-activesync/src/snapshot/rrule_until_to_compact
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
---

# Signature

`fn recurrence_application_data(rrule: &str) -> Option<Value>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push_text](../../../../../functions/crates/lpe-activesync/src/snapshot/push_text.md)
- [rrule_until_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/rrule_until_to_compact.md)

# Called by

- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)