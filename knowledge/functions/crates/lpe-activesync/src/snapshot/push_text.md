---
type: Rust Function
title: push_text
resource: crates/lpe-activesync/src/snapshot.rs#L265-L269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/contact_application_data
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
  - functions/crates/lpe-activesync/src/snapshot/push_attendees
  - functions/crates/lpe-activesync/src/snapshot/recurrence_application_data
---

# Signature

`fn push_text(children: &mut Vec<Value>, page: u8, name: &str, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/contact_application_data.md)
- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)
- [push_attendees](../../../../../functions/crates/lpe-activesync/src/snapshot/push_attendees.md)
- [recurrence_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/recurrence_application_data.md)