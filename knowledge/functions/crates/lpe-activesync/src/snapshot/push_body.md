---
type: Rust Function
title: push_body
resource: crates/lpe-activesync/src/snapshot.rs#L271-L285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/contact_application_data
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
---

# Signature

`fn push_body(children: &mut Vec<Value>, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [contact_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/contact_application_data.md)
- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)