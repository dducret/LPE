---
type: Rust Function
title: push_interval_part
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L542-L549
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence
---

# Signature

`fn push_interval_part(parts: &mut Vec<String>, recurrence: &str)`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_ews_recurrence](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)