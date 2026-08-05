---
type: Rust Function
title: vacation_response_properties
resource: crates/lpe-jmap/src/vacation.rs#L338-L353
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`fn vacation_response_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_vacation_response_get](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)