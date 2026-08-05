---
type: Rust Function
title: vacation_response_to_value
resource: crates/lpe-jmap/src/vacation.rs#L355-L384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`fn vacation_response_to_value( projection: &VacationResponseProjection, properties: &HashSet<String>, ) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_vacation_response_get](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)