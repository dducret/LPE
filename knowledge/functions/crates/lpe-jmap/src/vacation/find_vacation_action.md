---
type: Rust Function
title: find_vacation_action
resource: crates/lpe-jmap/src/vacation.rs#L309-L336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection
---

# Signature

`fn find_vacation_action(statements: &[Statement]) -> Option<(Option<String>, String)>`

# Called by

- [vacation_response_projection](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection.md)