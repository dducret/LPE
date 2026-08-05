---
type: Rust Method
title: vacation_response_projection
resource: crates/lpe-jmap/src/vacation.rs#L186-L204
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/vacation/find_vacation_action
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`async fn vacation_response_projection( &self, account_id: uuid::Uuid, ) -> Result<VacationResponseProjection>`

# Calls

- [find_vacation_action](../../../../../../functions/crates/lpe-jmap/src/vacation/find_vacation_action.md)

# Called by

- [handle_vacation_response_get](../../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)