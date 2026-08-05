---
type: Rust Function
title: participant_value
resource: crates/lpe-jmap/src/calendar.rs#L1020-L1053
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/participants_from_event
---

# Signature

`fn participant_value( name: &str, email: &str, roles: Value, participation_status: Option<&str>, expect_reply: bool, ) -> Value`

# Called by

- [participants_from_event](../../../../../functions/crates/lpe-jmap/src/calendar/participants_from_event.md)