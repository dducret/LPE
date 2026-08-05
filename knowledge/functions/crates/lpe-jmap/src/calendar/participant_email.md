---
type: Rust Function
title: participant_email
resource: crates/lpe-jmap/src/calendar.rs#L1376-L1395
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants
---

# Signature

`fn participant_email(participant: &Map<String, Value>, owner: bool) -> Result<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_jmap_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants.md)