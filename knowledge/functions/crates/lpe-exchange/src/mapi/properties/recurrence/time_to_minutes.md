---
type: Rust Function
title: time_to_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L356-L368
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes
---

# Signature

`fn time_to_minutes(time: &str) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [event_start_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes.md)