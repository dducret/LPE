---
type: Rust Function
title: calendar_status_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L899-L910
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
---

# Signature

`fn calendar_status_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)