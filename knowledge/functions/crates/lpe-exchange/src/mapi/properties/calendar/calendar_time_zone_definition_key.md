---
type: Rust Function
title: calendar_time_zone_definition_key
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L877-L904
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi
---

# Signature

`fn calendar_time_zone_definition_key(value: &[u8]) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [calendar_time_zone_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi.md)