---
type: Rust Function
title: recurrence_datetime_minutes_since_1601
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L831-L836
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json
---

# Signature

`fn recurrence_datetime_minutes_since_1601(value: &str) -> Option<u32>`

# Calls

- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [recurrence_modified_exceptions_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json.md)