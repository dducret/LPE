---
type: Rust Function
title: until_date
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L338-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical
---

# Signature

`fn until_date(value: &str) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [recurrence_pattern_from_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_pattern_from_canonical.md)