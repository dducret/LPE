---
type: Rust Function
title: skip_recur_bytes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L789-L795
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions
---

# Signature

`fn skip_recur_bytes(value: &[u8], offset: &mut usize, len: usize) -> Result<()>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [read_recur_extended_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions.md)