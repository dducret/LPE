---
type: Rust Function
title: pending_attachment_hidden
resource: crates/lpe-exchange/src/mapi/tables/attachments.rs#L111-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
---

# Signature

`fn pending_attachment_hidden(properties: &HashMap<u32, MapiValue>) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)