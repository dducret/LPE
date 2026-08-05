---
type: Rust Function
title: optional_mapi_value_text
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L268-L273
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values
---

# Signature

`fn optional_mapi_value_text(values: &HashMap<u32, MapiValue>, tags: &[u32]) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)

# Called by

- [parse_simple_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [recipient_display_name_from_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values.md)