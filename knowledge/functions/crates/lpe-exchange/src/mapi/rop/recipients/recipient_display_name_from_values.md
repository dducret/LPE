---
type: Rust Function
title: recipient_display_name_from_values
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L257-L266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
---

# Signature

`fn recipient_display_name_from_values(values: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [optional_mapi_value_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text.md)

# Called by

- [parse_simple_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)