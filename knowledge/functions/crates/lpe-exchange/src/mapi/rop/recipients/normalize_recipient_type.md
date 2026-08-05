---
type: Rust Function
title: normalize_recipient_type
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L275-L283
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
---

# Signature

`fn normalize_recipient_type(recipient_type: u8) -> Result<u8>`

# Called by

- [parse_simple_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)