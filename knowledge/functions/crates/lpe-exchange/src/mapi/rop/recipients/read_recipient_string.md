---
type: Rust Function
title: read_recipient_string
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L307-L313
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
---

# Signature

`fn read_recipient_string(cursor: &mut Cursor<'_>, unicode: bool) -> Result<String>`

# Calls

- [read_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)

# Called by

- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)