---
type: Rust Function
title: parse_pending_recipient_row
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L74-L94
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns
---

# Signature

`pub(in crate::mapi) fn parse_pending_recipient_row( row_id: u32, fallback_recipient_type: u8, columns: &[u32], row: &[u8], principal: &AccountPrincipal, address_book_entries: &[ExchangeAddressBookEntry], ) -> Result<PendingRecipient>`

# Calls

- [parse_wrapped_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [parse_simple_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)

# Called by

- [modify_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [modify_recipients_parses_outlook_flagged_recipient_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row.md)
- [modify_recipients_accepts_microsoft_message_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns.md)