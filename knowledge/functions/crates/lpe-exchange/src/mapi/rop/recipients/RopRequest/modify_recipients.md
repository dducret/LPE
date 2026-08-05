---
type: Rust Method
title: modify_recipients
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L24-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row
---

# Signature

`pub(in crate::mapi) fn modify_recipients( &self, principal: &AccountPrincipal, address_book_entries: &[ExchangeAddressBookEntry], ) -> Result<Vec<PendingRecipientChange>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [parse_pending_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)

# Called by

- [append_modify_recipients_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row.md)