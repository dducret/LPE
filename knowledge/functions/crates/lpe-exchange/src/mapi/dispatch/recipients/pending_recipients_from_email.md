---
type: Rust Function
title: pending_recipients_from_email
resource: crates/lpe-exchange/src/mapi/dispatch/recipients.rs#L27-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
---

# Signature

`pub(super) fn pending_recipients_from_email(email: &JmapEmail) -> Vec<PendingRecipient>`

# Calls

- [message_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)

# Called by

- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)