---
type: Rust Function
title: apply_staged_message_recipient_replacement
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L436-L467
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/submitted_recipients_from_pending
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn apply_staged_message_recipient_replacement<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, message_id: u64, recipients: &[PendingRecipient], mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [submitted_recipients_from_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/submitted_recipients_from_pending.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)