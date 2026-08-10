---
type: Rust Function
title: append_recipient_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/recipients.rs#L46-L96
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_recipient_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [append_remove_all_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response.md)
- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [append_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)