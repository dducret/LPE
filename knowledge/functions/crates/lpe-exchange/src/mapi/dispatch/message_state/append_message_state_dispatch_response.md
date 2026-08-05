---
type: Rust Function
title: append_message_state_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/message_state.rs#L10-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_message_state_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [append_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response.md)
- [append_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)