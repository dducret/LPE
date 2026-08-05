---
type: Rust Function
title: append_submission_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L190-L235
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_submission_dispatch_response<S>( store: &S, principal: &AccountPrincipal, mapi_request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], responses: &mut Vec<u8>, created_emails: &mut Vec<JmapEmail>, ) where S: ExchangeStore,`

# Calls

- [append_spooler_advisory_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response.md)
- [append_deferred_action_messages_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response.md)
- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [append_abort_submit_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)
- [append_transport_info_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)