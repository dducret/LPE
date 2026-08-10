---
type: Rust Function
title: append_abort_submit_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L796-L858
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_source_is_sent
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_audit_entry
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_cancel_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response
---

# Signature

`pub(super) async fn append_abort_submit_response<S>( store: &S, principal: &AccountPrincipal, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [abort_submit_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_folder_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [abort_submit_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/abort_submit_message_id.md)
- [abort_submit_canonical_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id.md)
- [abort_submit_source_is_sent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_source_is_sent.md)
- [abort_submit_audit_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_audit_entry.md)
- [abort_submit_cancel_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_cancel_response.md)

# Called by

- [append_submission_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response.md)