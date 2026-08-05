---
type: Rust Function
title: append_modify_recipients_response
resource: crates/lpe-exchange/src/mapi/dispatch/recipients.rs#L164-L333
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_pending_recipient_changes
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/pending_recipients_from_email
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_recipient_dispatch_response
---

# Signature

`pub(super) async fn append_modify_recipients_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [fetch_address_book_entries](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [modify_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [apply_pending_recipient_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_pending_recipient_changes.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [pending_recipients_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/pending_recipients_from_email.md)

# Called by

- [append_recipient_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_recipient_dispatch_response.md)