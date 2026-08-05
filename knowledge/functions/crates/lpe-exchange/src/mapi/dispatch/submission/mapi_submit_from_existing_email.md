---
type: Rust Function
title: mapi_submit_from_existing_email
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L70-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_email
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`pub(super) async fn mapi_submit_from_existing_email<S>( store: &S, principal: &AccountPrincipal, email: &JmapEmail, ) -> Result<SubmitMessageInput> where S: ExchangeStore,`

# Calls

- [mapi_submit_attachments_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/mapi_submit_attachments_from_email.md)
- [mapi_submit_from_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_email.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)