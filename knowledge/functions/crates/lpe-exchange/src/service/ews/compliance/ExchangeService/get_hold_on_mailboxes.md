---
type: Rust Method
title: get_hold_on_mailboxes
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L41-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_hold_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_hold_on_mailboxes_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_hold_on_mailboxes( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_mailbox_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails.md)
- [fetch_ews_hold_mailboxes](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_hold_mailboxes.md)
- [get_hold_on_mailboxes_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_hold_on_mailboxes_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)