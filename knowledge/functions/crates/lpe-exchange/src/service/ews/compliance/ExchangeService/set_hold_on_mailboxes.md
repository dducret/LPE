---
type: Rust Method
title: set_hold_on_mailboxes
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L54-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/compliance/discovery_query_text
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_ews_hold_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/set_hold_on_mailboxes_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn set_hold_on_mailboxes( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_mailbox_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [discovery_query_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/discovery_query_text.md)
- [set_ews_hold_mailboxes](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_ews_hold_mailboxes.md)
- [set_hold_on_mailboxes_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/set_hold_on_mailboxes_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)