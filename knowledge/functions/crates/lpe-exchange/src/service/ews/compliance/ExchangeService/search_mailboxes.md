---
type: Rust Method
title: search_mailboxes
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L27-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/compliance/discovery_query_text
  - functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/search_ews_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/search_mailboxes_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn search_mailboxes( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [discovery_query_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/discovery_query_text.md)
- [requested_mailbox_emails](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mailboxes/requested_mailbox_emails.md)
- [search_ews_mailboxes](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/search_ews_mailboxes.md)
- [search_mailboxes_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/search_mailboxes_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)