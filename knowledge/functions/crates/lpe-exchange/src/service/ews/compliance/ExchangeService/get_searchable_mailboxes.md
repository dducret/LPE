---
type: Rust Method
title: get_searchable_mailboxes
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L19-L25
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_searchable_mailboxes
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_searchable_mailboxes_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_searchable_mailboxes( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_searchable_mailboxes](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_searchable_mailboxes.md)
- [get_searchable_mailboxes_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_searchable_mailboxes_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)