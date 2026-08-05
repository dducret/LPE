---
type: Rust Method
title: find_people
resource: crates/lpe-exchange/src/service/ews/directory.rs#L26-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/find_people_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn find_people( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [find_people_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/find_people_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)