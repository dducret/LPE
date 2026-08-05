---
type: Rust Method
title: get_persona
resource: crates/lpe-exchange/src/service/ews/directory.rs#L35-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_persona( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [get_persona_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)