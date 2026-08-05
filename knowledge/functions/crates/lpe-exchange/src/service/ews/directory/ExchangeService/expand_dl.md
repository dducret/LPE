---
type: Rust Method
title: expand_dl
resource: crates/lpe-exchange/src/service/ews/directory.rs#L17-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn expand_dl( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [expand_dl_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/expand_dl_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)