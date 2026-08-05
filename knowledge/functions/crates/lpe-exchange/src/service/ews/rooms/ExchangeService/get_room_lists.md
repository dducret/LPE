---
type: Rust Method
title: get_room_lists
resource: crates/lpe-exchange/src/service/ews/rooms.rs#L32-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/rooms/get_room_lists_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_room_lists( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [get_room_lists_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/get_room_lists_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)