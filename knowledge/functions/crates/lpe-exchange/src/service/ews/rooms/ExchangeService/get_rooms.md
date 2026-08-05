---
type: Rust Method
title: get_rooms
resource: crates/lpe-exchange/src/service/ews/rooms.rs#L8-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/rooms/requested_room_list_address
  - functions/crates/lpe-exchange/src/service/ews/rooms/computed_room_list_address
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  - functions/crates/lpe-exchange/src/service/ews/rooms/get_rooms_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_rooms( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_room_list_address](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/requested_room_list_address.md)
- [computed_room_list_address](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/computed_room_list_address.md)
- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)
- [get_rooms_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/get_rooms_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)