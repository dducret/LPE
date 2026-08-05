---
type: Rust Function
title: get_room_lists_response
resource: crates/lpe-exchange/src/service/ews/rooms.rs#L86-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/rooms/computed_room_list_address
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_room_lists
---

# Signature

`pub(in crate::service) fn get_room_lists_response( principal: &AccountPrincipal, entries: &[ExchangeAddressBookEntry], ) -> String`

# Calls

- [computed_room_list_address](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/computed_room_list_address.md)

# Called by

- [get_room_lists](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_room_lists.md)