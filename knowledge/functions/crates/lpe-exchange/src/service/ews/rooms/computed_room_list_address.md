---
type: Rust Function
title: computed_room_list_address
resource: crates/lpe-exchange/src/service/ews/rooms.rs#L41-L48
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms
  - functions/crates/lpe-exchange/src/service/ews/rooms/get_room_lists_response
---

# Signature

`pub(in crate::service) fn computed_room_list_address(principal: &AccountPrincipal) -> String`

# Called by

- [get_rooms](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms.md)
- [get_room_lists_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/get_room_lists_response.md)