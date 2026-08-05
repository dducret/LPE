---
type: Rust Function
title: requested_room_list_address
resource: crates/lpe-exchange/src/service/ews/rooms.rs#L50-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms
---

# Signature

`pub(in crate::service) fn requested_room_list_address(request: &str) -> Option<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [get_rooms](../../../../../../../functions/crates/lpe-exchange/src/service/ews/rooms/ExchangeService/get_rooms.md)