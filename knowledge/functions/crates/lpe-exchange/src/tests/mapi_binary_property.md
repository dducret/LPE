---
type: Rust Function
title: mapi_binary_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13897-L13902
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
  - functions/crates/lpe-exchange/src/tests/mapi_message_cnset_property
  - functions/crates/lpe-exchange/src/tests/mapi_deleted_message_idset_property
  - functions/crates/lpe-exchange/src/tests/mapi_read_message_idset_property
  - functions/crates/lpe-exchange/src/tests/mapi_unread_message_idset_property
---

# Signature

`fn mapi_binary_property(tag: u32, value: &[u8]) -> Vec<u8>`

# Called by

- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)
- [mapi_message_cnset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_message_cnset_property.md)
- [mapi_deleted_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_deleted_message_idset_property.md)
- [mapi_read_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_read_message_idset_property.md)
- [mapi_unread_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_unread_message_idset_property.md)