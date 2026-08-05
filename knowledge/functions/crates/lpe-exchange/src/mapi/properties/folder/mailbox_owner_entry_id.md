---
type: Rust Function
title: mailbox_owner_entry_id
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L251-L262
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`pub(crate) fn mailbox_owner_entry_id(principal: &AccountPrincipal) -> Vec<u8>`

# Calls

- [nspi_entry_unprefixed_legacy_dn](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [nspi_entry_display_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_display_type.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [pending_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/pending_message_property_value.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)