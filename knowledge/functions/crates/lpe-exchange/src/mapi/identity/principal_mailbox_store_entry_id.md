---
type: Rust Function
title: principal_mailbox_store_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L595-L599
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_outlook_mailbox_store_object_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
---

# Signature

`pub(crate) fn principal_mailbox_store_entry_id(principal: &AccountPrincipal) -> Vec<u8>`

# Calls

- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)
- [mailbox_store_object_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id.md)

# Called by

- [format_common_views_wlink_target_decoding](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding.md)
- [navigation_shortcut_property_value_for_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)
- [navigation_shortcut_projects_outlook_mailbox_store_object_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_outlook_mailbox_store_object_entry_id.md)
- [common_views_query_rows_projects_mailbox_store_object_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)