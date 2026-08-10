---
type: Rust Function
title: read_rop_utf16z
resource: crates/lpe-exchange/src/tests/mod.rs#L13604-L13614
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_decode_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/hierarchy_query_display_container_rows
  - functions/crates/lpe-exchange/src/tests/hierarchy_query_calendar_contract_rows
---

# Signature

`fn read_rop_utf16z(bytes: &[u8], offset: &mut usize) -> Result<String, String>`

# Calls

- [strict_decode_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_utf16z.md)

# Called by

- [mapi_over_http_microsoft_create_message_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_over_http_sync_import_associated_message_persists_and_replays_fai](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_associated_message_persists_and_replays_fai.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [hierarchy_query_display_container_rows](../../../../../functions/crates/lpe-exchange/src/tests/hierarchy_query_display_container_rows.md)
- [hierarchy_query_calendar_contract_rows](../../../../../functions/crates/lpe-exchange/src/tests/hierarchy_query_calendar_contract_rows.md)