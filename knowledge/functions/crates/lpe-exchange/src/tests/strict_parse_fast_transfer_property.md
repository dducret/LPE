---
type: Rust Function
title: strict_parse_fast_transfer_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13149-L13222
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/read_strict_u32
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start
  - functions/crates/lpe-exchange/src/tests/read_strict_slice
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_parse_fast_transfer_property( bytes: &[u8], offset: usize, ) -> Result<StrictFastTransferProperty, String>`

# Calls

- [read_strict_u32](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_u32.md)
- [fast_transfer_property_value_start](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start.md)
- [read_strict_slice](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_slice.md)

# Called by

- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)