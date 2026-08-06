---
type: Rust Function
title: read_rop_binary_u16
resource: crates/lpe-exchange/src/tests/mod.rs#L13488-L13502
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/calendar_change_key_from_get_properties_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/hierarchy_query_calendar_contract_rows
---

# Signature

`fn read_rop_binary_u16<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [calendar_change_key_from_get_properties_response](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/calendar_change_key_from_get_properties_response.md)
- [mapi_over_http_default_contacts_folder_properties_use_persisted_change_number](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number.md)
- [mapi_over_http_microsoft_create_message_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties.md)
- [mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed.md)
- [mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [hierarchy_query_calendar_contract_rows](../../../../../functions/crates/lpe-exchange/src/tests/hierarchy_query_calendar_contract_rows.md)