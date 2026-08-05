---
type: Rust Method
title: actual_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L271-L277
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/identity/durable_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/source_key_for_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/long_term_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_with_provider
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/message_entry_id_from_object_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows
  - functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test
---

# Signature

`pub(crate) fn actual_object_id(&self, object_id: u64) -> Option<u64>`

# Calls

- [is_logical_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [allocate_logon_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [durable_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/durable_object_id.md)
- [wire_id_bytes_from_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/wire_id_bytes_from_object_id.md)
- [source_key_for_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/source_key_for_object_id.md)
- [long_term_id_from_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/long_term_id_from_object_id.md)
- [folder_entry_id_with_provider](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/folder_entry_id_with_provider.md)
- [message_entry_id_from_object_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/message_entry_id_from_object_ids.md)
- [mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows.md)
- [durable_special_folder_id_for_test](../../../../../../../functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test.md)