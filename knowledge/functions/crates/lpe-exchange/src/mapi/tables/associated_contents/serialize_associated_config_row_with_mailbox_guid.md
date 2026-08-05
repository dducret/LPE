---
type: Rust Function
title: serialize_associated_config_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L141-L154
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties
---

# Signature

`pub(in crate::mapi) fn serialize_associated_config_row_with_mailbox_guid( message: &MapiAssociatedConfigMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_debug_associated_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [inbox_associated_rows_project_folder_id_and_last_modification_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time.md)
- [persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties.md)