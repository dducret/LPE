---
type: Rust Function
title: serialize_folder_row_with_context
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L807-L843
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_type
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/tests/mapi_hierarchy_row_projects_inbox_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxcfold_hierarchy_row_projects_folder_message_size_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/folder_type_rows_follow_microsoft_values
  - functions/crates/lpe-exchange/src/mapi/tables/tests/real_quick_step_folder_projects_configuration_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/access_rows_follow_microsoft_flags
---

# Signature

`pub(in crate::mapi) fn serialize_folder_row_with_context( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], columns: &[u32], mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [mapi_mailbox_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name.md)
- [mapi_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_type.md)
- [folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [serialize_folder_row_with_context_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [mapi_hierarchy_row_projects_inbox_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/mapi_hierarchy_row_projects_inbox_display_name.md)
- [microsoft_oxcfold_hierarchy_row_projects_folder_message_size_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxcfold_hierarchy_row_projects_folder_message_size_columns.md)
- [folder_type_rows_follow_microsoft_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/folder_type_rows_follow_microsoft_values.md)
- [real_quick_step_folder_projects_configuration_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/real_quick_step_folder_projects_configuration_class.md)
- [access_rows_follow_microsoft_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/access_rows_follow_microsoft_flags.md)