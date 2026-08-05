---
type: Rust Function
title: wlink_ordinal_bytes
resource: crates/lpe-exchange/src/mapi/properties.rs#L1159-L1177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_group_header_and_link_properties_round_trip_group_identity
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_navigation_shortcut_example_preserves_wlink_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contacts_navigation_shortcuts_use_contact_folder_type
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_associated_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_sharing_local_folder_id
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_preserves_persisted_address_book_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_outlook_mailbox_store_object_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_wlink_ordinal_projection_is_injective_and_avoids_reserved_markers
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_wlink_identifiers_use_exact_binary_tags
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_section_one_projects_favorites_group_name
  - functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_group_header_does_not_project_group_name
  - functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_common_views_shortcuts
---

# Signature

`pub(in crate::mapi) fn wlink_ordinal_bytes(value: u32) -> Vec<u8>`

# Calls

- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys.md)
- [navigation_shortcut_group_header_and_link_properties_round_trip_group_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_group_header_and_link_properties_round_trip_group_identity.md)
- [microsoft_navigation_shortcut_example_preserves_wlink_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_navigation_shortcut_example_preserves_wlink_properties.md)
- [outlook_contacts_navigation_shortcuts_use_contact_folder_type](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_contacts_navigation_shortcuts_use_contact_folder_type.md)
- [navigation_shortcut_projects_associated_table_identity_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_associated_table_identity_columns.md)
- [navigation_shortcut_projects_sharing_local_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_sharing_local_folder_id.md)
- [navigation_shortcut_preserves_persisted_address_book_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_preserves_persisted_address_book_store_entry_id.md)
- [navigation_shortcut_projects_outlook_mailbox_store_object_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_projects_outlook_mailbox_store_object_entry_id.md)
- [microsoft_oxocfg_wlink_ordinal_projection_is_injective_and_avoids_reserved_markers](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_wlink_ordinal_projection_is_injective_and_avoids_reserved_markers.md)
- [navigation_shortcut_wlink_identifiers_use_exact_binary_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_wlink_identifiers_use_exact_binary_tags.md)
- [navigation_shortcut_section_one_projects_favorites_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_section_one_projects_favorites_group_name.md)
- [navigation_shortcut_group_header_does_not_project_group_name](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/navigation_shortcut_group_header_does_not_project_group_name.md)
- [persisted_common_views_shortcuts](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/persisted_common_views_shortcuts.md)