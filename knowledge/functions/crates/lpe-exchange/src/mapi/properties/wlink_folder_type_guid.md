---
type: Rust Function
title: wlink_folder_type_guid
resource: crates/lpe-exchange/src/mapi/properties.rs#L1139-L1179
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_mail_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_contact_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_task_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_note_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_journal_folder_type_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
---

# Signature

`pub(in crate::mapi) fn wlink_folder_type_guid(message: &MapiNavigationShortcutMessage) -> [u8; 16]`

# Calls

- [wlink_mail_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_mail_folder_type_guid.md)
- [wlink_contact_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_contact_folder_type_guid.md)
- [wlink_task_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_task_folder_type_guid.md)
- [wlink_note_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_note_folder_type_guid.md)
- [wlink_journal_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_journal_folder_type_guid.md)

# Called by

- [validated_navigation_shortcut_from_mapi_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)
- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)