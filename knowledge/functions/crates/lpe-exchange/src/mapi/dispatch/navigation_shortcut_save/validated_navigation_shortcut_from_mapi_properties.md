---
type: Rust Function
title: validated_navigation_shortcut_from_mapi_properties
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L152-L260
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/required_navigation_shortcut_u32
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/required_navigation_shortcut_binary_16
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_mail_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
---

# Signature

`fn validated_navigation_shortcut_from_mapi_properties( account_id: Uuid, id: Option<Uuid>, properties: &HashMap<u32, MapiValue>, ) -> Result<crate::mapi_store::MapiNavigationShortcutMessage>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [required_navigation_shortcut_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/required_navigation_shortcut_u32.md)
- [required_navigation_shortcut_binary_16](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/required_navigation_shortcut_binary_16.md)
- [navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties.md)
- [wlink_mail_folder_type_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_mail_folder_type_guid.md)
- [wlink_folder_type_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid.md)

# Called by

- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)