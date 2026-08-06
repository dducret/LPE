---
type: Rust Function
title: wlink_mail_folder_type_guid
resource: crates/lpe-exchange/src/mapi/properties.rs#L1096-L1102
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid
---

# Signature

`pub(in crate::mapi) fn wlink_mail_folder_type_guid() -> [u8; 16]`

# Called by

- [validated_navigation_shortcut_from_mapi_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)
- [wlink_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid.md)