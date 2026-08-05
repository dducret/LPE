---
type: Rust Function
title: required_navigation_shortcut_binary_16
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L138-L150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
---

# Signature

`fn required_navigation_shortcut_binary_16( properties: &HashMap<u32, MapiValue>, property_tag: u32, ) -> Result<[u8; 16]>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [validated_navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)