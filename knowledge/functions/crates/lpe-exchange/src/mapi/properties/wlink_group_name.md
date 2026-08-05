---
type: Rust Function
title: wlink_group_name
resource: crates/lpe-exchange/src/mapi/properties.rs#L1040-L1054
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id
---

# Signature

`fn wlink_group_name(message: &MapiNavigationShortcutMessage) -> String`

# Calls

- [default_wlink_group_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)

# Called by

- [navigation_shortcut_property_value_with_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_with_store_entry_id.md)