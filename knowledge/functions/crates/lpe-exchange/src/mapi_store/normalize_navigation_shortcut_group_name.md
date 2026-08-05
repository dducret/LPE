---
type: Rust Function
title: normalize_navigation_shortcut_group_name
resource: crates/lpe-exchange/src/mapi_store.rs#L1115-L1130
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message
---

# Signature

`fn normalize_navigation_shortcut_group_name( section: u32, group_header_id: Option<Uuid>, group_name: &str, ) -> String`

# Calls

- [default_wlink_group_uuid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)

# Called by

- [navigation_shortcut_message](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message.md)