---
type: Rust Function
title: pending_common_views_message_is_navigation_shortcut
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L98-L110
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
---

# Signature

`fn pending_common_views_message_is_navigation_shortcut( properties: &HashMap<u32, MapiValue>, ) -> bool`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)