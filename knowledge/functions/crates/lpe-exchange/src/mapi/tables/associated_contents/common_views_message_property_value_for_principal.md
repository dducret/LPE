---
type: Rust Function
title: common_views_message_property_value_for_principal
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L439-L466
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_for_principal
---

# Signature

`fn common_views_message_property_value_for_principal( message: &MapiCommonViewsMessage, principal: &AccountPrincipal, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [navigation_shortcut_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)

# Called by

- [serialize_common_views_property_row_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_for_principal.md)