---
type: Rust Function
title: default_post_message_class_for_container_class
resource: crates/lpe-exchange/src/mapi/properties.rs#L637-L654
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
---

# Signature

`pub(in crate::mapi) fn default_post_message_class_for_container_class( container_class: &str, ) -> Option<&'static str>`

# Called by

- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [public_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [special_folder_property_value_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)