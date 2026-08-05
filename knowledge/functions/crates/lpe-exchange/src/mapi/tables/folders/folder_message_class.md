---
type: Rust Function
title: folder_message_class
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L3-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_expected_container_class
---

# Signature

`pub(in crate::mapi) fn folder_message_class(mailbox: &JmapMailbox) -> &'static str`

# Calls

- [mailbox_advertised_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id.md)
- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)

# Called by

- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [hierarchy_row_expected_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_expected_container_class.md)