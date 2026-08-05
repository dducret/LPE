---
type: Rust Function
title: mailbox_projects_hidden_attribute
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L29-L34
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
---

# Signature

`pub(in crate::mapi) fn mailbox_projects_hidden_attribute(mailbox: &JmapMailbox) -> bool`

# Called by

- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)