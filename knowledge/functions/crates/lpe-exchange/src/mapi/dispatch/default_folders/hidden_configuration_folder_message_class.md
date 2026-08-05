---
type: Rust Function
title: hidden_configuration_folder_message_class
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L416-L437
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_projects_hidden_attribute
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
---

# Signature

`fn hidden_configuration_folder_message_class( folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<&'static str>`

# Calls

- [try_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [mailbox_projects_hidden_attribute](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_projects_hidden_attribute.md)
- [folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)

# Called by

- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)