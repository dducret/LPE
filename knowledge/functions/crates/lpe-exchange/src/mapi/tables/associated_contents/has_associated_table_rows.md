---
type: Rust Function
title: has_associated_table_rows
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L157-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
---

# Signature

`pub(super) fn has_associated_table_rows(folder_id: u64, snapshot: &MapiMailStoreSnapshot) -> bool`

# Calls

- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view.md)

# Called by

- [should_use_associated_config_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)