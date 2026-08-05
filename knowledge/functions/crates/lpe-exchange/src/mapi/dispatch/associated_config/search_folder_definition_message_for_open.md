---
type: Rust Function
title: search_folder_definition_message_for_open
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L152-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(super) fn search_folder_definition_message_for_open( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<SearchFolderDefinition>`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)