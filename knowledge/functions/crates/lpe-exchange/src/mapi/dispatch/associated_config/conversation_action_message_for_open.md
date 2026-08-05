---
type: Rust Function
title: conversation_action_message_for_open
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L115-L124
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_rejects_default_action_from_wrong_folder
---

# Signature

`pub(super) fn conversation_action_message_for_open( snapshot: &MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<crate::mapi_store::MapiConversationActionMessage>`

# Calls

- [conversation_action_table_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_message_for_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [conversation_action_open_prefers_action_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity.md)
- [conversation_action_open_rejects_default_action_from_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_rejects_default_action_from_wrong_folder.md)