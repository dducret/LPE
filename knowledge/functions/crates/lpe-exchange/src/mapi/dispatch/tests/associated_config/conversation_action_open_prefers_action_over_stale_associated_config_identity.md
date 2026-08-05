---
type: Rust Function
title: conversation_action_open_prefers_action_over_stale_associated_config_identity
resource: crates/lpe-exchange/src/mapi/dispatch/tests/associated_config.rs#L60-L105
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_conversation_actions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open
---

# Signature

`fn conversation_action_open_prefers_action_over_stale_associated_config_identity()`

# Calls

- [remember_mapi_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_conversation_actions](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_conversation_actions.md)
- [with_associated_configs](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [conversation_action_message_for_open](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/conversation_action_message_for_open.md)