---
type: Rust Method
title: with_conversation_actions
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L170-L184
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_conversation_actions( mut self, conversation_actions: Vec<ConversationAction>, ) -> Self`

# Called by

- [conversation_action_open_prefers_action_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)