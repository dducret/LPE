---
type: Rust Function
title: conversation_action_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L855-L897
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`fn conversation_action_sync_object( message: &crate::mapi_store::MapiConversationActionMessage, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [conversation_action_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/conversation_action_property_value.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [conversation_action_subject](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/conversation_action_subject.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)