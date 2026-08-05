---
type: Rust Function
title: ensure_virtual_local_freebusy_message
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L556-L565
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/virtual_local_freebusy_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages
---

# Signature

`pub(super) fn ensure_virtual_local_freebusy_message( messages: &mut Vec<MapiDelegateFreeBusyMessage>, )`

# Calls

- [is_outlook_local_freebusy_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [virtual_local_freebusy_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/virtual_local_freebusy_message.md)

# Called by

- [with_delegate_freebusy_messages](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages.md)