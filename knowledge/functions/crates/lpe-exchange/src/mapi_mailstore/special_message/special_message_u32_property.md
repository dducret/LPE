---
type: Rust Function
title: special_message_u32_property
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L54-L63
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status
---

# Signature

`fn special_message_u32_property(object: &SpecialMessageSyncFact, property_tag: u32) -> Option<u32>`

# Calls

- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [special_message_access](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access.md)
- [special_message_access_level](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level.md)
- [special_message_status](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status.md)