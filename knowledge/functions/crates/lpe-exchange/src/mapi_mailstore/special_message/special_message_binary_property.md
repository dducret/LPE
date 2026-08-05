---
type: Rust Function
title: special_message_binary_property
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L41-L52
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list
---

# Signature

`fn special_message_binary_property( object: &SpecialMessageSyncFact, property_tag: u32, ) -> Option<&[u8]>`

# Called by

- [special_message_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key.md)
- [special_message_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key.md)
- [special_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key.md)
- [special_message_change_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key.md)
- [special_message_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list.md)