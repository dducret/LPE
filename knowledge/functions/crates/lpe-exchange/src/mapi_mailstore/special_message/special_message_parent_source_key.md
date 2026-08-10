---
type: Rust Function
title: special_message_parent_source_key
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L97-L101
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_parent_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`pub(super) fn special_message_parent_source_key(object: &SpecialMessageSyncFact) -> Vec<u8>`

# Calls

- [special_message_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_binary_property.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [special_message_sync_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_parent_source_key.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)