---
type: Rust Function
title: strict_test_replid_globset
resource: crates/lpe-exchange/src/tests/mod.rs#L13688-L13697
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_deleted_message_idset_property
  - functions/crates/lpe-exchange/src/tests/mapi_read_message_idset_property
  - functions/crates/lpe-exchange/src/tests/mapi_unread_message_idset_property
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta
---

# Signature

`fn strict_test_replid_globset(counters: &[u64]) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_deleted_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_deleted_message_idset_property.md)
- [mapi_read_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_read_message_idset_property.md)
- [mapi_unread_message_idset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_unread_message_idset_property.md)
- [strict_hierarchy_decoder_accepts_deletion_only_delta](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta.md)