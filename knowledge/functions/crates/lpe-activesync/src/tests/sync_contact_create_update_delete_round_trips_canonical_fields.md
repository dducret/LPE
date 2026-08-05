---
type: Rust Function
title: sync_contact_create_update_delete_round_trips_canonical_fields
resource: crates/lpe-activesync/src/tests.rs#L5765-L5845
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/tests/handle_sync_node
  - functions/crates/lpe-activesync/src/tests/sync_commands_node
  - functions/crates/lpe-activesync/src/tests/only_sync_collection
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/tests/collection_sync_key
---

# Signature

`async fn sync_contact_create_update_delete_round_trips_canonical_fields()`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [handle_sync_node](../../../../../functions/crates/lpe-activesync/src/tests/handle_sync_node.md)
- [sync_commands_node](../../../../../functions/crates/lpe-activesync/src/tests/sync_commands_node.md)
- [only_sync_collection](../../../../../functions/crates/lpe-activesync/src/tests/only_sync_collection.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [collection_sync_key](../../../../../functions/crates/lpe-activesync/src/tests/collection_sync_key.md)