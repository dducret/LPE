---
type: Rust Function
title: parse_collection_mailbox_id
resource: crates/lpe-activesync/src/snapshot.rs#L623-L627
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item
---

# Signature

`pub(crate) fn parse_collection_mailbox_id(collection: &CollectionDefinition) -> Result<Uuid>`

# Called by

- [collection_state](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [fetch_collection_states_by_ids](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids.md)
- [apply_mail_sync_commands](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)
- [handle_move_item](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)