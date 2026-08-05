---
type: Rust Method
title: collection_state
resource: crates/lpe-activesync/src/service.rs#L647-L674
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_all_mail_states
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections
---

# Signature

`async fn collection_state( &self, account_id: Uuid, collection: &CollectionDefinition, ) -> Result<Vec<CollectionStateEntry>>`

# Calls

- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [parse_collection_mailbox_id](../../../../../../functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id.md)
- [fetch_all_mail_states](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_all_mail_states.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [get_item_estimate_response](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
- [changed_ping_collections](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections.md)