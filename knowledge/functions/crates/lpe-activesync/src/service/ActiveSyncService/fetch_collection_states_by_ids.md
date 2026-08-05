---
type: Rust Method
title: fetch_collection_states_by_ids
resource: crates/lpe-activesync/src/service.rs#L863-L889
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/pending_page_is_stable
---

# Signature

`async fn fetch_collection_states_by_ids( &self, account_id: Uuid, collection: &CollectionDefinition, ids: &[Uuid], ) -> Result<Vec<ActiveSyncItemState>>`

# Calls

- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [parse_collection_mailbox_id](../../../../../../functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id.md)

# Called by

- [pending_page_is_stable](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/pending_page_is_stable.md)