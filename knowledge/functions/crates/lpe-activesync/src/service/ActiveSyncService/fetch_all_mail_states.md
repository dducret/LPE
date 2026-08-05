---
type: Rust Method
title: fetch_all_mail_states
resource: crates/lpe-activesync/src/service.rs#L676-L702
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
---

# Signature

`async fn fetch_all_mail_states( &self, account_id: Uuid, mailbox_id: Uuid, ) -> Result<Vec<ActiveSyncItemState>>`

# Called by

- [collection_state](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)