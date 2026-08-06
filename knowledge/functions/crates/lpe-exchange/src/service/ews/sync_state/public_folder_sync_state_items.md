---
type: Rust Function
title: public_folder_sync_state_items
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L615-L659
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_items
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`fn public_folder_sync_state_items(sync_state: &str, collection_id: &str) -> PublicFolderSyncState`

# Calls

- [collaboration_sync_state_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_items.md)

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)