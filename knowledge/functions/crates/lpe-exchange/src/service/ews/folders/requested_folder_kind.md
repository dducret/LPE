---
type: Rust Function
title: requested_folder_kind
resource: crates/lpe-exchange/src/service/ews/folders.rs#L522-L586
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state
  - functions/crates/lpe-exchange/src/service/ews/folders/sync_state_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn requested_folder_kind(request: &str) -> Option<FolderKind>`

# Calls

- [requested_sync_state](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state.md)
- [sync_state_folder_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/sync_state_folder_kind.md)
- [requested_mailbox_role](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role.md)
- [requested_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id.md)

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)