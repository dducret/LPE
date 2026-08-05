---
type: Rust Function
title: requested_sync_state
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L696-L698
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id
---

# Signature

`pub(in crate::service) fn requested_sync_state(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [requested_mailbox_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids.md)
- [requested_folder_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)
- [requested_sync_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id.md)