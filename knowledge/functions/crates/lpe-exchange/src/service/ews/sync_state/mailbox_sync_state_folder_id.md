---
type: Rust Function
title: mailbox_sync_state_folder_id
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L765-L769
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids
---

# Signature

`pub(in crate::service) fn mailbox_sync_state_folder_id(sync_state: &str) -> Option<Uuid>`

# Called by

- [requested_mailbox_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids.md)