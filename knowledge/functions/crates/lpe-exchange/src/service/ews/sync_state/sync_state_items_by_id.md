---
type: Rust Function
title: sync_state_items_by_id
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L706-L713
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn sync_state_items_by_id( items: &[SyncStateItem], ) -> HashMap<Uuid, Option<String>>`

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)