---
type: Rust Function
title: sync_checkpoint_kind
resource: crates/lpe-exchange/src/mapi/sync.rs#L118-L124
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
---

# Signature

`pub(in crate::mapi) fn sync_checkpoint_kind(sync_type: u8) -> MapiCheckpointKind`

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_synchronization_open_collector_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)