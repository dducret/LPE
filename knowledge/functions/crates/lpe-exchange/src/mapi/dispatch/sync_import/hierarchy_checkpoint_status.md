---
type: Rust Function
title: hierarchy_checkpoint_status
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L863-L896
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(super) fn hierarchy_checkpoint_status( checkpoint_kind: MapiCheckpointKind, folder_id: u64, checkpoint: &MapiSyncCheckpoint, ) -> &'static str`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)