---
type: Rust Function
title: mailbox_id_for_mapi_folder_id
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L1015-L1023
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents
---

# Signature

`pub(in crate::mapi) fn mailbox_id_for_mapi_folder_id( mailboxes: &[JmapMailbox], folder_id: u64, ) -> Option<Uuid>`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [requires_snapshot_backed_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents.md)