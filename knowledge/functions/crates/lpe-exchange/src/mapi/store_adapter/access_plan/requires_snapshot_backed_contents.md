---
type: Rust Function
title: requires_snapshot_backed_contents
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L203-L222
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_snapshot_backed_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mailbox_id_for_mapi_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`pub(in crate::mapi) fn requires_snapshot_backed_contents( plan: &MapiAccessPlan, mailboxes: &[JmapMailbox], ) -> bool`

# Calls

- [is_snapshot_backed_collaboration_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_snapshot_backed_collaboration_folder.md)
- [mailbox_id_for_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mailbox_id_for_mapi_folder_id.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)