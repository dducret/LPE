---
type: Rust Function
title: is_snapshot_backed_collaboration_folder
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L223-L243
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder
---

# Signature

`fn is_snapshot_backed_collaboration_folder(folder_id: u64) -> bool`

# Called by

- [requires_snapshot_backed_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/requires_snapshot_backed_contents.md)
- [is_windowable_mail_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder.md)