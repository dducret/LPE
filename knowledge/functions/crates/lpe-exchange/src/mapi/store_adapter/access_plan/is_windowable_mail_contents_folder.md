---
type: Rust Function
title: is_windowable_mail_contents_folder
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L787-L806
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_snapshot_backed_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort
---

# Signature

`fn is_windowable_mail_contents_folder(folder_id: u64) -> bool`

# Calls

- [is_snapshot_backed_collaboration_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_snapshot_backed_collaboration_folder.md)
- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [simulated_default_view_content_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort.md)