---
type: Rust Function
title: mapi_parent_folder_id
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L113-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree
---

# Signature

`pub(super) fn mapi_parent_folder_id(mailbox: &JmapMailbox) -> u64`

# Calls

- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [mailbox_advertised_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [hierarchy_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [mailbox_shadowed_by_active_outlook_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder.md)
- [hierarchy_row_parent_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id.md)
- [hierarchy_folder_is_in_ipm_subtree](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_folder_is_in_ipm_subtree.md)