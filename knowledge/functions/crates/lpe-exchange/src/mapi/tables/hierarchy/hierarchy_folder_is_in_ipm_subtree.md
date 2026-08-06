---
type: Rust Function
title: hierarchy_folder_is_in_ipm_subtree
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L526-L551
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags
---

# Signature

`fn hierarchy_folder_is_in_ipm_subtree(folder_id: u64, mailboxes: &[JmapMailbox]) -> bool`

# Calls

- [try_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [mapi_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_parent_folder_id.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)

# Called by

- [hierarchy_row_folder_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags.md)