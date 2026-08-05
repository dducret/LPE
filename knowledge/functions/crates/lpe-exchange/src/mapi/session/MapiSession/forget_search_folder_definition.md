---
type: Rust Method
title: forget_search_folder_definition
resource: crates/lpe-exchange/src/mapi/session.rs#L993-L1005
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
---

# Signature

`pub(in crate::mapi) fn forget_search_folder_definition( &mut self, folder_id: u64, ) -> Option<SearchFolderDefinition>`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)