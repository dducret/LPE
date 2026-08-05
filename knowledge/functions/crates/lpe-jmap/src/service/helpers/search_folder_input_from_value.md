---
type: Rust Function
title: search_folder_input_from_value
resource: crates/lpe-jmap/src/service/helpers.rs#L228-L273
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
---

# Signature

`pub(super) fn search_folder_input_from_value( id: Option<Uuid>, account_id: Uuid, value: &Value, ) -> Result<UpsertSearchFolderInput>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_search_folder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)