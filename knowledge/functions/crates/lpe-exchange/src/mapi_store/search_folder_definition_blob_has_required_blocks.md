---
type: Rust Function
title: search_folder_definition_blob_has_required_blocks
resource: crates/lpe-exchange/src/mapi_store.rs#L1174-L1223
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/common_views_search_folder_definition_is_projectable
---

# Signature

`fn search_folder_definition_blob_has_required_blocks(blob: &[u8]) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [common_views_search_folder_definition_is_projectable](../../../../../functions/crates/lpe-exchange/src/mapi_store/common_views_search_folder_definition_is_projectable.md)