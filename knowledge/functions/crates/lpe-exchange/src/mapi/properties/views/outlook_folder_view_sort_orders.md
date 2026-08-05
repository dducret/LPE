---
type: Rust Function
title: outlook_folder_view_sort_orders
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L314-L332
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort
---

# Signature

`pub(in crate::mapi) fn outlook_folder_view_sort_orders( folder_id: u64, view_name: &str, ) -> Vec<MapiSortOrder>`

# Calls

- [outlook_folder_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [default_view_contents_table_initial_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort.md)
- [simulated_default_view_content_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort.md)