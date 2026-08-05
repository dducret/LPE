---
type: Rust Function
title: outlook_default_folder_named_view_name
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L202-L215
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort
---

# Signature

`pub(crate) fn outlook_default_folder_named_view_name(folder_id: u64) -> &'static str`

# Called by

- [default_view_contents_table_initial_sort](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort.md)
- [simulated_default_view_content_sort](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort.md)