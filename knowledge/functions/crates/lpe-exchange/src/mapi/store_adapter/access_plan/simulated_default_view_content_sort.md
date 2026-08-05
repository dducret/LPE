---
type: Rust Function
title: simulated_default_view_content_sort
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L688-L698
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`fn simulated_default_view_content_sort(folder_id: u64, associated: bool) -> Vec<MapiSortOrder>`

# Calls

- [is_windowable_mail_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder.md)
- [outlook_default_folder_named_view_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name.md)
- [outlook_folder_view_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders.md)

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)