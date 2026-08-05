---
type: Rust Function
title: default_view_contents_table_initial_sort
resource: crates/lpe-exchange/src/mapi/dispatch/table_open.rs#L462-L476
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_contents_table_starts_with_descriptor_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_default_view_contents_table_uses_sent_to_descriptor_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_contents_table_does_not_start_with_default_view_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_contents_table_does_not_inherit_synthetic_view_sort
---

# Signature

`pub(super) fn default_view_contents_table_initial_sort( folder_id: u64, associated: bool, container_class: &str, ) -> Vec<MapiSortOrder>`

# Calls

- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [outlook_default_folder_named_view_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name.md)
- [outlook_folder_view_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [default_view_contents_table_starts_with_descriptor_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_contents_table_starts_with_descriptor_sort.md)
- [sent_default_view_contents_table_uses_sent_to_descriptor_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_default_view_contents_table_uses_sent_to_descriptor_sort.md)
- [associated_contents_table_does_not_start_with_default_view_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_contents_table_does_not_start_with_default_view_sort.md)
- [calendar_normal_contents_table_does_not_inherit_synthetic_view_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_contents_table_does_not_inherit_synthetic_view_sort.md)