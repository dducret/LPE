---
type: Rust Function
title: contents_table_object_with_default_view_sort
resource: crates/lpe-exchange/src/mapi/dispatch/table_open.rs#L446-L460
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_contents_table_starts_with_descriptor_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_default_view_contents_table_uses_sent_to_descriptor_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_contents_table_does_not_start_with_default_view_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_contents_table_does_not_inherit_synthetic_view_sort
---

# Signature

`pub(super) fn contents_table_object_with_default_view_sort( folder_id: u64, associated: bool, sort_orders: Vec<MapiSortOrder>, ) -> MapiObject`

# Calls

- [contents_table_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_object.md)

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [default_view_contents_table_starts_with_descriptor_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_contents_table_starts_with_descriptor_sort.md)
- [sent_default_view_contents_table_uses_sent_to_descriptor_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_default_view_contents_table_uses_sent_to_descriptor_sort.md)
- [associated_contents_table_does_not_start_with_default_view_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_contents_table_does_not_start_with_default_view_sort.md)
- [calendar_normal_contents_table_does_not_inherit_synthetic_view_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_contents_table_does_not_inherit_synthetic_view_sort.md)