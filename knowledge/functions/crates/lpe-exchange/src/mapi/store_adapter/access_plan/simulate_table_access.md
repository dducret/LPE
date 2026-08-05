---
type: Rust Function
title: simulate_table_access
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L279-L686
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_allocate_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_content_table_sort_orders
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_content_query
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_no_advance
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_origin
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_row_count
  - functions/crates/lpe-exchange/src/mapi/session/synchronization_context_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_seek_uses_content_window_total
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_find_row_stays_selective
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_contents_find_row_still_requires_full_snapshot
---

# Signature

`pub(in crate::mapi) fn simulate_table_access( plan: &mut MapiAccessPlan, session: &MapiSession, handles: &mut HashMap<u32, MapiObject>, next_handle: &mut u32, handle_slots: &mut Vec<u32>, request: &RopRequest, )`

# Calls

- [resolve_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [simulate_allocate_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_allocate_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [default_hierarchy_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns.md)
- [simulated_default_view_content_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulated_default_view_content_sort.md)
- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [is_windowable_mail_contents_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/is_windowable_mail_contents_folder.md)
- [mapi_content_table_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_content_table_sort_orders.md)
- [add_content_query](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_content_query.md)
- [table_view_signature](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/table_view_signature.md)
- [sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders.md)
- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)
- [query_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [query_forward_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [query_no_advance](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_no_advance.md)
- [seek_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_origin.md)
- [seek_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_row_count.md)
- [synchronization_context_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/synchronization_context_state.md)

# Called by

- [extend_access_plan_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
- [access_plan_normal_mail_contents_seek_uses_content_window_total](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_seek_uses_content_window_total.md)
- [access_plan_associated_contents_find_row_stays_selective](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_find_row_stays_selective.md)
- [access_plan_normal_contents_find_row_still_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_contents_find_row_still_requires_full_snapshot.md)