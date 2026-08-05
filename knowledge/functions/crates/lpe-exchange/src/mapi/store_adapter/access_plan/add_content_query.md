---
type: Rust Function
title: add_content_query
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L717-L768
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/content_query_ranges_can_merge
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_seek_total_query_with_following_query_rows_window
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_overlapping_content_windows
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_content_window_that_bridges_existing_ranges
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_inside_existing_content_window
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_inside_later_content_window
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_before_existing_content_window_without_widening
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_before_later_content_window_without_widening
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probes_at_different_offsets
---

# Signature

`pub(in crate::mapi) fn add_content_query( plan: &mut MapiAccessPlan, folder_id: u64, view_signature: u64, offset: usize, limit: usize, sort_orders: Vec<MapiContentTableSort>, )`

# Calls

- [content_query_ranges_can_merge](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/content_query_ranges_can_merge.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [access_plan_merges_seek_total_query_with_following_query_rows_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_seek_total_query_with_following_query_rows_window.md)
- [access_plan_merges_overlapping_content_windows](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_overlapping_content_windows.md)
- [access_plan_merges_content_window_that_bridges_existing_ranges](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_content_window_that_bridges_existing_ranges.md)
- [access_plan_merges_total_probe_inside_existing_content_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_inside_existing_content_window.md)
- [access_plan_merges_existing_total_probe_inside_later_content_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_inside_later_content_window.md)
- [access_plan_merges_total_probe_before_existing_content_window_without_widening](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_before_existing_content_window_without_widening.md)
- [access_plan_merges_existing_total_probe_before_later_content_window_without_widening](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_before_later_content_window_without_widening.md)
- [access_plan_merges_total_probes_at_different_offsets](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probes_at_different_offsets.md)