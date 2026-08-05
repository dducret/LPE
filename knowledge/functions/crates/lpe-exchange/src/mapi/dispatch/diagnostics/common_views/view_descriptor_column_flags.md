---
type: Rust Function
title: view_descriptor_column_flags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L734-L761
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_sort_direction_matches_column_flags
---

# Signature

`fn view_descriptor_column_flags(descriptor: &[u8], target: u32) -> Option<u32>`

# Calls

- [view_descriptor_column_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_view_descriptor_binary_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary.md)
- [view_descriptor_sort_direction_matches_column_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_sort_direction_matches_column_flags.md)