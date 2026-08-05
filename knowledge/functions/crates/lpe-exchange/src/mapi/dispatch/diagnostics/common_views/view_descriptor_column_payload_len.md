---
type: Rust Function
title: view_descriptor_column_payload_len
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L770-L791
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary
---

# Signature

`fn view_descriptor_column_payload_len(descriptor: &[u8]) -> Option<usize>`

# Calls

- [view_descriptor_column_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_view_descriptor_binary_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary.md)