---
type: Rust Function
title: view_descriptor_column_count
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L763-L768
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_handoff_invariant_warnings
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_payload_len
---

# Signature

`fn view_descriptor_column_count(descriptor: &[u8]) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_view_handoff_invariant_warnings](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_handoff_invariant_warnings.md)
- [format_view_descriptor_binary_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary.md)
- [view_descriptor_column_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags.md)
- [view_descriptor_column_payload_len](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_payload_len.md)