---
type: Rust Function
title: format_view_descriptor_binary_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L649-L716
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_payload_len
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape
---

# Signature

`pub(in crate::mapi::dispatch) fn format_view_descriptor_binary_summary( descriptor: &[u8], ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [view_descriptor_column_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count.md)
- [view_descriptor_column_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags.md)
- [view_descriptor_column_payload_len](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_payload_len.md)

# Called by

- [log_outlook_view_handoff](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [view_handoff_descriptor_summary_reports_outlook_view_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape.md)