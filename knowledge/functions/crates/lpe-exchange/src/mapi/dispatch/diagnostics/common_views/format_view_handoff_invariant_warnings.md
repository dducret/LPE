---
type: Rust Function
title: format_view_handoff_invariant_warnings
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L151-L183
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
---

# Signature

`fn format_view_handoff_invariant_warnings( folder_id: u64, message: &crate::mapi_store::MapiCommonViewNamedViewMessage, descriptor_binary: &[u8], descriptor_strings: &str, folder_local_default_visible_in_fai_table: bool, ) -> String`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [view_descriptor_column_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_count.md)

# Called by

- [log_outlook_view_handoff](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)