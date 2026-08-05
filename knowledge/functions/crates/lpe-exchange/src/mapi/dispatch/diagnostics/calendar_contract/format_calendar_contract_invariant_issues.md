---
type: Rust Function
title: format_calendar_contract_invariant_issues
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L333-L381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_sort_direction_matches_column_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
---

# Signature

`fn format_calendar_contract_invariant_issues( view: Option<&crate::mapi_store::MapiCommonViewNamedViewMessage>, descriptor: &[u8], descriptor_strings: &[u8], entry_id_target: Option<(u64, u64)>, named_id_reuse: &str, fai_inventory: &str, ) -> String`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [view_descriptor_sort_direction_matches_column_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_sort_direction_matches_column_flags.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)