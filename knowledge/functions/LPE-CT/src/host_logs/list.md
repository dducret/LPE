---
type: Rust Function
title: list
resource: LPE-CT/src/host_logs.rs#L67-L94
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/category_definition
  - functions/LPE-CT/src/host_logs/host_log_dir
  - functions/LPE-CT/src/host_logs/discover_log_names
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/host_logs/item_for_name
  - functions/LPE-CT/src/host_logs/virtual_item
  called_by:
  - functions/LPE-CT/src/http_routes/host_logs_list
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_records_terminal_events_after_visible_release
---

# Signature

`pub(crate) fn list(category: &str) -> Result<HostLogList, HostLogError>`

# Calls

- [category_definition](../../../../functions/LPE-CT/src/host_logs/category_definition.md)
- [host_log_dir](../../../../functions/LPE-CT/src/host_logs/host_log_dir.md)
- [discover_log_names](../../../../functions/LPE-CT/src/host_logs/discover_log_names.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [item_for_name](../../../../functions/LPE-CT/src/host_logs/item_for_name.md)
- [virtual_item](../../../../functions/LPE-CT/src/host_logs/virtual_item.md)

# Called by

- [host_logs_list](../../../../functions/LPE-CT/src/http_routes/host_logs_list.md)
- [test_view_trace_records_terminal_events_after_visible_release](../../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_records_terminal_events_after_visible_release.md)