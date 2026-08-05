---
type: Rust Function
title: rpc_proxy_wrapped_rop_buffer
resource: crates/lpe-exchange/src/tests/mod.rs#L12433-L12448
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_empty_extended_execute_returns_success
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_extended_execute_release_keeps_handle_table
  - functions/crates/lpe-exchange/src/tests/rpc_proxy_bootstrap_logon_execute_rop
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children
---

# Signature

`fn rpc_proxy_wrapped_rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8>`

# Called by

- [mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_large_getprops_uses_flagged_html_and_open_stream.md)
- [mapi_over_http_empty_extended_execute_returns_success](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_empty_extended_execute_returns_success.md)
- [mapi_over_http_extended_execute_release_keeps_handle_table](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_extended_execute_release_keeps_handle_table.md)
- [rpc_proxy_bootstrap_logon_execute_rop](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy_bootstrap_logon_execute_rop.md)
- [rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children](../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children.md)