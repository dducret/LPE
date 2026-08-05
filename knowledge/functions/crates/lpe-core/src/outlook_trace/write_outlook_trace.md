---
type: Rust Function
title: write_outlook_trace
resource: crates/lpe-core/src/outlook_trace.rs#L68-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
  - functions/crates/lpe-exchange/src/service/ews/dispatch/trace_ews_event
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection
---

# Signature

`pub fn write_outlook_trace(event: &OutlookTraceEvent<'_>)`

# Calls

- [write_outlook_trace_with_config](../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config.md)

# Called by

- [trace_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection.md)
- [log_post_common_views_handoff_execute_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [trace_mapi_connection](../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [trace_ews_event](../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/trace_ews_event.md)
- [trace_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection.md)