---
type: Rust Function
title: record_execute_stream_batch_observation
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L305-L332
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_stream_batch_observed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn record_execute_stream_batch_observation( principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, request_handle_table_summary: &str, session: &mut MapiSession, )`

# Calls

- [record_outlook_stream_batch_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_stream_batch_observed.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)