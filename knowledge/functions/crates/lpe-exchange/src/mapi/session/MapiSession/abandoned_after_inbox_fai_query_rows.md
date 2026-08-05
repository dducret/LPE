---
type: Rust Method
title: abandoned_after_inbox_fai_query_rows
resource: crates/lpe-exchange/src/mapi/session.rs#L585-L595
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn abandoned_after_inbox_fai_query_rows(&self) -> bool`

# Called by

- [outlook_startup_gate_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)