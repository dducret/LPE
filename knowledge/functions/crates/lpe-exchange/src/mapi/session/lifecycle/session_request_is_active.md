---
type: Rust Function
title: session_request_is_active
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L49-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/active_session_requests
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
---

# Signature

`pub(in crate::mapi) fn session_request_is_active(session_id: &str) -> bool`

# Calls

- [active_session_requests](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/active_session_requests.md)

# Called by

- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)