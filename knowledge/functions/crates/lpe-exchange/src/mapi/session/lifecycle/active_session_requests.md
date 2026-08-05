---
type: Rust Function
title: active_session_requests
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L12-L14
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/ActiveSessionRequest/drop/drop
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_request_is_active
---

# Signature

`pub(in crate::mapi) fn active_session_requests() -> &'static Mutex<HashSet<String>>`

# Called by

- [drop](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/ActiveSessionRequest/drop/drop.md)
- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [session_request_is_active](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_request_is_active.md)