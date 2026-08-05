---
type: Rust Function
title: rotate_session_request_sequence_token
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L310-L321
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies
---

# Signature

`pub(in crate::mapi) fn rotate_session_request_sequence_token(session_id: &str) -> bool`

# Calls

- [sessions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions.md)
- [prune_expired_sessions_locked](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked.md)

# Called by

- [refresh_accepted_session_response_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)