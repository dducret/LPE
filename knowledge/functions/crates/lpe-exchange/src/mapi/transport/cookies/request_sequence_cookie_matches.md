---
type: Rust Function
title: request_sequence_cookie_matches
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L32-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
---

# Signature

`pub(in crate::mapi) fn request_sequence_cookie_matches( endpoint: MapiEndpoint, headers: &HeaderMap, session_id: &str, ) -> bool`

# Calls

- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [request_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie.md)

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)