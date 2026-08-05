---
type: Rust Function
title: get_session
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L323-L330
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie
---

# Signature

`pub(in crate::mapi) fn get_session(session_id: &str) -> Option<MapiSession>`

# Calls

- [sessions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions.md)
- [prune_expired_sessions_locked](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [request_sequence_cookie_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches.md)
- [sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)