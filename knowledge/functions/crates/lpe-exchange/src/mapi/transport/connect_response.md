---
type: Rust Function
title: connect_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L386-L424
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/transport/connect_auxiliary_buffer
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_connect_body_debug
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_stale_session_context
  - functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_an_expired_session_context
  - functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_session_context_for_another_endpoint_or_principal
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) fn connect_response( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, ) -> Response`

# Calls

- [reconnect_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [create_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [log_mapi_session_establish](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [write_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [connect_auxiliary_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_auxiliary_buffer.md)
- [log_connect_body_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_connect_body_debug.md)
- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)

# Called by

- [connect_rejects_a_stale_session_context](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_stale_session_context.md)
- [connect_rejects_an_expired_session_context](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_an_expired_session_context.md)
- [connect_rejects_a_session_context_for_another_endpoint_or_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_a_session_context_for_another_endpoint_or_principal.md)
- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)