---
type: Rust Function
title: request_cookie_transport_debug
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L96-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
---

# Signature

`pub(crate) fn request_cookie_transport_debug( endpoint: MapiEndpoint, headers: &HeaderMap, ) -> RequestCookieTransportDebug`

# Calls

- [request_named_cookie_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates.md)
- [cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name.md)
- [sequence_cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name.md)
- [cookie_value_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [log_mapi_transport_connection](../../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)