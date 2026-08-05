---
type: Rust Function
title: cookie_value_debug
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L152-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_suffix
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn cookie_value_debug(value: Option<&str>) -> CookieValueDebug`

# Calls

- [cookie_value_suffix](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_suffix.md)

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [log_mapi_session_establish](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish.md)
- [request_cookie_transport_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [session_cookie_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)