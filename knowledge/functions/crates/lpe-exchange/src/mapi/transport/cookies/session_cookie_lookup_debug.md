---
type: Rust Function
title: session_cookie_lookup_debug
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L119-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection
  - functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch
---

# Signature

`pub(in crate::mapi) fn session_cookie_lookup_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, ) -> SessionCookieLookupDebug`

# Calls

- [request_named_cookie_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates.md)
- [cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name.md)
- [sequence_cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name.md)
- [cookie_value_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)

# Called by

- [log_session_cookie_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection.md)
- [session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch.md)