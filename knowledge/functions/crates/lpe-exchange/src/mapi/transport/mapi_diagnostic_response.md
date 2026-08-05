---
type: Rust Function
title: mapi_diagnostic_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L638-L651
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_disabled_mutation_response
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
---

# Signature

`pub(in crate::mapi) fn mapi_diagnostic_response( request_type: &str, request_id: &str, response_code: u16, message: &str, ) -> Response`

# Calls

- [mapi_diagnostic_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
- [nspi_disabled_mutation_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_disabled_mutation_response.md)
- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_dn_to_mid_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_dn_to_mid_response.md)
- [nspi_get_prop_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_minimal_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_minimal_ids_response.md)
- [nspi_template_info_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_template_info_response.md)
- [reconnect_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [established_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [mapi_error_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_error_response.md)
- [disconnect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [notification_wait_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)