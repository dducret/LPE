---
type: Rust Function
title: handle_mapi
resource: crates/lpe-exchange/src/mapi/transport.rs#L71-L370
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
  - functions/crates/lpe-exchange/src/mapi/transport/headers/request_type
  - functions/crates/lpe-exchange/src/mapi/transport/headers/request_id
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/headers/is_guid_counter_header
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_info
  - functions/crates/lpe-exchange/src/mapi/transport/headers/host_header
  - functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_header
  - functions/crates/lpe-exchange/src/mapi/transport/headers/is_valid_content_length
  - functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_matches_body
  - functions/crates/lpe-exchange/src/mapi/transport/headers/is_mapi_content_type
  - functions/crates/lpe-exchange/src/mapi/wire/MapiHttpRequestType/requires_nspi_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies
---

# Signature

`pub(crate) async fn handle_mapi<S, V>( store: &S, validator: &Validator<V>, endpoint: MapiEndpoint, headers: &HeaderMap, _body: &[u8], ) -> Result<Response> where S: ExchangeStore + Clone + Send + Sync + 'static, V: Detector,`

# Calls

- [authenticate_account](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [request_type](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/request_type.md)
- [request_id](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/request_id.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [finalize_mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response.md)
- [log_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [is_guid_counter_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_guid_counter_header.md)
- [client_info](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_info.md)
- [host_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/host_header.md)
- [content_length_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_header.md)
- [is_valid_content_length](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_valid_content_length.md)
- [content_length_matches_body](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_matches_body.md)
- [is_mapi_content_type](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/is_mapi_content_type.md)
- [requires_nspi_session](../../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiHttpRequestType/requires_nspi_session.md)
- [established_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [log_session_cookie_lookup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [connect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [disconnect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [notification_wait_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [ping_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
- [refresh_accepted_session_response_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)