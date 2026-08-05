---
type: Rust Module
title: transport
resource: crates/lpe-exchange/src/tests/mapi_over_http/transport.rs#L1-L1740
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/tokio-stream-streamext
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [mapi_over_http_connect_creates_emsmdb_session](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_creates_emsmdb_session.md)
- [mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence.md)
- [mapi_over_http_store_load_failure_after_logon_is_unknown_failure_with_session_cookies](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_store_load_failure_after_logon_is_unknown_failure_with_session_cookies.md)
- [mapi_over_http_malformed_execute_body_is_invalid_body_with_session_cookies](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_malformed_execute_body_is_invalid_body_with_session_cookies.md)
- [mapi_over_http_transport_echoes_request_id_and_client_info](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_echoes_request_id_and_client_info.md)
- [mapi_over_http_transport_maps_response_code_to_header_and_envelope](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_transport_maps_response_code_to_header_and_envelope.md)
- [mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect.md)
- [mapi_over_http_reconnect_invalidates_previous_context](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_reconnect_invalidates_previous_context.md)
- [mapi_over_http_execute_prefers_latest_duplicate_session_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_prefers_latest_duplicate_session_cookie.md)
- [mapi_over_http_execute_prefers_latest_cookie_header](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_prefers_latest_cookie_header.md)
- [mapi_over_http_rejects_missing_request_id_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_request_id_with_parseable_error.md)
- [mapi_over_http_rejects_missing_request_type_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_request_type_with_parseable_error.md)
- [mapi_over_http_rejects_unknown_request_type_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_unknown_request_type_with_parseable_error.md)
- [mapi_over_http_rejects_missing_client_info_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_client_info_with_parseable_error.md)
- [mapi_over_http_rejects_invalid_client_info_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_client_info_with_parseable_error.md)
- [mapi_over_http_rejects_missing_host_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_host_with_parseable_error.md)
- [mapi_over_http_rejects_missing_content_length_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_content_length_with_parseable_error.md)
- [mapi_over_http_response_content_length_covers_full_mapi_envelope](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_response_content_length_covers_full_mapi_envelope.md)
- [mapi_over_http_rejects_invalid_content_length_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_content_length_with_parseable_error.md)
- [mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation.md)
- [mapi_over_http_rejects_invalid_request_id_with_parseable_error](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_invalid_request_id_with_parseable_error.md)
- [mapi_over_http_rejects_missing_content_type](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_missing_content_type.md)
- [mapi_over_http_disconnect_consumes_emsmdb_session](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_disconnect_consumes_emsmdb_session.md)
- [mapi_over_http_execute_rejects_missing_and_malformed_session_cookies](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_rejects_missing_and_malformed_session_cookies.md)
- [mapi_over_http_disconnect_rejects_stale_session_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_disconnect_rejects_stale_session_cookie.md)
- [mapi_over_http_notification_wait_accepts_prior_sequence_and_does_not_block_execute](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_notification_wait_accepts_prior_sequence_and_does_not_block_execute.md)
- [mapi_over_http_notification_wait_streams_processing_and_pending_frames](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_notification_wait_streams_processing_and_pending_frames.md)
- [mapi_over_http_microsoft_oxcmapihttp_ping_refreshes_idle_session_context](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmapihttp_ping_refreshes_idle_session_context.md)
- [mapi_over_http_ping_accepts_an_earlier_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_accepts_an_earlier_sequence_cookie.md)
- [mapi_over_http_ping_rejects_nonzero_content_length](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_ping_rejects_nonzero_content_length.md)
- [mapi_over_http_execute_and_replay_refresh_session_cookies](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_execute_and_replay_refresh_session_cookies.md)
- [mapi_over_http_replays_duplicate_execute_request_without_rerunning_rops](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_replays_duplicate_execute_request_without_rerunning_rops.md)
- [mapi_over_http_rejects_duplicate_execute_request_id_with_different_body](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_duplicate_execute_request_id_with_different_body.md)
- [mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence.md)
- [mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object.md)
- [mapi_over_http_open_attachment_rejects_invalid_microsoft_flags_without_batch_drift](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_open_attachment_rejects_invalid_microsoft_flags_without_batch_drift.md)
- [mapi_options_handler_reports_transport_session_ready](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_options_handler_reports_transport_session_ready.md)
- [mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags.md)
- [mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect.md)

# Imports

- `super::*`
- `tokio_stream::StreamExt`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)