---
type: Rust Method
title: status
resource: LPE-CT/src/host_logs.rs#L58-L60
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery
  - functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge
  - functions/LPE-CT/src/http_routes/host_log_api_error
  - functions/LPE-CT/src/observability/observe_http
  - functions/LPE-CT/src/readiness/check_optional_http_dependency
  - functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message
  - functions/LPE-CT/src/submission/authenticate_smtp_client
  - functions/LPE-CT/src/submission/submit_message
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
  - functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection
  - functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection
  - functions/crates/lpe-admin-api/src/observability/observe_http
  - functions/crates/lpe-admin-api/src/readiness/check_optional_http_dependency
  - functions/crates/lpe-admin-api/src/snapshots/create_snapshot
  - functions/crates/lpe-admin-api/src/snapshots/restore_snapshot
  - functions/crates/lpe-cli/src/send_outbound_handoff
  - functions/crates/lpe-dav/src/responses/options_response
  - functions/crates/lpe-dav/src/responses/redirect_response
  - functions/crates/lpe-dav/src/responses/response_with_headers
  - functions/crates/lpe-dav/src/responses/status_only
  - functions/crates/lpe-dav/src/responses/status_with_etag
  - functions/crates/lpe-dav/src/responses/error_response
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
  - functions/crates/lpe-exchange/src/service/ews/diagnostics/log_ews_connection
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  - functions/crates/lpe-storage/src/storage_backend/s3_probe_pool
---

# Signature

`pub(crate) fn status(&self) -> StatusCode`

# Called by

- [probe_lpe_core_delivery](../../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery.md)
- [probe_lpe_recipient_bridge](../../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge.md)
- [host_log_api_error](../../../../../functions/LPE-CT/src/http_routes/host_log_api_error.md)
- [observe_http](../../../../../functions/LPE-CT/src/observability/observe_http.md)
- [check_optional_http_dependency](../../../../../functions/LPE-CT/src/readiness/check_optional_http_dependency.md)
- [deliver_inbound_message](../../../../../functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message.md)
- [authenticate_smtp_client](../../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)
- [submit_message](../../../../../functions/LPE-CT/src/submission/submit_message.md)
- [verify_recipient_with_core](../../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)
- [log_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)
- [trace_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection.md)
- [observe_http](../../../../../functions/crates/lpe-admin-api/src/observability/observe_http.md)
- [check_optional_http_dependency](../../../../../functions/crates/lpe-admin-api/src/readiness/check_optional_http_dependency.md)
- [create_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/create_snapshot.md)
- [restore_snapshot](../../../../../functions/crates/lpe-admin-api/src/snapshots/restore_snapshot.md)
- [send_outbound_handoff](../../../../../functions/crates/lpe-cli/src/send_outbound_handoff.md)
- [options_response](../../../../../functions/crates/lpe-dav/src/responses/options_response.md)
- [redirect_response](../../../../../functions/crates/lpe-dav/src/responses/redirect_response.md)
- [response_with_headers](../../../../../functions/crates/lpe-dav/src/responses/response_with_headers.md)
- [status_only](../../../../../functions/crates/lpe-dav/src/responses/status_only.md)
- [status_with_etag](../../../../../functions/crates/lpe-dav/src/responses/status_with_etag.md)
- [error_response](../../../../../functions/crates/lpe-dav/src/responses/error_response.md)
- [log_mapi_connection](../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [trace_mapi_connection](../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [log_ews_connection](../../../../../functions/crates/lpe-exchange/src/service/ews/diagnostics/log_ews_connection.md)
- [log_mapi_transport_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)
- [log_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)
- [trace_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql.md)
- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [s3_probe_pool](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)