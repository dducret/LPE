---
type: Rust Module
title: client_config
resource: crates/lpe-admin-api/src/client_config.rs#L1-L1230
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-body-bytes-extract-path-query-http-header-content-type-header-location-headermap-headervalue-statuscode-uri-response-intoresponse-response-routing-get-json-router
  - external/lpe-core-outlook-trace-write-outlook-trace-outlooktracedirection-outlooktraceevent
  - external/lpe-storage-storage
  - external/serde-deserialize
  - external/serde-json-json
  - external/std-env
  - external/tracing-info-warn
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [router](../../../../functions/crates/lpe-admin-api/src/client_config/router.md)
- [thunderbird_autoconfig](../../../../functions/crates/lpe-admin-api/src/client_config/thunderbird_autoconfig.md)
- [jmap_well_known](../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known.md)
- [jmap_well_known_location](../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known_location.md)
- [outlook_autodiscover_get](../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get.md)
- [outlook_autodiscover_post](../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [AutodiscoverJsonQuery](../../../../classes/crates/lpe-admin-api/src/client_config/AutodiscoverJsonQuery.md)
- [outlook_autodiscover_json](../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json.md)
- [PublishedEndpoints](../../../../classes/crates/lpe-admin-api/src/client_config/PublishedEndpoints.md)
- [from_headers](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)
- [exchange_autodiscover_enabled](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exchange_autodiscover_enabled.md)
- [mapi_autodiscover_enabled](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled.md)
- [mapi_http_autodiscover_selected](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_http_autodiscover_selected.md)
- [exch_autodiscover_enabled](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled.md)
- [expr_autodiscover_enabled](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled.md)
- [soap_exchange_autodiscover_enabled](../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/soap_exchange_autodiscover_enabled.md)
- [render_autodiscover_json](../../../../functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json.md)
- [supported_autodiscover_json_protocols](../../../../functions/crates/lpe-admin-api/src/client_config/supported_autodiscover_json_protocols.md)
- [autodiscover_json_invalid_protocol_response](../../../../functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response.md)
- [valid_mapi_http_capability](../../../../functions/crates/lpe-admin-api/src/client_config/valid_mapi_http_capability.md)
- [render_thunderbird_autoconfig](../../../../functions/crates/lpe-admin-api/src/client_config/render_thunderbird_autoconfig.md)
- [render_outlook_autodiscover](../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)
- [render_exchange_provider_autodiscover_protocols](../../../../functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols.md)
- [mailbox_server_name](../../../../functions/crates/lpe-admin-api/src/client_config/mailbox_server_name.md)
- [fallback_host](../../../../functions/crates/lpe-admin-api/src/client_config/fallback_host.md)
- [exchange_provider_ews_url_fields](../../../../functions/crates/lpe-admin-api/src/client_config/exchange_provider_ews_url_fields.md)
- [render_mapi_http_autodiscover_protocol](../../../../functions/crates/lpe-admin-api/src/client_config/render_mapi_http_autodiscover_protocol.md)
- [render_ews_web_autodiscover_protocol](../../../../functions/crates/lpe-admin-api/src/client_config/render_ews_web_autodiscover_protocol.md)
- [web_external_protocol](../../../../functions/crates/lpe-admin-api/src/client_config/web_external_protocol.md)
- [render_mobilesync_autodiscover](../../../../functions/crates/lpe-admin-api/src/client_config/render_mobilesync_autodiscover.md)
- [render_soap_user_settings_autodiscover](../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover.md)
- [render_soap_user_settings_response](../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_response.md)
- [soap_string_user_setting](../../../../functions/crates/lpe-admin-api/src/client_config/soap_string_user_setting.md)
- [soap_string_list_user_setting](../../../../functions/crates/lpe-admin-api/src/client_config/soap_string_list_user_setting.md)
- [parse_autodiscover_email](../../../../functions/crates/lpe-admin-api/src/client_config/parse_autodiscover_email.md)
- [requested_soap_user_settings](../../../../functions/crates/lpe-admin-api/src/client_config/requested_soap_user_settings.md)
- [requested_mobilesync_schema](../../../../functions/crates/lpe-admin-api/src/client_config/requested_mobilesync_schema.md)
- [xml_tag_value](../../../../functions/crates/lpe-admin-api/src/client_config/xml_tag_value.md)
- [public_host](../../../../functions/crates/lpe-admin-api/src/client_config/public_host.md)
- [public_scheme](../../../../functions/crates/lpe-admin-api/src/client_config/public_scheme.md)
- [header_value](../../../../functions/crates/lpe-admin-api/src/client_config/header_value.md)
- [safe_header](../../../../functions/crates/lpe-admin-api/src/client_config/safe_header.md)
- [log_autodiscover_connection](../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)
- [trace_autodiscover_connection](../../../../functions/crates/lpe-admin-api/src/client_config/trace_autodiscover_connection.md)
- [host_without_port](../../../../functions/crates/lpe-admin-api/src/client_config/host_without_port.md)
- [read_u16_env](../../../../functions/crates/lpe-admin-api/src/client_config/read_u16_env.md)
- [env_flag](../../../../functions/crates/lpe-admin-api/src/client_config/env_flag.md)
- [email_domain](../../../../functions/crates/lpe-admin-api/src/client_config/email_domain.md)
- [ews_host](../../../../functions/crates/lpe-admin-api/src/client_config/ews_host.md)
- [legacy_user](../../../../functions/crates/lpe-admin-api/src/client_config/legacy_user.md)
- [deployment_id](../../../../functions/crates/lpe-admin-api/src/client_config/deployment_id.md)
- [escape_xml](../../../../functions/crates/lpe-admin-api/src/client_config/escape_xml.md)
- [xml_response](../../../../functions/crates/lpe-admin-api/src/client_config/xml_response.md)

# Imports

- `axum::{
    body::Bytes,
    extract::{Path, Query},
    http::{header::CONTENT_TYPE, header::LOCATION, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
}`
- `lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent}`
- `lpe_storage::Storage`
- `serde::Deserialize`
- `serde_json::json`
- `std::env`
- `tracing::{info, warn}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)