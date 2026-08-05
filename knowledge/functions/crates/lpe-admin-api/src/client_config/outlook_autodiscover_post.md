---
type: Rust Function
title: outlook_autodiscover_post
resource: crates/lpe-admin-api/src/client_config.rs#L93-L148
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/parse_autodiscover_email
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers
  - functions/crates/lpe-admin-api/src/client_config/requested_soap_user_settings
  - functions/crates/lpe-admin-api/src/client_config/requested_mobilesync_schema
  - functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_response
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  - functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection
  - functions/crates/lpe-admin-api/src/client_config/render_mobilesync_autodiscover
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
---

# Signature

`async fn outlook_autodiscover_post(uri: Uri, headers: HeaderMap, body: Bytes) -> Response`

# Calls

- [parse_autodiscover_email](../../../../../functions/crates/lpe-admin-api/src/client_config/parse_autodiscover_email.md)
- [from_headers](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)
- [requested_soap_user_settings](../../../../../functions/crates/lpe-admin-api/src/client_config/requested_soap_user_settings.md)
- [requested_mobilesync_schema](../../../../../functions/crates/lpe-admin-api/src/client_config/requested_mobilesync_schema.md)
- [render_soap_user_settings_response](../../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_response.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)
- [log_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)
- [render_mobilesync_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_mobilesync_autodiscover.md)
- [render_outlook_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)