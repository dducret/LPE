---
type: Rust Function
title: outlook_autodiscover_json
resource: crates/lpe-admin-api/src/client_config.rs#L156-L181
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers
  - functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response
  - functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_accepts_outlook_redirect_count_request
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_handler_rejects_rest_request_with_redirect_count
---

# Signature

`async fn outlook_autodiscover_json( uri: Uri, headers: HeaderMap, Path(email): Path<String>, Query(query): Query<AutodiscoverJsonQuery>, ) -> Response`

# Calls

- [from_headers](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)
- [render_autodiscover_json](../../../../../functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json.md)
- [autodiscover_json_invalid_protocol_response](../../../../../functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response.md)
- [log_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)

# Called by

- [autodiscover_json_accepts_outlook_redirect_count_request](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_accepts_outlook_redirect_count_request.md)
- [autodiscover_json_handler_rejects_rest_request_with_redirect_count](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_handler_rejects_rest_request_with_redirect_count.md)