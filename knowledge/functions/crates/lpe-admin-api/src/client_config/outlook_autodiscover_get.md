---
type: Rust Function
title: outlook_autodiscover_get
resource: crates/lpe-admin-api/src/client_config.rs#L73-L91
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
  - functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection
---

# Signature

`async fn outlook_autodiscover_get(uri: Uri, headers: HeaderMap) -> Response`

# Calls

- [from_headers](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)
- [render_outlook_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)
- [log_autodiscover_connection](../../../../../functions/crates/lpe-admin-api/src/client_config/log_autodiscover_connection.md)