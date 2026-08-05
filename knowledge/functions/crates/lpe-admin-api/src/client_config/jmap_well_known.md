---
type: Rust Function
title: jmap_well_known
resource: crates/lpe-admin-api/src/client_config.rs#L61-L67
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/jmap_well_known_location
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
---

# Signature

`async fn jmap_well_known(headers: HeaderMap) -> Response`

# Calls

- [jmap_well_known_location](../../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known_location.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)