---
type: Rust Function
title: xml_response
resource: crates/lpe-exchange/src/service/ews/xml.rs#L7-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
---

# Signature

`pub(in crate::service) fn xml_response(status: StatusCode, body: String) -> Response`

# Calls

- [into_response](../../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)