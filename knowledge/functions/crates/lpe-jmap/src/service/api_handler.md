---
type: Rust Function
title: api_handler
resource: crates/lpe-jmap/src/service.rs#L150-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/api_request_exceeds_call_limit
  - functions/crates/lpe-jmap/src/error/jmap_problem
  - functions/crates/lpe-jmap/src/service/helpers/validate_declared_capabilities
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn api_handler( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<JmapApiRequest>, ) -> HttpResult<JmapApiResponse>`

# Calls

- [api_request_exceeds_call_limit](../../../../../functions/crates/lpe-jmap/src/service/helpers/api_request_exceeds_call_limit.md)
- [jmap_problem](../../../../../functions/crates/lpe-jmap/src/error/jmap_problem.md)
- [validate_declared_capabilities](../../../../../functions/crates/lpe-jmap/src/service/helpers/validate_declared_capabilities.md)
- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)