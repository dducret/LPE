---
type: Rust Function
title: upload_concurrency_limit
resource: crates/lpe-jmap/src/service.rs#L202-L213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/try_acquire_upload_request_permit
  - functions/crates/lpe-jmap/src/error/jmap_problem
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
---

# Signature

`async fn upload_concurrency_limit(request: Request<Body>, next: Next) -> Response`

# Calls

- [try_acquire_upload_request_permit](../../../../../functions/crates/lpe-jmap/src/service/try_acquire_upload_request_permit.md)
- [jmap_problem](../../../../../functions/crates/lpe-jmap/src/error/jmap_problem.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)