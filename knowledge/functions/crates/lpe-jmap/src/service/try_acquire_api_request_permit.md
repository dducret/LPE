---
type: Rust Function
title: try_acquire_api_request_permit
resource: crates/lpe-jmap/src/service.rs#L194-L200
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/api_concurrency_limit
  - functions/crates/lpe-jmap/src/tests/api_request_concurrency_permits_match_advertised_limit
---

# Signature

`pub(crate) fn try_acquire_api_request_permit() -> Option<OwnedSemaphorePermit>`

# Called by

- [api_concurrency_limit](../../../../../functions/crates/lpe-jmap/src/service/api_concurrency_limit.md)
- [api_request_concurrency_permits_match_advertised_limit](../../../../../functions/crates/lpe-jmap/src/tests/api_request_concurrency_permits_match_advertised_limit.md)