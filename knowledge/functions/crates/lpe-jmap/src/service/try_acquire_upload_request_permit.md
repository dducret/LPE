---
type: Rust Function
title: try_acquire_upload_request_permit
resource: crates/lpe-jmap/src/service.rs#L215-L221
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/upload_concurrency_limit
  - functions/crates/lpe-jmap/src/tests/upload_concurrency_permits_match_advertised_limit
---

# Signature

`pub(crate) fn try_acquire_upload_request_permit() -> Option<OwnedSemaphorePermit>`

# Called by

- [upload_concurrency_limit](../../../../../functions/crates/lpe-jmap/src/service/upload_concurrency_limit.md)
- [upload_concurrency_permits_match_advertised_limit](../../../../../functions/crates/lpe-jmap/src/tests/upload_concurrency_permits_match_advertised_limit.md)