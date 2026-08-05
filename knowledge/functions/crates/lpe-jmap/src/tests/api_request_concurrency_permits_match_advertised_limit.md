---
type: Rust Function
title: api_request_concurrency_permits_match_advertised_limit
resource: crates/lpe-jmap/src/tests.rs#L9804-L9816
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/try_acquire_api_request_permit
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn api_request_concurrency_permits_match_advertised_limit()`

# Calls

- [try_acquire_api_request_permit](../../../../../functions/crates/lpe-jmap/src/service/try_acquire_api_request_permit.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)