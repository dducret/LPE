---
type: Rust Function
title: bearer_headers_with_protocol_version
resource: crates/lpe-activesync/src/tests.rs#L1324-L1331
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds
  - functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably
  - functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version
---

# Signature

`fn bearer_headers_with_protocol_version(protocol_version: &str) -> HeaderMap`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [post_with_supported_protocol_version_succeeds](../../../../../functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds.md)
- [post_with_unsupported_protocol_version_is_rejected_predictably](../../../../../functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably.md)
- [unsupported_protocol_version_response_does_not_echo_request_version](../../../../../functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version.md)