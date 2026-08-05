---
type: Rust Function
title: error_response
resource: crates/lpe-activesync/src/response.rs#L67-L81
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/response/auth_challenge_response
  - functions/crates/lpe-activesync/src/response/add_common_headers
---

# Signature

`pub(crate) fn error_response(error: anyhow::Error) -> Response`

# Calls

- [auth_challenge_response](../../../../../functions/crates/lpe-activesync/src/response/auth_challenge_response.md)
- [add_common_headers](../../../../../functions/crates/lpe-activesync/src/response/add_common_headers.md)