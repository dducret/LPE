---
type: Rust Function
title: add_common_headers
resource: crates/lpe-activesync/src/response.rs#L49-L65
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/response/empty_response
  - functions/crates/lpe-activesync/src/response/auth_challenge_response
  - functions/crates/lpe-activesync/src/response/wbxml_response
  - functions/crates/lpe-activesync/src/response/error_response
---

# Signature

`fn add_common_headers(headers: &mut HeaderMap)`

# Called by

- [empty_response](../../../../../functions/crates/lpe-activesync/src/response/empty_response.md)
- [auth_challenge_response](../../../../../functions/crates/lpe-activesync/src/response/auth_challenge_response.md)
- [wbxml_response](../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)
- [error_response](../../../../../functions/crates/lpe-activesync/src/response/error_response.md)