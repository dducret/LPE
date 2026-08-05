---
type: Rust Function
title: parse_plain_initial_response
resource: crates/lpe-imap/src/auth.rs#L158-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
---

# Signature

`fn parse_plain_initial_response(encoded: &str) -> Result<(String, String)>`

# Called by

- [handle_authenticate](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)