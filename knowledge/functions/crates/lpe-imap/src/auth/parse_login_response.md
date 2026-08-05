---
type: Rust Function
title: parse_login_response
resource: crates/lpe-imap/src/auth.rs#L181-L196
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
---

# Signature

`fn parse_login_response(encoded: &str, field_name: &str) -> Result<String>`

# Called by

- [handle_authenticate](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)